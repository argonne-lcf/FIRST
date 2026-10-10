"""Optional, job-local node isolation capabilities; never a model launch gate."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Annotated, Any, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    field_validator,
    model_validator,
)

from .base_scheduler import JobSubmitResult, SchedulerJobState

Environment = Literal["dev", "prod"]
Host = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9.-]{0,252}$")]
Operation = Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
JobId = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")]
ClusterName = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")]
MAX_REPORT_BYTES = 65536
MAX_ENCODED_REPORT_BYTES = 98304
AUXILIARY_PREFIXES = (
    "first_quar_",
    "first_qreport_",
    "first_qrequest_",
    "first_tara_quarantine_",
)


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class NodeQuarantinePolicy(StrictModel):
    environment: Environment
    worker_path: str | None = None
    worker_sha256: Digest | None = None
    admission_path: str | None = None
    admission_sha256: Digest | None = None
    policy_path: str | None = None
    policy_sha256: Digest
    walltime_min: int = Field(default=10080, ge=1, le=10080)
    max_submission_attempts: int = Field(default=3, ge=1, le=3)
    max_new_nodes_15min: int = Field(default=2, ge=1, le=2)
    max_active_nodes: int = Field(default=32, ge=1, le=32)

    @field_validator("worker_path", "admission_path", "policy_path")
    @classmethod
    def pinned_compute_path(cls, value: str | None) -> str | None:
        if value is None:
            return None
        path = PurePosixPath(value)
        if (
            not path.is_absolute()
            or str(path) != value
            or ".." in path.parts
            or any(ord(c) < 32 for c in value)
        ):
            raise ValueError(
                "quarantine assets require canonical absolute compute paths"
            )
        return value

    @model_validator(mode="after")
    def paired_asset_pins(self) -> Self:
        for path, checksum in (
            (self.worker_path, self.worker_sha256),
            (self.admission_path, self.admission_sha256),
        ):
            if (path is None) != (checksum is None):
                raise ValueError(
                    "an optional worker/helper asset requires its matching checksum"
                )
        return self


class QuarantineSource(StrictModel):
    id: JobId
    name: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]*$", max_length=128)
    ctime: int = Field(gt=0)

    @property
    def job_id(self) -> str:
        return self.id


class ReportBase(StrictModel):
    schema_version: Literal[3]
    environment: Environment
    cluster: ClusterName
    operation: Operation
    origin: QuarantineSource
    host: Host
    observed_unix_ns: int = Field(gt=0)


class QuarantineFaultPayload(ReportBase):
    event: Literal["node_fault"]
    source: Literal["pre_pilot_admission", "lifecycle_post_cleanup"]
    attempt: Operation
    allocation_hosts: list[Host] = Field(min_length=1, max_length=20)
    master_hsn: str
    bundle_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    policy_sha256: Digest
    policy_json: str = Field(min_length=1, max_length=MAX_REPORT_BYTES)
    evidence_path: str = Field(min_length=1, max_length=2048)
    reports: list[dict[str, Any]] = Field(min_length=1, max_length=1)
    confirmation: list[dict[str, Any]] = Field(min_length=1, max_length=1)
    cleanup_proof: str | None = Field(default=None, max_length=32768)

    @model_validator(mode="after")
    def allocation_and_evidence(self) -> Self:
        if (
            len(set(self.allocation_hosts)) != len(self.allocation_hosts)
            or self.host not in self.allocation_hosts
        ):
            raise ValueError("fault allocation membership is invalid")
        path = PurePosixPath(self.evidence_path)
        if (
            not path.is_absolute()
            or str(path) != self.evidence_path
            or ".." in path.parts
        ):
            raise ValueError("fault evidence requires a canonical absolute path")
        if (self.source == "lifecycle_post_cleanup") != bool(self.cleanup_proof):
            raise ValueError("lifecycle faults require their real cleanup proof only")
        return self


class WorkerIdentity(StrictModel):
    pid: int = Field(gt=0)
    pgid: int = Field(gt=0)
    sid: int = Field(gt=0)
    starttime: int = Field(gt=0)
    uid: int = Field(ge=0)


class QuarantineActivationPayload(ReportBase):
    event: Literal["activated"]
    quarantine_job_id: JobId
    worker_identity: WorkerIdentity
    gpu_initialized: Literal[False]
    expires_unix: int = Field(gt=0)
    native_job: dict[str, Any] | None = None
    native_nodes: dict[str, dict[str, Any]] | None = Field(
        default=None, min_length=1, max_length=16
    )


class QuarantineScreenPayload(ReportBase):
    event: Literal["screen_result"]
    quarantine_job_id: JobId
    nonce: Operation
    outcome: Literal["PASS", "FAIL"]
    reports: list[dict[str, Any]] = Field(max_length=1)
    confirmation: list[dict[str, Any]] = Field(max_length=1)
    master_hsn: str | None
    bundle_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    policy_sha256: Digest
    policy_json: str | None = Field(default=None, max_length=MAX_REPORT_BYTES)
    error: str | None = Field(default=None, max_length=300)

    @model_validator(mode="after")
    def passing_samples(self) -> Self:
        if self.outcome == "PASS" and (
            len(self.reports) != 1
            or len(self.confirmation) != 1
            or not self.master_hsn
            or not self.policy_json
            or self.error
        ):
            raise ValueError(
                "passing return screen requires complete native paired evidence"
            )
        return self


QuarantinePayload = Annotated[
    QuarantineFaultPayload | QuarantineActivationPayload | QuarantineScreenPayload,
    Field(discriminator="event"),
]
QUARANTINE_PAYLOAD: TypeAdapter[
    QuarantineFaultPayload | QuarantineActivationPayload | QuarantineScreenPayload
] = TypeAdapter(QuarantinePayload)


class QuarantineScreenRequest(StrictModel):
    schema_version: Literal[3]
    operation: Operation
    host: Host
    quarantine_job_id: JobId
    nonce: Operation
    requested_unix: int = Field(gt=0)


class QuarantineJobIdentity(StrictModel):
    job_id: JobId
    job_name: str = Field(min_length=1, max_length=128)
    ctime: int = Field(gt=0)
    environment: Environment
    cluster: ClusterName
    host: Host
    operation: Operation
    source: QuarantineSource


class QuarantineJobSpec(StrictModel):
    environment: Environment
    cluster: ClusterName
    host: Host
    operation: Operation
    source: QuarantineSource
    queue: str = Field(pattern=r"^[A-Za-z0-9_.-]+$")
    account: str = Field(pattern=r"^[A-Za-z0-9_.-]+$")
    log_path: str = Field(min_length=1, max_length=2048)
    policy: NodeQuarantinePolicy

    @property
    def job_name(self) -> str:
        return f"first_quar_{self.environment}_{self.host}_{self.operation[:8]}"


@dataclass(frozen=True)
class QuarantineJobStatus:
    identity: QuarantineJobIdentity
    state: SchedulerJobState
    allocated_hosts: tuple[str, ...]
    exclusive: bool
    expires_unix: int | None
    hold_type: str | None = None
    request_verified: bool = False


@dataclass(frozen=True)
class QuarantineReport:
    job_id: str
    job_name: str
    ctime: int
    payload: (
        QuarantineFaultPayload | QuarantineActivationPayload | QuarantineScreenPayload
    )
    fingerprint: str

    @property
    def environment(self) -> Environment:
        return self.payload.environment

    @property
    def operation(self) -> str:
        return self.payload.operation


@dataclass(frozen=True)
class QuarantineReportRejected:
    job_id: str
    job_name: str
    ctime: int | None
    environment: Environment | None
    fingerprint: str
    reason: str

    @property
    def name(self) -> str:
        return self.job_name


class QuarantineSubmissionRejected(RuntimeError):
    """The bridge explicitly rejected createJob; no acknowledgement ambiguity."""


class SchedulerQuarantineAdapter(ABC):
    """Optional scheduler capability, isolated from normal model lifecycle."""

    @abstractmethod
    async def list_quarantine_jobs(self) -> list[QuarantineJobStatus]: ...

    @abstractmethod
    async def list_quarantine_reports(
        self,
    ) -> list[QuarantineReport | QuarantineReportRejected]: ...

    @abstractmethod
    async def submit_quarantine(self, spec: QuarantineJobSpec) -> JobSubmitResult: ...

    @abstractmethod
    async def get_quarantine_status(
        self, job_id: str
    ) -> QuarantineJobStatus | None: ...

    @abstractmethod
    async def get_source_allocation(
        self, source: QuarantineSource
    ) -> tuple[str, ...]: ...

    @abstractmethod
    async def verify_quarantine_activation(
        self, payload: QuarantineActivationPayload, status: QuarantineJobStatus
    ) -> None: ...

    @abstractmethod
    async def delete_quarantine(self, identity: QuarantineJobIdentity) -> None: ...

    @abstractmethod
    async def request_quarantine_screen(
        self, identity: QuarantineJobIdentity, nonce: str
    ) -> JobSubmitResult: ...

    @abstractmethod
    async def acknowledge_quarantine_report(self, report: QuarantineReport) -> None: ...

    @abstractmethod
    async def acknowledge_quarantine_screen_request(
        self, report: QuarantineReport
    ) -> None: ...
