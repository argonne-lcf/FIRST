"""
These schemas describe the communication between first-gateway and first-pilot.

Do not confuse with admin-created pilot resources inside `resources` subpackage
"""

import os
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Self

import yaml
from pydantic import BaseModel, Field, PrivateAttr, computed_field, field_validator
from pydantic_settings import BaseSettings, EnvSettingsSource, SettingsConfigDict

from .types import (
    RESOURCE_NAME_PATTERN,
    GpuClaim,
    GpuDiscovery,
    NodeAdmission,
    ReplicaState,
    ResolvedLaunchSpec,
    SSHDiscovery,
    normalize_proxy_path,
    upstream_endpoint_path,
)

PILOT_SERVER_CN = "first-pilot"
PILOT_SERVER_SAN = "first-pilot.internal"


class PilotClientRole(StrEnum):
    """
    Client identities accepted by pilot NGINX. The value is the certificate CN;
    the pilot authorizes each request by role (see nginx_manager).
    """

    control = "first-control"
    router = "first-router"
    metrics = "first-metrics"


_JOB_ENV_OVERRIDE_FIELDS = frozenset(
    {
        "job_name",
        "external_port",
        "nginx_path",
        "ip_allowlist",
        "workdir",
        "node_file_env",
        "gpu_discovery",
        "node_admission",
        "queue",
        "account",
        "num_nodes",
        "gpus_per_node",
        "walltime_min",
    }
)


class ReplicaStartRequest(BaseModel):
    """
    Gateway request to start a replica on the pilot manager.

    `name` is interpolated into the pilot NGINX config as a location path, so
    it is restricted to the same character set as resource names.

    `supported_endpoints` (from the Model) and `prometheus_metrics_path` (from
    the deployment) are the only replica paths the pilot NGINX will proxy.
    """

    name: str = Field(min_length=1, max_length=320, pattern=RESOURCE_NAME_PATTERN)
    deployment_name: str
    launch_spec: ResolvedLaunchSpec
    gpu_indices: list[tuple[int, int]]
    supported_endpoints: list[str]
    prometheus_metrics_path: str | None

    @field_validator("supported_endpoints")
    @classmethod
    def normalize_endpoints(cls, v: list[str]) -> list[str]:
        return [normalize_proxy_path(e) for e in v]

    @field_validator("prometheus_metrics_path")
    @classmethod
    def normalize_metrics_path(cls, v: str | None) -> str | None:
        return f"/{normalize_proxy_path(v)}" if v else None

    @property
    def proxy_paths(self) -> list[str]:
        """Replica paths (leading slash) to expose, de-duplicated in order."""
        paths = [upstream_endpoint_path(e) for e in self.supported_endpoints]
        if self.prometheus_metrics_path:
            paths.append(self.prometheus_metrics_path)
        return list(dict.fromkeys(paths))


class ReplicaInfo(BaseModel):
    """
    Status information about a replica placed on the pilot manager.
    """

    name: str
    url: str
    state: ReplicaState
    started_at: datetime
    state_message: str
    served_model_name: str
    resources: list[GpuClaim]
    log_path: Path


class AddressInfo(BaseModel):
    """
    Endpoint discovery: how the gateway learns where the pilot manager can be
    reached.
    """

    hostname: str
    ip: str
    external_port: int
    control_path: str

    @property
    def base_url(self) -> str:
        return f"https://{self.ip}:{self.external_port}"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def control_url(self) -> str:
        return f"{self.base_url}/{self.control_path.lstrip('/')}"


class GpuInfo(BaseModel):
    """
    One GPU managed by a pilot, as `nvidia-smi --query-gpu` last reported it.

    The optional fields are None where the GPU does not report them (nvidia-smi
    prints "[N/A]").
    """

    index: str = Field(pattern=r"^[0-9]+$")
    name: str = Field(min_length=1)
    memory_total_mib: int = Field(gt=0)
    memory_used_mib: int = Field(ge=0)
    memory_free_mib: int | None = Field(None, ge=0)
    uuid: str | None = Field(None, min_length=1)
    driver_version: str | None = Field(None, min_length=1)
    ecc_uncorrected_volatile: int | None = Field(None, ge=0)
    ecc_uncorrected_aggregate: int | None = Field(None, ge=0)
    pstate: str | None = None  # P0 (maximum performance) to P12 (minimum)
    temperature_c: int | None = None
    utilization_gpu_pct: int | None = Field(None, ge=0, le=100)
    utilization_memory_pct: int | None = Field(None, ge=0, le=100)
    power_draw_w: float | None = Field(None, ge=0)
    # Bitmask of nvidia-smi clocks_throttle_reasons, e.g. "0x0000000000000004"
    clocks_throttle_reasons: str | None = None


class HostGpus(BaseModel):
    """
    Information about a host and its GPU resources managed under a pilot.
    """

    hostname: str
    gpus: list[GpuInfo]


class PilotResources(BaseModel):
    """
    Information about all hosts/GPUs managed under a pilot.
    """

    hosts: list[HostGpus] = []
    observed_at: datetime | None = None


class PilotJobStatus(BaseModel):
    """
    Result of /status endpoint from pilot manager control API: polled by gateway
    to discover resources and sync Replica status.
    """

    resources: PilotResources
    replicas: list[ReplicaInfo]


class PilotRuntimeConfig(BaseSettings):
    """
    The on-disk YAML contract between the gateway (which produces it at
    pilot-job submit time) and the first-pilot process (which loads it at
    startup).
    """

    model_config = SettingsConfigDict(
        env_prefix="pilot_", case_sensitive=False, extra="ignore"
    )
    _tmpdir: TemporaryDirectory[str] | None = PrivateAttr(default=None)

    ca_crt: str
    server_crt: str
    server_key: str

    external_port: int
    nginx_path: Path
    ip_allowlist: list[str]
    workdir: Path
    node_file_env: str
    gpu_discovery: GpuDiscovery = Field(default_factory=SSHDiscovery)
    node_admission: NodeAdmission = Field(default_factory=NodeAdmission)
    # The pilot job's own queue and account, reused for quarantine jobs.
    queue: str
    account: str
    num_nodes: int = Field(ge=1)
    gpus_per_node: int = Field(ge=1)
    job_name: str
    # Scheduler walltime; the server certificate must outlive it.
    walltime_min: int = Field(ge=1)

    # IPv4 network interfaces (e.g. ["hsn0", "hsn1"]) NGINX listens on, in
    # preference order. Interfaces that are missing, down, or have no IPv4
    # address are skipped; at least one must resolve. The first resolved address
    # is advertised.
    network_interfaces: list[str] = Field(min_length=1)

    @property
    def nginx_base_dir(self) -> Path:
        return self.workdir / "nginx"

    @property
    def replica_base_dir(self) -> Path:
        return self.workdir / "replicas"

    @property
    def readyfile_dir(self) -> Path:
        return self.workdir / "readyfiles"

    @property
    def audit_dir(self) -> Path:
        # Durable across the allocation: never move this under nginx_base_dir.
        return self.workdir / "audit"

    @property
    def control_uds_path(self) -> Path:
        if self._tmpdir is None:
            self._tmpdir = TemporaryDirectory()
        return Path(self._tmpdir.name) / f"pilot-control-{os.getpid()}.sock"

    def ensure_dirs(self) -> None:
        for d in (
            self.nginx_base_dir,
            self.replica_base_dir,
            self.readyfile_dir,
            self.audit_dir,
        ):
            d.mkdir(exist_ok=True, parents=True)

    @classmethod
    def load(cls) -> Self:
        """
        Load from PILOT_CONFIG_FILE environment variable pointing to a yaml
        config file. ``PILOT_``-prefixed environment variables override values
        from that file so the submitter can specialize a pre-baked GraphQL pilot
        config for each job.
        """
        yaml_path = os.environ["PILOT_CONFIG_FILE"]
        config_raw = yaml.safe_load(Path(yaml_path).read_text())
        if not isinstance(config_raw, dict):
            raise ValueError("pilot runtime config must be a YAML mapping")

        env_raw = {
            name: value
            for name, value in EnvSettingsSource(cls)().items()
            if name in _JOB_ENV_OVERRIDE_FIELDS
        }
        return cls.model_validate({**config_raw, **env_raw})
