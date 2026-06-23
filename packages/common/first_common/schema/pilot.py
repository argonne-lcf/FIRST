from datetime import datetime

from pydantic import BaseModel, computed_field

from .types import GpuClaim, PilotLaunchSpec, ReplicaPhase


class ReplicaStartRequest(BaseModel):
    name: str
    deployment_name: str
    launch_spec: PilotLaunchSpec
    resources: list[GpuClaim]


class ReplicaInfo(BaseModel):
    name: str
    url: str
    phase: ReplicaPhase
    started_at: datetime


class AddressInfo(BaseModel):
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
    index: str
    name: str
    memory_total_mib: int | None
    memory_used_mib: int | None


class HostGpus(BaseModel):
    """The GPUs presumed available on one host."""

    hostname: str
    gpus: list[GpuInfo]


class PilotResources(BaseModel):
    hosts: list[HostGpus]


class PilotJobStatus(BaseModel):
    resources: PilotResources
    replicas: list[ReplicaInfo]
