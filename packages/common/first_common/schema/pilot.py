"""
These schemas describe the communication between first-gateway and first-pilot.

Do not confuse with admin-created pilot resources inside `resources` subpackage
"""

import os
from datetime import datetime
from pathlib import Path
from typing import Self

import yaml
from pydantic import BaseModel, computed_field

from .types import GpuClaim, PilotLaunchSpec, ReplicaPhase


class ReplicaStartRequest(BaseModel):
    """
    Gateway request to start a replica on the pilot manager.
    """

    name: str
    deployment_name: str
    launch_spec: PilotLaunchSpec
    resources: list[GpuClaim]


class ReplicaInfo(BaseModel):
    """
    Status information about a replica placed on the pilot manager.
    """

    name: str
    url: str
    phase: ReplicaPhase
    started_at: datetime
    status_info: str
    served_model_name: str


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
    Information about a GPU resource managed by a pilot.
    """

    index: str
    name: str
    memory_total_mib: int | None
    memory_used_mib: int | None


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

    hosts: list[HostGpus]


class PilotJobStatus(BaseModel):
    """
    Result of /status endpoint from pilot manager control API: polled by gateway
    to discover resources and sync Replica status.
    """

    resources: PilotResources
    replicas: list[ReplicaInfo]


class PilotRuntimeConfig(BaseModel):
    """
    The on-disk YAML contract between the gateway (which produces it at
    pilot-job submit time) and the first-pilot process (which loads it at
    startup).
    """

    ca_crt: str
    server_crt: str
    server_key: str

    external_port: int
    nginx_path: Path
    ip_allowlist: list[str]
    workdir: Path
    node_file_env: str
    job_name: str

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
    def control_port_internal(self) -> int:
        return self.external_port + 1

    def ensure_dirs(self) -> None:
        for d in (self.nginx_base_dir, self.replica_base_dir, self.readyfile_dir):
            d.mkdir(exist_ok=True, parents=True)

    @classmethod
    def load(cls) -> Self:
        """
        Load from PILOT_CONFIG_FILE environment variable pointing to a yaml
        config file.
        """
        yaml_path = os.environ["PILOT_CONFIG_FILE"]
        config_raw = yaml.safe_load(Path(yaml_path).read_text())
        return cls.model_validate(config_raw)
