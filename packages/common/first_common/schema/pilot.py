"""
These schemas describe the communication between first-gateway and first-pilot.

Do not confuse with admin-created pilot resources inside `resources` subpackage
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Self

import yaml
from pydantic import BaseModel, Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

from .types import GpuClaim, PilotLaunchSpec, ReplicaState


class ReplicaStartRequest(BaseModel):
    """
    Gateway request to start a replica on the pilot manager.
    """

    name: str
    deployment_name: str
    launch_spec: PilotLaunchSpec
    gpu_indices: list[tuple[int, int]]


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

    hosts: list[HostGpus] = []


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

    ca_crt: str
    server_crt: str
    server_key: str

    external_port: int
    nginx_path: Path
    nginx_sha256: str | None = Field(default=None, pattern=r"[0-9a-f]{64}")
    pilot_runtime_manifest_sha256: str | None = Field(
        default=None, pattern=r"[0-9a-f]{64}"
    )
    pilot_source_identity_sha256: str | None = Field(
        default=None, pattern=r"[0-9a-f]{64}"
    )
    ip_allowlist: list[str]
    workdir: Path
    node_file_env: str
    pals_path: Path | None = None
    num_nodes: int = Field(ge=1)
    gpus_per_node: int = Field(ge=1)
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
        config file.  Missing fields in the file are supplanted by "pilot_" prefixed
        environment variables.
        """
        yaml_path = os.environ["PILOT_CONFIG_FILE"]
        config_raw = yaml.safe_load(Path(yaml_path).read_text())
        if not isinstance(config_raw, dict):
            raise ValueError("pilot runtime config must be a YAML mapping")

        # These values are allocation-specific.  GraphQL schedulers cannot stage
        # a fresh config file per job, so PilotSubmitter supplies authoritative
        # values through the command environment.  Override any stale values in
        # the pre-baked config rather than allowing them to describe a different
        # allocation.
        for field_name in (
            "job_name",
            "external_port",
            "nginx_path",
            "nginx_sha256",
            "workdir",
            "node_file_env",
            "pals_path",
            "num_nodes",
            "gpus_per_node",
        ):
            env_name = f"PILOT_{field_name.upper()}"
            if env_name in os.environ:
                value = os.environ[env_name]
                config_raw[field_name] = None if value == "" else value
        for field_name, env_name in (
            ("pilot_runtime_manifest_sha256", "PILOT_RUNTIME_MANIFEST_SHA256"),
            ("pilot_source_identity_sha256", "PILOT_SOURCE_IDENTITY_SHA256"),
        ):
            if env_name in os.environ:
                value = os.environ[env_name]
                config_raw[field_name] = None if value == "" else value
        if "PILOT_IP_ALLOWLIST_JSON" in os.environ:
            try:
                allowlist = json.loads(os.environ["PILOT_IP_ALLOWLIST_JSON"])
            except json.JSONDecodeError as exc:
                raise ValueError("PILOT_IP_ALLOWLIST_JSON is invalid JSON") from exc
            if not isinstance(allowlist, list) or not all(
                isinstance(value, str) for value in allowlist
            ):
                raise ValueError("PILOT_IP_ALLOWLIST_JSON must be a string list")
            config_raw["ip_allowlist"] = allowlist
        return cls.model_validate(config_raw)
