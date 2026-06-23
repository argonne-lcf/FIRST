import os
from pathlib import Path
from typing import Self

import yaml
from pydantic_settings import BaseSettings


class Config(BaseSettings):
    # PEM contents (not paths) — the gateway injects these at job-submission
    # time so admins don't have to stage/rotate cert material on the HPC
    # filesystem. NginxManager writes them to mode-600 files inside its
    # private tmpdir at startup.
    ca_crt: str
    server_crt: str
    server_key: str

    external_port: int
    nginx_path: Path
    ip_allowlist: list[str]
    workdir: Path
    node_file_env: str
    job_name: str

    @classmethod
    def load(cls) -> Self:
        """
        Loads config from PILOT_CONFIG_FILE path pointing to YAML file, if available.
        Otherwise, falls back to loading from environment variables.
        """
        if yaml_path := os.environ.get("PILOT_CONFIG_FILE"):
            return cls.model_validate(yaml.safe_load(yaml_path))
        return cls()

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
