import os
from pathlib import Path
from typing import Annotated, Self

import yaml
from pydantic import AfterValidator
from pydantic_settings import BaseSettings


def _is_file(p: Path) -> Path:
    if not p.is_file():
        raise ValueError(f"{p} does not point to a valid file")
    return p


FilePath = Annotated[Path, AfterValidator(_is_file)]


class Config(BaseSettings):
    ca_crt_path: FilePath
    server_crt_path: FilePath
    server_key_path: FilePath

    external_port: int
    control_port: int
    nginx_path: FilePath
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
