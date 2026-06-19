from pathlib import Path
from typing import Annotated, Self

import yaml
from pydantic import AfterValidator, BaseModel


def _is_file(p: Path) -> Path:
    if not p.is_file():
        raise ValueError(f"{p} does not point to a valid file")
    return p


FilePath = Annotated[Path, AfterValidator(_is_file)]


class Config(BaseModel):
    ca_crt_path: FilePath
    server_crt_path: FilePath
    server_key_path: FilePath

    nginx_path: FilePath
    ip_allowlist: list[str]

    @classmethod
    def load_yaml(cls, yaml_path: Path) -> Self:
        return cls.model_validate(yaml.safe_load(yaml_path))
