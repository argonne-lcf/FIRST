from pathlib import Path
from typing import ClassVar, Self

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    _cached: ClassVar[Self | None] = None

    model_config = SettingsConfigDict(
        # .env.prod will override .env:
        env_file=(".env", ".env.first", ".env.prod"),
        env_prefix="first_",
        case_sensitive=False,
        extra="ignore",
    )

    db_url: SecretStr
    redis_url: str
    prompt_storage_dir: Path = Path("prompt-records")

    @classmethod
    def load(cls) -> Self:
        if cls._cached is None:
            cls._cached = cls()
            cls._cached.prompt_storage_dir.mkdir(exist_ok=True, parents=True)
        return cls._cached
