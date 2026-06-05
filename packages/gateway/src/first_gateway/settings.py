from pathlib import Path
from typing import ClassVar, Self

from pydantic import (
    SecretStr,
    computed_field,
)
from pydantic_settings import BaseSettings, SettingsConfigDict


class GlobusAuthSettings(BaseSettings):
    app_id: str
    app_secret: SecretStr
    policies: list[str] = []
    authorized_idp_domains: list[str] = []
    user_groups: list[str] = []
    admin_group: str
    authorized_groups_per_idp: dict[str, list[str]] = {}
    authorized_service_usernames: list[str] = []

    @computed_field  # type: ignore[prop-decorator]
    @property
    def policies_str(self) -> str:
        return ",".join(self.policies)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def authorized_idp_domains_str(self) -> str:
        """
        For error message to hide restricted identity provided
        """
        idp_overlap = set(self.authorized_idp_domains) & set(
            self.authorized_groups_per_idp
        )
        if len(idp_overlap) == 0:
            return ", ".join(self.authorized_idp_domains)
        else:
            domains_string = [
                domain
                for domain in self.authorized_idp_domains
                if not domain in self.authorized_groups_per_idp
            ]
            return ", ".join(domains_string) + ", or providers with approved projects"


class Settings(BaseSettings):
    _cached: ClassVar[Self | None] = None

    model_config = SettingsConfigDict(
        # .env.prod will override .env:
        env_file=(".env", ".env.first", ".env.secret", ".env.prod"),
        env_prefix="first_",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",
    )

    prompt_storage_dir: Path = Path("prompt-records")

    db_url: SecretStr
    redis_url: str

    globus: GlobusAuthSettings

    @classmethod
    def load(cls) -> Self:
        if cls._cached is None:
            cls._cached = cls()
            cls._cached.prompt_storage_dir.mkdir(exist_ok=True, parents=True)
        return cls._cached


if __name__ == "__main__":
    from rich import print

    print("Loaded settings:", Settings.load())
