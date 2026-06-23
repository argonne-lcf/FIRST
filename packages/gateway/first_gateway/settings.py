from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator, TypedDict

from globus_compute_sdk import Client as ComputeClient
from globus_sdk import ClientApp, ConfidentialAppAuthClient
from httpx import AsyncClient
from pydantic import (
    SecretStr,
    computed_field,
)
from pydantic_settings import BaseSettings, SettingsConfigDict
from redis.asyncio import Redis as AsyncRedis
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


class ClientState(TypedDict):
    """
    Centralized, shared instances of connection-pooling client resources.
    """

    settings: "Settings"
    httpx_client: AsyncClient
    redis: AsyncRedis
    db_engine: AsyncEngine
    db_sessionmaker: async_sessionmaker[AsyncSession]
    auth_client: ConfidentialAppAuthClient
    compute_client: ComputeClient


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
    model_config = SettingsConfigDict(
        # Auto-detect and layer variables for local development (outside of containers)
        env_file=(
            ".env.default",  # common
            ".env.local",  # development host
            ".env.secret",  # .gitignored secrets
        ),
        env_prefix="first_",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",
    )

    prompt_storage_dir: Path = Path("prompt-records")
    log_level: str = "INFO"

    db_url: SecretStr
    redis_url: str

    globus: GlobusAuthSettings
    pilot_ca_crt: str
    pilot_ca_key: SecretStr

    @asynccontextmanager
    async def build_clients(self) -> AsyncGenerator[ClientState, None]:
        """
        Initializes shared client resources
        """
        engine = create_async_engine(
            self.db_url.get_secret_value(),
            pool_size=5,
            max_overflow=10,
        )
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)

        redis = AsyncRedis.from_url(self.redis_url)
        await redis.ping()
        try:
            async with AsyncClient() as httpx_client:
                yield ClientState(
                    settings=self,
                    db_engine=engine,
                    db_sessionmaker=sessionmaker,
                    redis=redis,
                    httpx_client=httpx_client,
                    auth_client=ConfidentialAppAuthClient(
                        self.globus.app_id, self.globus.app_secret.get_secret_value()
                    ),
                    compute_client=ComputeClient(
                        app=ClientApp(
                            client_id=self.globus.app_id,
                            client_secret=self.globus.app_secret.get_secret_value(),
                        ),
                        do_version_check=False,
                    ),
                )
        finally:
            await redis.aclose()
            await engine.dispose()
