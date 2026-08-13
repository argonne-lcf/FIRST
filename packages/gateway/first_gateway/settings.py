from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, AsyncGenerator, Literal

from globus_compute_sdk import Client as ComputeClient
from globus_sdk import ClientApp, ConfidentialAppAuthClient
from httpx import AsyncClient, Timeout
from pydantic import Field, SecretStr, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict
from redis.asyncio import Redis as AsyncRedis
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from .database.redis.pubsub import RedisPubSub
from .database.redis.repo import RedisRepo
from .services.keycloak_client import KeycloakServiceTokenAuth


@dataclass
class ClientState:
    """
    Centralized, shared instances of connection-pooling client resources.
    """

    settings: "Settings"
    redis: AsyncRedis
    redis_repo: RedisRepo
    redis_pubsub: RedisPubSub
    db_engine: AsyncEngine
    db_sessionmaker: async_sessionmaker[AsyncSession]
    auth_client: ConfidentialAppAuthClient
    compute_client: ComputeClient
    keycloak_clients: dict[str, AsyncClient]


class GlobusAuthSettings(BaseSettings):
    app_id: str
    app_secret: SecretStr
    compute_client_id: str
    compute_client_secret: SecretStr
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


class KeycloakSettings(BaseSettings):
    base_url: str
    realm: str
    impersonation_client_id: str
    impersonation_client_secret: SecretStr
    audience: str
    requested_subject: str = "openinference_svc"
    ssl_verify: bool = True
    timeout: float = 10.0

    @property
    def token_url(self) -> str:
        return f"{self.base_url}/realms/{self.realm}/protocol/openid-connect/token"


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
    controller_metrics_host: Literal["0.0.0.0", "127.0.0.1"] = "0.0.0.0"
    controller_metrics_port: Annotated[int, Field(ge=1, le=65535)] = 9100

    db_url: SecretStr
    redis_url: str

    globus: GlobusAuthSettings
    pilot_ca_crt: str
    pilot_ca_key: SecretStr
    health_slack_webhook_url: str | None = None
    gateway_health_url: str = "http://127.0.0.1/health"

    keycloak_clients: dict[str, KeycloakSettings] = {}

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

        redis = AsyncRedis.from_url(self.redis_url, decode_responses=True)
        await redis.ping()
        try:
            yield ClientState(
                settings=self,
                db_engine=engine,
                db_sessionmaker=sessionmaker,
                redis=redis,
                redis_repo=RedisRepo(redis),
                redis_pubsub=RedisPubSub(redis),
                auth_client=ConfidentialAppAuthClient(
                    self.globus.app_id, self.globus.app_secret.get_secret_value()
                ),
                compute_client=ComputeClient(
                    app=ClientApp(
                        client_id=self.globus.compute_client_id,
                        client_secret=self.globus.compute_client_secret.get_secret_value(),
                    ),
                    do_version_check=False,
                ),
                keycloak_clients={
                    name: AsyncClient(
                        auth=KeycloakServiceTokenAuth(cfg),
                        verify=cfg.ssl_verify,
                        timeout=Timeout(cfg.timeout),
                    )
                    for name, cfg in self.keycloak_clients.items()
                },
            )
        finally:
            await redis.aclose()
            await engine.dispose()
