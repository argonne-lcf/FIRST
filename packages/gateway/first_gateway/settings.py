from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncGenerator, Self

from globus_compute_sdk import Client as ComputeClient
from globus_sdk import ClientApp, ConfidentialAppAuthClient
from httpx import AsyncClient, Timeout
from pydantic import (
    BaseModel,
    SecretStr,
    computed_field,
    model_validator,
)
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
    # Cluster credentials: only controller-manager holds them (the gateway
    # refuses to start with them, see apiserver/api.py).
    compute_client: ComputeClient | None
    keycloak_clients: dict[str, AsyncClient]


class GlobusAuthSettings(BaseSettings):
    app_id: str
    app_secret: SecretStr
    compute_client_id: str | None = None
    compute_client_secret: SecretStr | None = None
    policies: list[str] = []
    authorized_idp_domains: list[str] = []
    user_groups: list[str] = []
    admin_group: str
    authorized_groups_per_idp: dict[str, list[str]] = {}
    authorized_service_usernames: list[str] = []

    @model_validator(mode="after")
    def validate_global_gates(self) -> Self:
        if not self.policies:
            raise ValueError("Globus policies must not be empty")
        if not self.authorized_idp_domains:
            raise ValueError("Globus authorized_idp_domains must not be empty")
        unmatched = set(self.authorized_groups_per_idp) - set(
            self.authorized_idp_domains
        )
        if unmatched:
            # A misspelled key would leave the real domain without its group
            # restriction, admitting every identity from it.
            raise ValueError(
                "Globus authorized_groups_per_idp contains domains absent from "
                f"authorized_idp_domains: {', '.join(sorted(unmatched))}"
            )
        return self

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


class LogShippingSettings(BaseModel):
    """
    Where the health alerter finds the state of log shipping
    """

    prometheus_url: str = "http://prometheus:9090"
    # One status file per prompt shipper named here: a missing one is an alert.
    status_dir: Path = Path("/run/log-shipping")
    prompt_shippers: list[str] = ["v2"]


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
    # The built admin console (packages/admin-console/dist), served by the
    # loopback-only control server at /admin-console/ when set.
    admin_console_dir: Path | None = None
    log_level: str = "INFO"
    # Per-logger overrides (for selective debugging):
    # FIRST_LOG_LEVELS='{"first_gateway.platforms.schedulers.graphql_pbs": "DEBUG"}'
    log_levels: dict[str, str] = {
        "first_gateway.platforms.schedulers.graphql_pbs": "INFO",
    }

    db_url: SecretStr
    redis_url: str

    globus: GlobusAuthSettings
    data_staging_globus_collection_id: str = ""
    # Transfer-only confidential client with access-manager on the staging
    # collection (gateway only). Unset: the staging route returns 503.
    data_staging_client_id: str | None = None
    data_staging_client_secret: SecretStr | None = None
    # Pilot mTLS: the CA that signs pilot server certs, and this service's own
    # client identity. Compose decides which role's cert is mounted here.
    pilot_ca_crt_file: Path = Path("/run/secrets/pilot_ca.crt")
    pilot_client_crt_file: Path = Path("/run/secrets/pilot_client.crt")
    pilot_client_key_file: Path = Path("/run/secrets/pilot_client.key")
    health_slack_bot_token: SecretStr | None = None
    health_slack_channel: str | None = None
    gateway_health_url: str = "http://127.0.0.1/health"
    log_shipping: LogShippingSettings | None = None

    keycloak_clients: dict[str, KeycloakSettings] = {}

    def _build_compute_client(self) -> ComputeClient | None:
        """The Globus Compute client, if configured (controller-manager only)."""
        secret = self.globus.compute_client_secret
        if secret is None or not secret.get_secret_value():
            return None
        if not self.globus.compute_client_id:
            raise ValueError(
                "FIRST_GLOBUS__COMPUTE_CLIENT_SECRET is set without "
                "FIRST_GLOBUS__COMPUTE_CLIENT_ID"
            )
        return ComputeClient(
            app=ClientApp(
                client_id=self.globus.compute_client_id,
                client_secret=secret.get_secret_value(),
            ),
            do_version_check=False,
        )

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
                compute_client=self._build_compute_client(),
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
