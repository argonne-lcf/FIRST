from datetime import datetime
from functools import cached_property
from typing import Any, AsyncIterator, Literal, Self

from pydantic import BaseModel, field_validator
from redis.asyncio import Redis

from first_common.schema.types import (
    OverloadPolicy,
    RouterParams,
    UsagePolicy,
)

from .keys import Keys
from .pubsub import Channel, RedisPubSub


class BackendConfig(BaseModel):
    id: str
    model_url: str
    backend_model_name: str
    api_key: str | None


class DeploymentConfig(BaseModel):
    kind: Literal["pilot", "static"]
    name: str
    router_params: RouterParams
    prometheus_metrics_path: str | None
    prometheus_scrape_interval_sec: int
    backends: list[BackendConfig]


class ModelConfig(BaseModel):
    name: str
    aliases: list[str]
    allowed_groups: list[str]
    allowed_domains: list[str]
    supported_endpoints: list[str]
    max_model_len: int | None = None
    created_at: datetime | None = None
    display_name: str | None = None
    capabilities: dict[str, Any] = {}
    usage_limits: UsagePolicy
    overload: OverloadPolicy
    deployments: list[DeploymentConfig]

    @field_validator("supported_endpoints")
    @classmethod
    def normalize_endpoints(cls, v: list[str]) -> list[str]:
        return [e.strip().strip("/") for e in v]


class RouterConfig(BaseModel):
    """
    The contract between the control plane and data plane.

    The Control Plane is the sole writer of the RouterConfig: it coalesces
    information about all model instances that are running and routeable.

    The apiserver is the sole reader of the RouterConfig: it uses this
    live-updating configuration snapshot to route incoming traffic to model backends.
    """

    version: int = 0
    models: list[ModelConfig] = []

    @cached_property
    def models_by_name(self) -> dict[str, ModelConfig]:
        return {model.name: model for model in self.models}

    @cached_property
    def models_by_alias(self) -> dict[str, ModelConfig]:
        return {alias: model for model in self.models for alias in model.aliases}

    @classmethod
    async def load(cls, client: Redis) -> Self:
        raw = await client.get(Keys.config())
        return cls.model_validate_json(raw) if raw else cls()

    async def publish(self, client: Redis) -> int:
        """Atomically swap the blob (version bumped) and notify subscribers.

        A plain SET of the whole document is the atomicity mechanism: readers
        can never observe a torn config.  Callers should coalesce membership
        churn (1-2 s debounce) rather than publish per event.
        """
        self.version += 1
        await client.set(Keys.config(), self.model_dump_json())
        await RedisPubSub(client).publish(Channel.router_cfg_updated, str(self.version))
        return self.version

    @classmethod
    async def subscribe(cls, client: Redis) -> AsyncIterator[Self]:
        """Yield fresh configs as versions are announced (poll fallback is the
        caller's job).  Convenience for the snapshot manager."""
        pubsub = RedisPubSub(client)

        async for _ in pubsub.subscribe(Channel.router_cfg_updated):
            cfg = await cls.load(client)
            yield cfg
