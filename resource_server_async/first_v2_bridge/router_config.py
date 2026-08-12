"""Trimmed, duplicated copy of V2's RouterConfig parser.

Only the fields the bridge reads are modeled. extra="ignore" drops the many
V2 fields we don't care about.
"""

import redis
from pydantic import BaseModel, ConfigDict

# The single JSON blob key written by the V2 control plane.
ROUTER_CONFIG_KEY = "router-cfg"

# Dedicated connection to V2's Redis, kept separate from V1's own cache/broker
# client (resource_server_async.cache.get_redis_client) so the two deployments
# never share a database. Cached singleton, keyed on the URL.
_bridge_redis: dict[str, "redis.Redis"] = {}


def get_bridge_redis_client(redis_url: str | None) -> "redis.Redis":
    """Return a Redis client for V2's Redis.

    Raises if ``redis_url`` is unset: the bridge must never silently fall back
    to V1's Redis (that would read the wrong, empty database).
    """
    if not redis_url:
        raise RuntimeError(
            "FIRST_V2_REDIS_URL is not set; the bridge requires a dedicated "
            "connection to V2's Redis and must not fall back to V1's."
        )
    client = _bridge_redis.get(redis_url)
    if client is None:
        client = redis.Redis.from_url(redis_url)
        _bridge_redis[redis_url] = client
    return client


class BackendConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    model_url: str
    backend_model_name: str
    api_key: str | None = None


class DeploymentConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    kind: str
    name: str
    backends: list[BackendConfig] = []


class ModelConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str
    aliases: list[str] = []
    allowed_groups: list[str] = []
    allowed_domains: list[str] = []
    deployments: list[DeploymentConfig] = []


class RouterConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    version: int = 0
    models: list[ModelConfig] = []

    @classmethod
    def load(cls, client: "redis.Redis") -> "RouterConfig":
        """Read and parse the router config blob from Redis (sync client)."""
        raw = client.get(ROUTER_CONFIG_KEY)
        if not raw:
            return cls()
        assert isinstance(raw, (str, bytes, bytearray))
        return cls.model_validate_json(raw)
