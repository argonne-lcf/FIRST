"""Redis runtime state access layer."""

from datetime import UTC, datetime
from typing import Any

from redis.asyncio import Redis

from first_common.schema.resources.runtime import BackendRuntime, ModelRuntime

from .keys import Keys


def _to_str(v: Any) -> str:
    """Coerce a Redis return value to str. Belt-and-suspenders for decode_responses=True."""
    if isinstance(v, (bytes, bytearray)):
        return v.decode()
    return str(v)


def _parse_backend_runtime(inflight_raw: Any, errors_raw: Any) -> BackendRuntime:
    return BackendRuntime(
        inflight=int(inflight_raw) if inflight_raw else 0,
        cooldown_errors=int(errors_raw) if errors_raw else 0,
    )


def _parse_model_runtime(raw: dict[Any, Any]) -> ModelRuntime:
    if not raw:
        return ModelRuntime()

    data = {_to_str(k): _to_str(v) for k, v in raw.items()}
    last_ts = data.get("last_reject_ts")
    return ModelRuntime(
        total_inflight=int(data.get("inflight", "0")),
        capacity_rejects_total=int(data.get("capacity_rejects_total", "0")),
        last_capacity_reject=(
            datetime.fromtimestamp(float(last_ts), tz=UTC) if last_ts else None
        ),
    )


class RedisRepo:
    def __init__(self, client: Redis) -> None:
        self.client = client

    async def get_backend_runtime(
        self, model_name: str, backend_id: str
    ) -> BackendRuntime:
        async with self.client.pipeline(transaction=False) as pipe:
            pipe.hget(Keys.model_inflight(model_name), backend_id)
            pipe.get(Keys.backend_errors(backend_id))
            inflight_raw, errors_raw = await pipe.execute()
        return _parse_backend_runtime(inflight_raw, errors_raw)

    async def get_all_backend_runtimes(
        self, model_name: str
    ) -> dict[str, BackendRuntime]:
        raw_map = await self.client.hgetall(Keys.model_inflight(model_name))
        if not raw_map:
            return {}

        inflight_map = {_to_str(k): _to_str(v) for k, v in raw_map.items()}
        backend_ids = list(inflight_map)
        error_keys = [Keys.backend_errors(bid) for bid in backend_ids]
        error_vals = await self.client.mget(error_keys)

        result: dict[str, BackendRuntime] = {}
        for bid, errors_raw in zip(backend_ids, error_vals):
            result[bid] = BackendRuntime(
                inflight=int(inflight_map[bid]),
                cooldown_errors=int(_to_str(errors_raw)) if errors_raw else 0,
            )
        return result

    async def get_many_backend_runtimes(
        self, keys: list[tuple[str, str]]
    ) -> list[BackendRuntime]:
        """
        Fetch a list of BackendRuntimes by (model_name, backend_id)
        """
        if not keys:
            return []
        async with self.client.pipeline(transaction=False) as pipe:
            for model_name, backend_id in keys:
                pipe.hget(Keys.model_inflight(model_name), backend_id)
                pipe.get(Keys.backend_errors(backend_id))
            results = await pipe.execute()

        out: list[BackendRuntime] = []
        for i in range(0, len(results), 2):
            out.append(_parse_backend_runtime(results[i], results[i + 1]))
        return out

    async def get_model_runtime(self, model_name: str) -> ModelRuntime:
        raw = await self.client.hgetall(Keys.model_demand(model_name))
        return _parse_model_runtime(raw)

    async def get_many_model_runtimes(
        self, model_names: list[str]
    ) -> list[ModelRuntime]:
        if not model_names:
            return []
        async with self.client.pipeline(transaction=False) as pipe:
            for name in model_names:
                pipe.hgetall(Keys.model_demand(name))
            results = await pipe.execute()
        return [_parse_model_runtime(data) for data in results]

    async def get_cached_token(self, token_hash: str) -> str | None:
        val = await self.client.get(Keys.token_introspect(token_hash))
        return _to_str(val) if val else None

    async def set_cached_token(self, token_hash: str, value: str, ttl: int) -> None:
        await self.client.set(Keys.token_introspect(token_hash), value, ex=ttl)

    async def mark_authed_user(self, user_id: str, ttl: int = 120) -> bool:
        """Returns True if this is the first mark within the TTL window."""
        return bool(
            await self.client.set(Keys.authed_user(user_id), "", nx=True, ex=ttl)
        )

    async def is_new_error_log(
        self,
        user: str,
        status_code: int,
        fingerprint: str | None = None,
        ttl: int = 30,
    ) -> bool:
        """Returns True if this user/status/fingerprint combo hasn't been seen within TTL."""
        if fingerprint is None:
            key = Keys.log_dedup_5xx(user, status_code)
        else:
            key = Keys.log_dedup_4xx(user, fingerprint, status_code)
        return bool(await self.client.set(key, "", nx=True, ex=ttl))
