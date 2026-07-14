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


def _parse_model_rejects(raw: dict[Any, Any]) -> tuple[int, datetime | None]:
    """Extract rejection counters from the model demand hash."""
    if not raw:
        return 0, None
    data = {_to_str(k): _to_str(v) for k, v in raw.items()}
    last_ts = data.get("last_reject_ts")
    return (
        int(data.get("capacity_rejects_total", "0")),
        datetime.fromtimestamp(float(last_ts), tz=UTC) if last_ts else None,
    )


class RedisRepo:
    def __init__(self, client: Redis) -> None:
        self.client = client

    async def get_backend_runtime(
        self, model_name: str, backend_id: str
    ) -> BackendRuntime:
        async with self.client.pipeline(transaction=False) as pipe:
            pipe.zcard(Keys.backend_inflight(model_name, backend_id))
            pipe.get(Keys.backend_errors(backend_id))
            inflight_raw, errors_raw = await pipe.execute()
        return _parse_backend_runtime(inflight_raw, errors_raw)

    async def get_many_backend_runtimes(
        self, keys: list[tuple[str, str]]
    ) -> list[BackendRuntime]:
        if not keys:
            return []
        async with self.client.pipeline(transaction=False) as pipe:
            for model_name, backend_id in keys:
                pipe.zcard(Keys.backend_inflight(model_name, backend_id))
                pipe.get(Keys.backend_errors(backend_id))
            results = await pipe.execute()

        out: list[BackendRuntime] = []
        for i in range(0, len(results), 2):
            out.append(_parse_backend_runtime(results[i], results[i + 1]))
        return out

    async def get_model_runtime(self, model_name: str) -> ModelRuntime:
        async with self.client.pipeline(transaction=False) as pipe:
            pipe.zcard(Keys.model_inflight(model_name))
            pipe.hgetall(Keys.model_rejects(model_name))
            inflight, rejects_raw = await pipe.execute()
        rejects, last_reject = _parse_model_rejects(rejects_raw)
        return ModelRuntime(
            total_inflight=int(inflight),
            capacity_rejects_total=rejects,
            last_capacity_reject=last_reject,
        )

    async def get_many_model_runtimes(
        self, model_names: list[str]
    ) -> list[ModelRuntime]:
        if not model_names:
            return []
        async with self.client.pipeline(transaction=False) as pipe:
            for name in model_names:
                pipe.zcard(Keys.model_inflight(name))
                pipe.hgetall(Keys.model_rejects(name))
            results = await pipe.execute()

        out: list[ModelRuntime] = []
        for i in range(0, len(results), 2):
            inflight = int(results[i])
            rejects, last_reject = _parse_model_rejects(results[i + 1])
            out.append(
                ModelRuntime(
                    total_inflight=inflight,
                    capacity_rejects_total=rejects,
                    last_capacity_reject=last_reject,
                )
            )
        return out

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
