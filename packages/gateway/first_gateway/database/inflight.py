import contextlib
import uuid
from typing import AsyncIterator

from redis.asyncio import Redis

_START_SCRIPT = """
local key = KEYS[1]
local request_id = ARGV[1]
local ttl = tonumber(ARGV[2])

local now = tonumber(redis.call('TIME')[1])
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
redis.call('ZADD', key, now + ttl, request_id)
redis.call('EXPIRE', key, ttl * 2)
return redis.call('ZCARD', key)
"""

_READ_SCRIPT = """
local key = KEYS[1]
local now = tonumber(redis.call('TIME')[1])
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
return redis.call('ZCARD', key)
"""


class AsyncInflightCounter:
    """Tracks concurrent in-flight requests per key using a Redis sorted set.

    Each tracked request is a ZADD member with score = expiry timestamp.
    Self-cleaning: expired entries are pruned on every read/start.

    Keys are stored under ``inflight:{key}`` in Redis.  Callers should
    use type-qualified keys to avoid collisions across resource types,
    e.g. ``counter.track("pilot_deployment:my-llama")``.
    """

    def __init__(self, client: Redis, max_request_seconds: int = 300) -> None:
        self.client = client
        self.ttl = max_request_seconds
        self._start = client.register_script(_START_SCRIPT)
        self._read = client.register_script(_READ_SCRIPT)

    def _zkey(self, key: str) -> str:
        return f"inflight:{key}"

    @contextlib.asynccontextmanager
    async def track(self, key: str) -> AsyncIterator[int]:
        """Track a request as in-flight for the duration of the context."""
        request_id = uuid.uuid4().hex
        zkey = self._zkey(key)
        count = await self._start(keys=[zkey], args=[request_id, self.ttl])
        try:
            yield int(count)
        finally:
            await self.client.zrem(zkey, request_id)

    async def add(self, key: str) -> int:
        """Register an in-flight entry without a context manager (e.g. for
        cold-start demand signaling where the request 503s immediately)."""
        request_id = uuid.uuid4().hex
        zkey = self._zkey(key)
        return int(await self._start(keys=[zkey], args=[request_id, self.ttl]))

    async def count(self, key: str) -> int:
        """Current in-flight count (after pruning expired entries)."""
        return int(await self._read(keys=[self._zkey(key)]))
