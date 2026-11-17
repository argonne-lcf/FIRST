"""The gateway's Redis pool waits for a free connection instead of failing."""

import asyncio
import time

import pytest
from redis.asyncio import BlockingConnectionPool
from redis.exceptions import ConnectionError as RedisConnectionError

from first_gateway import Settings

HOLD_SEC = 0.2  # each BLPOP on an empty key keeps its connection this long


def _settings(max_connections: int, timeout: float) -> Settings:
    return Settings().model_copy(
        update={"redis_max_connections": max_connections, "redis_pool_timeout": timeout}
    )


async def _hold(redis_key: str, settings: Settings, users: int) -> None:
    redis = settings.build_redis()
    try:
        await asyncio.gather(
            *(redis.blpop([redis_key], timeout=HOLD_SEC) for _ in range(users))
        )
    finally:
        await redis.aclose()


def test_pool_is_bounded_and_blocking() -> None:
    redis = _settings(7, 1.5).build_redis()
    pool = redis.connection_pool
    assert isinstance(pool, BlockingConnectionPool)
    assert pool.max_connections == 7
    assert pool.timeout == 1.5


async def test_more_users_than_connections_wait_instead_of_failing() -> None:
    # 6 concurrent calls on 2 connections: three rounds of HOLD_SEC, no errors.
    start = time.perf_counter()
    await _hold("test:redis-pool:wait", _settings(2, 5.0), users=6)
    assert time.perf_counter() - start >= 3 * HOLD_SEC * 0.9


async def test_wait_is_bounded_by_pool_timeout() -> None:
    with pytest.raises(RedisConnectionError, match="No connection available"):
        await _hold("test:redis-pool:timeout", _settings(1, HOLD_SEC / 4), users=2)
