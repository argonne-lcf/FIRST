import pytest
from redis.asyncio import Redis

from first_gateway import Settings
from first_gateway.database.inflight import AsyncInflightCounter


@pytest.fixture
async def redis():  # type: ignore[no-untyped-def]
    url = Settings().redis_url
    r = Redis.from_url(url)
    await r.flushdb()
    try:
        yield r
    finally:
        await r.aclose()


@pytest.fixture
def counter(redis: Redis) -> AsyncInflightCounter:
    return AsyncInflightCounter(redis, max_request_seconds=10)


async def test_count_returns_zero_when_empty(counter: AsyncInflightCounter) -> None:
    assert await counter.count("pilot_deployment:x") == 0


async def test_track_increments_and_decrements(counter: AsyncInflightCounter) -> None:
    key = "pilot_deployment:my-model"
    async with counter.track(key) as n:
        assert n == 1
        assert await counter.count(key) == 1
    assert await counter.count(key) == 0


async def test_track_concurrent(counter: AsyncInflightCounter) -> None:
    key = "static_deployment:llama"
    async with counter.track(key) as n1:
        assert n1 == 1
        async with counter.track(key) as n2:
            assert n2 == 2
            assert await counter.count(key) == 2
        assert await counter.count(key) == 1
    assert await counter.count(key) == 0


async def test_add_without_context_manager(counter: AsyncInflightCounter) -> None:
    key = "pilot_deployment:offline"
    n = await counter.add(key)
    assert n == 1
    assert await counter.count(key) == 1


async def test_different_keys_are_independent(counter: AsyncInflightCounter) -> None:
    async with counter.track("pilot_deployment:a"):
        async with counter.track("static_deployment:a"):
            assert await counter.count("pilot_deployment:a") == 1
            assert await counter.count("static_deployment:a") == 1


async def test_key_namespace(counter: AsyncInflightCounter) -> None:
    assert counter._zkey("pilot_deployment:x") == "inflight:pilot_deployment:x"
