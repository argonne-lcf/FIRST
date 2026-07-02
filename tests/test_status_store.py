import asyncio

import pytest
from pydantic import BaseModel
from redis.asyncio import Redis

from first_common.errors import StatusCASFailed
from first_gateway import Settings
from first_gateway.status_store import StatusStore


class FakeStatus(BaseModel):
    count: int = 0
    label: str = ""


class FakeStatusStore(StatusStore[FakeStatus]):
    resource = "fake"
    model = FakeStatus
    ttl_seconds = 60


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
def store(redis: Redis) -> FakeStatusStore:
    return FakeStatusStore(redis)


async def test_get_returns_default_when_cold(store: FakeStatusStore) -> None:
    result = await store.get("nonexistent")
    assert result == FakeStatus()
    assert result.count == 0
    assert result.label == ""


async def test_set_and_get(store: FakeStatusStore) -> None:
    status = FakeStatus(count=42, label="hello")
    await store.update("x", status)
    result = await store.get("x")
    assert result == status


async def test_get_many_mixed(store: FakeStatusStore) -> None:
    await store.update("a", FakeStatus(count=1))
    result = await store.get_many(["a", "missing"])
    assert result["a"].count == 1
    assert result["missing"] == FakeStatus()


async def test_get_many_empty(store: FakeStatusStore) -> None:
    assert await store.get_many([]) == {}


async def test_update_merges_fields(store: FakeStatusStore) -> None:
    await store.update("x", FakeStatus(count=10, label="orig"))
    merged = await store.update("x", FakeStatus(count=20))
    assert merged.count == 20
    assert merged.label == "orig"


async def test_update_creates_when_cold(store: FakeStatusStore) -> None:
    merged = await store.update("new", FakeStatus(label="fresh"))
    assert merged.label == "fresh"
    assert merged.count == 0


async def test_update_cas_retries_on_contention(
    redis: Redis, store: FakeStatusStore
) -> None:
    await store.update("x", FakeStatus(count=0))

    async def bump() -> FakeStatus:
        return await store.update("x", FakeStatus(count=1))

    results = await asyncio.gather(bump(), bump(), bump())
    assert all(r.count == 1 for r in results)

    final = await store.get("x")
    assert final.count == 1


async def test_delete(store: FakeStatusStore) -> None:
    await store.update("x", FakeStatus(count=1))
    await store.delete("x")
    assert await store.get("x") == FakeStatus()


async def test_key_namespace(store: FakeStatusStore) -> None:
    assert store._key("myname") == "status:fake:myname"


class _SmallCASStore(StatusStore[FakeStatus]):
    resource = "fake"
    model = FakeStatus
    ttl_seconds = 60
    max_cas_attempts = 2


async def test_update_cas_exhaustion(redis: Redis) -> None:
    """When every CAS attempt loses, StatusCASFailed is raised."""
    store = _SmallCASStore(redis)
    await store.update("x", FakeStatus(count=0))

    original_get = redis.get
    call_count = 0

    async def get_then_mutate(key, *args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal call_count
        val = await original_get(key, *args, **kwargs)
        call_count += 1
        await redis.set(key, FakeStatus(count=call_count).model_dump_json())
        return val

    redis.get = get_then_mutate  # type: ignore[assignment]
    try:
        with pytest.raises(StatusCASFailed):
            await store.update("x", FakeStatus(label="nope"))
    finally:
        redis.get = original_get  # type: ignore[method-assign]
