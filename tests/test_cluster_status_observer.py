"""Tests for the ClusterStatusObserver worker."""

import asyncio
from unittest.mock import MagicMock

import pytest
from httpx import AsyncClient
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from first_common.schema.types import ClusterStatus
from first_gateway import Settings
from first_gateway.controllers.cluster_status_observer import ClusterStatusObserver
from first_gateway.database.models import Cluster
from first_gateway.database.status_store import ClusterStatusStore

# --- Importable status_method callables used by the seeded specs -------------

_CALLS: list[dict[str, object]] = []


async def _status_up(client: AsyncClient, **kwargs: object) -> ClusterStatus:
    _CALLS.append({"client": client, **kwargs})
    return ClusterStatus.up


async def _status_down(client: AsyncClient, **kwargs: object) -> ClusterStatus:
    return ClusterStatus.down


async def _status_boom(client: AsyncClient, **kwargs: object) -> ClusterStatus:
    raise RuntimeError("status endpoint unreachable")


_HERE = "tests.test_cluster_status_observer"


@pytest.fixture
async def redis():  # type: ignore[no-untyped-def]
    url = Settings().redis_url
    r = Redis.from_url(url)
    await r.flushdb()
    try:
        yield r
    finally:
        await r.aclose()


def _make_client_state(
    db: async_sessionmaker[AsyncSession],
    redis: Redis,
) -> MagicMock:
    cs = MagicMock()
    cs.db_sessionmaker = db
    cs.redis = redis
    cs.httpx_client = MagicMock(spec=AsyncClient)
    return cs


async def _run_once(observer: ClusterStatusObserver, store: ClusterStatusStore) -> None:
    await observer._poll(store)


async def test_writes_status_transition_and_timestamp(
    db: async_sessionmaker[AsyncSession],
    redis: Redis,
) -> None:
    _CALLS.clear()
    async with db.begin() as sess:
        sess.add(
            Cluster(
                name="cl-up",
                status_method=f"{_HERE}._status_up",
                status_kwargs={"status_url": "http://x", "timeout": 5},
                status=ClusterStatus.unknown.value,
            )
        )

    cs = _make_client_state(db, redis)
    observer = ClusterStatusObserver("cluster-status", cs)
    store = ClusterStatusStore(redis)
    await _run_once(observer, store)

    async with db() as sess:
        cluster = (await sess.scalars(select(Cluster))).one()
    assert cluster.status == ClusterStatus.up.value

    info = await store.get("cl-up")
    assert info.last_status_check is not None

    # httpx client is injected into the status method.
    assert _CALLS and _CALLS[0]["client"] is cs.httpx_client
    assert _CALLS[0]["status_url"] == "http://x"


async def test_no_write_when_status_unchanged(
    db: async_sessionmaker[AsyncSession],
    redis: Redis,
) -> None:
    async with db.begin() as sess:
        sess.add(
            Cluster(
                name="cl-steady",
                status_method=f"{_HERE}._status_down",
                status_kwargs={},
                status=ClusterStatus.down.value,
            )
        )

    observer = ClusterStatusObserver("cluster-status", _make_client_state(db, redis))
    store = ClusterStatusStore(redis)
    await _run_once(observer, store)

    async with db() as sess:
        cluster = (await sess.scalars(select(Cluster))).one()
    # Already down; stays down and the premised update touches nothing.
    assert cluster.status == ClusterStatus.down.value
    # last_status_check is still written even without a transition.
    assert (await store.get("cl-steady")).last_status_check is not None


async def test_failed_check_leaves_status_and_skips_timestamp(
    db: async_sessionmaker[AsyncSession],
    redis: Redis,
) -> None:
    async with db.begin() as sess:
        sess.add(
            Cluster(
                name="cl-boom",
                status_method=f"{_HERE}._status_boom",
                status_kwargs={},
                status=ClusterStatus.up.value,
            )
        )

    observer = ClusterStatusObserver("cluster-status", _make_client_state(db, redis))
    store = ClusterStatusStore(redis)
    await _run_once(observer, store)

    async with db() as sess:
        cluster = (await sess.scalars(select(Cluster))).one()
    assert cluster.status == ClusterStatus.up.value
    assert (await store.get("cl-boom")).last_status_check is None


async def test_heartbeat_beats(
    db: async_sessionmaker[AsyncSession],
    redis: Redis,
) -> None:
    observer = ClusterStatusObserver(
        "cluster-status", _make_client_state(db, redis), heartbeat_timeout=10
    )
    observer.poll_interval = 0.05

    task = asyncio.create_task(observer.run())
    await asyncio.sleep(0.15)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert not observer.check_heartbeat().timed_out
