"""Tests for the DeploymentLoadObserver worker."""

import asyncio
from collections import deque
from unittest.mock import MagicMock

import pytest
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from first_gateway import Settings
from first_gateway.controllers.load_observer import (
    PILOT_DEPLOYMENT_PREFIX,
    DeploymentLoadObserver,
    _compute_status,
)
from first_gateway.database.inflight import AsyncInflightCounter
from first_gateway.database.models import (
    AccessGroup,
    Cluster,
    Model,
    PilotDeployment,
    StaticDeployment,
)
from first_gateway.database.status_store import (
    PilotDeploymentStatusStore,
    StaticDeploymentStatusStore,
)


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
    return cs


async def _seed(sess: AsyncSession) -> None:
    sess.add(AccessGroup(name="ag", allowed_groups=[], allowed_domains=[]))
    sess.add(Cluster(name="cl", status_method="none", status_kwargs={}))
    await sess.flush()
    sess.add(Model(name="mdl", access_group_name="ag", supported_endpoints=["chat"]))
    await sess.flush()
    sess.add(
        PilotDeployment(
            name="pd-alpha",
            cluster_name="cl",
            model_name="mdl",
            router_params={},
            health_check_method="none",
            health_check_kwargs={},
            prometheus_scrape_interval_sec=30,
            min_replicas=0,
            max_replicas=1,
            launch_spec={},
        )
    )
    sess.add(
        StaticDeployment(
            name="sd-beta",
            cluster_name="cl",
            model_name="mdl",
            api_url="http://localhost:8080",
            upstream_model_name="llama",
            router_params={},
            health_check_method="none",
            health_check_kwargs={},
            prometheus_scrape_interval_sec=30,
        )
    )
    await sess.flush()


def test_compute_status_single_sample() -> None:
    buf: deque[float] = deque([5.0], maxlen=30)
    s = _compute_status(buf)
    assert s.load_avg_1m == 5.0
    assert s.load_avg_5m == 5.0
    assert s.load_max_1m == 5.0
    assert s.load_max_5m == 5.0


def test_compute_status_uses_correct_windows() -> None:
    buf: deque[float] = deque(range(30), maxlen=30)
    s = _compute_status(buf)
    assert s.load_avg_1m == pytest.approx(sum(range(24, 30)) / 6)
    assert s.load_avg_5m == pytest.approx(sum(range(30)) / 30)
    assert s.load_max_1m == 29.0
    assert s.load_max_5m == 29.0


def test_compute_status_partial_buffer() -> None:
    buf: deque[float] = deque([1.0, 3.0, 5.0], maxlen=30)
    s = _compute_status(buf)
    assert s.load_avg_1m == 3.0
    assert s.load_avg_5m == 3.0
    assert s.load_max_1m == 5.0
    assert s.load_max_5m == 5.0


async def test_observer_polls_and_writes_status(
    db: async_sessionmaker[AsyncSession],
    redis: Redis,
) -> None:
    async with db.begin() as sess:
        await _seed(sess)

    counter = AsyncInflightCounter(redis)
    async with counter.track(f"{PILOT_DEPLOYMENT_PREFIX}:pd-alpha"):
        async with counter.track(f"{PILOT_DEPLOYMENT_PREFIX}:pd-alpha"):
            observer = DeploymentLoadObserver(
                "deployment-load", _make_client_state(db, redis)
            )
            observer.poll_interval = 0.05

            task = asyncio.create_task(observer.run())
            await asyncio.sleep(0.2)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    pilot_store = PilotDeploymentStatusStore(redis)
    static_store = StaticDeploymentStatusStore(redis)

    pilot_status = await pilot_store.get("pd-alpha")
    assert pilot_status.load_avg_1m == pytest.approx(2.0)

    static_status = await static_store.get("sd-beta")
    assert static_status.load_avg_1m == 0.0


async def test_observer_heartbeat(
    db: async_sessionmaker[AsyncSession],
    redis: Redis,
) -> None:
    async with db.begin() as sess:
        await _seed(sess)

    observer = DeploymentLoadObserver(
        "deployment-load", _make_client_state(db, redis), heartbeat_timeout=10
    )
    observer.poll_interval = 0.05

    task = asyncio.create_task(observer.run())
    await asyncio.sleep(0.15)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    status = observer.check_heartbeat()
    assert not status.timed_out
