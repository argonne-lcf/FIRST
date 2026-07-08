"""Tests for the ClusterHealthObserver worker."""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from first_common.schema.types import HealthCheckResult
from first_gateway import Settings
from first_gateway.controllers.cluster_health_observer import ClusterHealthObserver
from first_gateway.database.models import Cluster


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
) -> AsyncMock:
    cs = AsyncMock()
    cs.db_sessionmaker = db
    cs.redis = redis
    cs.httpx_client = AsyncMock(spec=AsyncClient)
    return cs


async def _run_once(observer: ClusterHealthObserver) -> None:
    await observer._poll()


async def test_writes_health_transition(
    db: async_sessionmaker[AsyncSession],
    redis: Redis,
) -> None:
    async with db.begin() as sess:
        sess.add(
            Cluster(
                name="cl-up",
                health_check={"health_url": "http://x/health"},
                health=HealthCheckResult.unknown.value,
            )
        )

    cs = _make_client_state(db, redis)
    observer = ClusterHealthObserver("cluster-health", cs)
    with patch(
        "first_gateway.controllers.cluster_health_observer.perform_health_check",
        new_callable=AsyncMock,
        return_value=HealthCheckResult.healthy,
    ):
        await _run_once(observer)

    async with db() as sess:
        cluster = (await sess.scalars(select(Cluster))).one()
    assert cluster.health == HealthCheckResult.healthy.value


async def test_no_write_when_health_unchanged(
    db: async_sessionmaker[AsyncSession],
    redis: Redis,
) -> None:
    async with db.begin() as sess:
        sess.add(
            Cluster(
                name="cl-steady",
                health_check={"health_url": "http://x/health"},
                health=HealthCheckResult.unhealthy.value,
            )
        )

    observer = ClusterHealthObserver("cluster-health", _make_client_state(db, redis))
    with patch(
        "first_gateway.controllers.cluster_health_observer.perform_health_check",
        new_callable=AsyncMock,
        return_value=HealthCheckResult.unhealthy,
    ):
        await _run_once(observer)

    async with db() as sess:
        cluster = (await sess.scalars(select(Cluster))).one()
    assert cluster.health == HealthCheckResult.unhealthy.value


async def test_unhealthy_check_transitions_health(
    db: async_sessionmaker[AsyncSession],
    redis: Redis,
) -> None:
    async with db.begin() as sess:
        sess.add(
            Cluster(
                name="cl-boom",
                health_check={"health_url": "http://x/health"},
                health=HealthCheckResult.healthy.value,
            )
        )

    observer = ClusterHealthObserver("cluster-health", _make_client_state(db, redis))
    with patch(
        "first_gateway.controllers.cluster_health_observer.perform_health_check",
        new_callable=AsyncMock,
        return_value=HealthCheckResult.unhealthy,
    ):
        await _run_once(observer)

    async with db() as sess:
        cluster = (await sess.scalars(select(Cluster))).one()
    assert cluster.health == HealthCheckResult.unhealthy.value


async def test_heartbeat_beats(
    db: async_sessionmaker[AsyncSession],
    redis: Redis,
) -> None:
    observer = ClusterHealthObserver(
        "cluster-health", _make_client_state(db, redis), heartbeat_timeout=10
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
