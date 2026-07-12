"""Tests for the unified HealthObserver worker."""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from first_common.schema.types import HealthCheckResult
from first_gateway import Settings
from first_gateway.controllers.workers.health_observer import HealthObserver
from first_gateway.database.models import (
    AccessGroup,
    Cluster,
    Model,
    StaticDeployment,
)

_PATCH_TARGET = "first_gateway.controllers.workers.health_observer.perform_health_check"


@pytest.fixture
async def redis():  # type: ignore[no-untyped-def]
    url = Settings().redis_url
    r = Redis.from_url(url, decode_responses=True)
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


def _observer(
    db: async_sessionmaker[AsyncSession],
    redis: Redis,
    *,
    heartbeat_timeout: float = 120.0,
) -> HealthObserver:
    return HealthObserver(
        "health", _make_client_state(db, redis), heartbeat_timeout=heartbeat_timeout
    )


async def _seed_parents(sess: AsyncSession) -> None:
    sess.add(AccessGroup(name="ag", allowed_groups=[], allowed_domains=[]))
    sess.add(Cluster(name="cl", health_check={"url": "", "debounce": 2}))
    await sess.flush()
    sess.add(Model(name="mdl", access_group_name="ag", supported_endpoints=["chat"]))
    await sess.flush()


def _static(name: str, health: str, *, debounce: int = 2) -> StaticDeployment:
    return StaticDeployment(
        name=name,
        cluster_name="cl",
        model_name="mdl",
        api_url="http://localhost:8080",
        upstream_model_name="llama",
        router_params={},
        health_check={"url": "http://localhost:8080/health", "debounce": debounce},
        prometheus_scrape_interval_sec=30,
        health=health,
    )


async def test_cluster_health_transition(
    db: async_sessionmaker[AsyncSession],
    redis: Redis,
) -> None:
    async with db.begin() as sess:
        sess.add(
            Cluster(
                name="cl-up",
                health_check={"url": "http://x/health", "debounce": 2},
                health=HealthCheckResult.unknown.value,
            )
        )

    observer = _observer(db, redis)
    with patch(
        _PATCH_TARGET, new_callable=AsyncMock, return_value=HealthCheckResult.healthy
    ):
        await observer._poll()

    async with db() as sess:
        cluster = (
            await sess.scalars(select(Cluster).where(Cluster.name == "cl-up"))
        ).one()
    assert cluster.health == HealthCheckResult.healthy.value


async def test_static_deployment_health_transition(
    db: async_sessionmaker[AsyncSession],
    redis: Redis,
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        sess.add(_static("sd-ok", HealthCheckResult.unknown.value))

    observer = _observer(db, redis)
    with patch(
        _PATCH_TARGET, new_callable=AsyncMock, return_value=HealthCheckResult.healthy
    ):
        await observer._poll()

    async with db() as sess:
        dep = (await sess.scalars(select(StaticDeployment))).one()
    assert dep.health == HealthCheckResult.healthy.value


async def test_unhealthy_debounced(
    db: async_sessionmaker[AsyncSession],
    redis: Redis,
) -> None:
    """First unhealthy poll is debounced; second consecutive failure transitions."""
    async with db.begin() as sess:
        sess.add(
            Cluster(
                name="cl-boom",
                health_check={"url": "http://x/health", "debounce": 2},
                health=HealthCheckResult.healthy.value,
            )
        )

    observer = _observer(db, redis)
    with patch(
        _PATCH_TARGET, new_callable=AsyncMock, return_value=HealthCheckResult.unhealthy
    ):
        await observer._poll()
        async with db() as sess:
            cluster = (await sess.scalars(select(Cluster))).one()
        assert cluster.health == HealthCheckResult.healthy.value

        await observer._poll()

    async with db() as sess:
        cluster = (await sess.scalars(select(Cluster))).one()
    assert cluster.health == HealthCheckResult.unhealthy.value


async def test_no_write_when_health_unchanged(
    db: async_sessionmaker[AsyncSession],
    redis: Redis,
) -> None:
    async with db.begin() as sess:
        sess.add(
            Cluster(
                name="cl-steady",
                health_check={"url": "http://x/health", "debounce": 2},
                health=HealthCheckResult.healthy.value,
            )
        )

    observer = _observer(db, redis)
    with patch(
        _PATCH_TARGET, new_callable=AsyncMock, return_value=HealthCheckResult.healthy
    ):
        await observer._poll()

    async with db() as sess:
        cluster = (await sess.scalars(select(Cluster))).one()
    assert cluster.health == HealthCheckResult.healthy.value


async def test_recovery_clears_debounce(
    db: async_sessionmaker[AsyncSession],
    redis: Redis,
) -> None:
    """A healthy result after failures resets the debounce counter."""
    async with db.begin() as sess:
        sess.add(
            Cluster(
                name="cl-flap",
                health_check={"url": "http://x/health", "debounce": 2},
                health=HealthCheckResult.healthy.value,
            )
        )

    observer = _observer(db, redis)
    mock = AsyncMock(return_value=HealthCheckResult.unhealthy)
    with patch(_PATCH_TARGET, mock):
        await observer._poll()

    mock.return_value = HealthCheckResult.healthy
    with patch(_PATCH_TARGET, mock):
        await observer._poll()

    async with db() as sess:
        cluster = (await sess.scalars(select(Cluster))).one()
    assert cluster.health == HealthCheckResult.healthy.value

    with patch(
        _PATCH_TARGET, new_callable=AsyncMock, return_value=HealthCheckResult.unhealthy
    ):
        await observer._poll()
        async with db() as sess:
            cluster = (await sess.scalars(select(Cluster))).one()
        assert cluster.health == HealthCheckResult.healthy.value, (
            "single failure after recovery should still be debounced"
        )


async def test_heartbeat_beats(
    db: async_sessionmaker[AsyncSession],
    redis: Redis,
) -> None:
    observer = _observer(db, redis, heartbeat_timeout=10)
    observer.poll_interval = 0.05

    task = asyncio.create_task(observer.run())
    await asyncio.sleep(0.15)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert not observer.check_heartbeat().timed_out
