"""Tests for the StaticDeploymentHealthObserver worker."""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from first_common.schema.types import HealthCheckResult
from first_gateway import Settings
from first_gateway.controllers.static_health_observer import (
    StaticDeploymentHealthObserver,
)
from first_gateway.database.models import (
    AccessGroup,
    Cluster,
    Model,
    StaticDeployment,
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
) -> AsyncMock:
    cs = AsyncMock()
    cs.db_sessionmaker = db
    cs.redis = redis
    cs.httpx_client = AsyncMock(spec=AsyncClient)
    return cs


async def _seed_parents(sess: AsyncSession) -> None:
    sess.add(AccessGroup(name="ag", allowed_groups=[], allowed_domains=[]))
    sess.add(Cluster(name="cl", health_check={"health_url": ""}))
    await sess.flush()
    sess.add(Model(name="mdl", access_group_name="ag", supported_endpoints=["chat"]))
    await sess.flush()


def _static(name: str, health: str) -> StaticDeployment:
    return StaticDeployment(
        name=name,
        cluster_name="cl",
        model_name="mdl",
        api_url="http://localhost:8080",
        upstream_model_name="llama",
        router_params={},
        health_check={"health_url": "http://localhost:8080/health"},
        prometheus_scrape_interval_sec=30,
        health=health,
    )


_PATCH_TARGET = "first_gateway.controllers.static_health_observer.perform_health_check"


async def test_writes_health_transition(
    db: async_sessionmaker[AsyncSession],
    redis: Redis,
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        sess.add(_static("sd-ok", HealthCheckResult.unknown.value))

    cs = _make_client_state(db, redis)
    observer = StaticDeploymentHealthObserver("static-health", cs)
    with patch(
        _PATCH_TARGET, new_callable=AsyncMock, return_value=HealthCheckResult.healthy
    ):
        await observer._poll()

    async with db() as sess:
        dep = (await sess.scalars(select(StaticDeployment))).one()
    assert dep.health == HealthCheckResult.healthy.value


async def test_unhealthy_transition(
    db: async_sessionmaker[AsyncSession],
    redis: Redis,
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        sess.add(_static("sd-bad", HealthCheckResult.healthy.value))

    observer = StaticDeploymentHealthObserver(
        "static-health", _make_client_state(db, redis)
    )
    with patch(
        _PATCH_TARGET, new_callable=AsyncMock, return_value=HealthCheckResult.unhealthy
    ):
        await observer._poll()

    async with db() as sess:
        dep = (await sess.scalars(select(StaticDeployment))).one()
    assert dep.health == HealthCheckResult.unhealthy.value


async def test_no_write_when_health_unchanged(
    db: async_sessionmaker[AsyncSession],
    redis: Redis,
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        sess.add(_static("sd-steady", HealthCheckResult.healthy.value))

    observer = StaticDeploymentHealthObserver(
        "static-health", _make_client_state(db, redis)
    )
    with patch(
        _PATCH_TARGET, new_callable=AsyncMock, return_value=HealthCheckResult.healthy
    ):
        await observer._poll()

    async with db() as sess:
        dep = (await sess.scalars(select(StaticDeployment))).one()
    assert dep.health == HealthCheckResult.healthy.value


async def test_heartbeat_beats(
    db: async_sessionmaker[AsyncSession],
    redis: Redis,
) -> None:
    observer = StaticDeploymentHealthObserver(
        "static-health", _make_client_state(db, redis), heartbeat_timeout=10
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
