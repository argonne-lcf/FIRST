"""Tests for the StaticDeploymentHealthObserver worker."""

import asyncio
from unittest.mock import MagicMock

import pytest
from httpx import AsyncClient
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from first_common.schema.types import DeploymentHealth, HealthEndpointStatus
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
from first_gateway.database.status_store import StaticDeploymentStatusStore

# --- Importable health_check_method callables used by the seeded specs -------

_CALLS: list[dict[str, object]] = []


async def _healthy(client: AsyncClient, **kwargs: object) -> HealthEndpointStatus:
    _CALLS.append({"client": client, **kwargs})
    return HealthEndpointStatus.healthy


async def _unhealthy(client: AsyncClient, **kwargs: object) -> HealthEndpointStatus:
    return HealthEndpointStatus.unhealthy


async def _boom(client: AsyncClient, **kwargs: object) -> HealthEndpointStatus:
    raise RuntimeError("health endpoint unreachable")


_HERE = "tests.test_static_health_observer"


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


async def _seed_parents(sess: AsyncSession) -> None:
    sess.add(AccessGroup(name="ag", allowed_groups=[], allowed_domains=[]))
    sess.add(Cluster(name="cl", status_method="x.y", status_kwargs={}))
    await sess.flush()
    sess.add(Model(name="mdl", access_group_name="ag", supported_endpoints=["chat"]))
    await sess.flush()


def _static(name: str, method: str, health: str) -> StaticDeployment:
    return StaticDeployment(
        name=name,
        cluster_name="cl",
        model_name="mdl",
        api_url="http://localhost:8080",
        upstream_model_name="llama",
        router_params={},
        health_check_method=method,
        health_check_kwargs={"health_path": "/health", "timeout": 5},
        prometheus_scrape_interval_sec=30,
        health=health,
    )


async def test_writes_health_transition_and_timestamp(
    db: async_sessionmaker[AsyncSession],
    redis: Redis,
) -> None:
    _CALLS.clear()
    async with db.begin() as sess:
        await _seed_parents(sess)
        sess.add(_static("sd-ok", f"{_HERE}._healthy", DeploymentHealth.offline.value))

    cs = _make_client_state(db, redis)
    observer = StaticDeploymentHealthObserver("static-health", cs)
    store = StaticDeploymentStatusStore(redis)
    await observer._poll(store)

    async with db() as sess:
        dep = (await sess.scalars(select(StaticDeployment))).one()
    assert dep.health == DeploymentHealth.healthy.value
    assert (await store.get("sd-ok")).last_health_check is not None

    # client and base_url are injected; kwargs are passed through.
    assert _CALLS and _CALLS[0]["client"] is cs.httpx_client
    assert _CALLS[0]["base_url"] == "http://localhost:8080"
    assert _CALLS[0]["health_path"] == "/health"


async def test_unhealthy_transition(
    db: async_sessionmaker[AsyncSession],
    redis: Redis,
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        sess.add(
            _static("sd-bad", f"{_HERE}._unhealthy", DeploymentHealth.healthy.value)
        )

    observer = StaticDeploymentHealthObserver(
        "static-health", _make_client_state(db, redis)
    )
    await observer._poll(StaticDeploymentStatusStore(redis))

    async with db() as sess:
        dep = (await sess.scalars(select(StaticDeployment))).one()
    assert dep.health == DeploymentHealth.unhealthy.value


async def test_failed_check_leaves_health_and_skips_timestamp(
    db: async_sessionmaker[AsyncSession],
    redis: Redis,
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        sess.add(_static("sd-boom", f"{_HERE}._boom", DeploymentHealth.healthy.value))

    observer = StaticDeploymentHealthObserver(
        "static-health", _make_client_state(db, redis)
    )
    store = StaticDeploymentStatusStore(redis)
    await observer._poll(store)

    async with db() as sess:
        dep = (await sess.scalars(select(StaticDeployment))).one()
    assert dep.health == DeploymentHealth.healthy.value
    assert (await store.get("sd-boom")).last_health_check is None


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
