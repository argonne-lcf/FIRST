"""
Tests that API resource routes correctly populate Redis runtime data.
"""

from __future__ import annotations

from typing import Any, AsyncGenerator

import httpx
import pytest
from redis.asyncio import Redis

from first_gateway import Settings
from first_gateway.database.redis.keys import Keys

from .fixtures.auth import ADMIN_TOKEN, auth_header
from .test_resource_apply import _apply, _load, _plan


@pytest.fixture
async def redis() -> AsyncGenerator[Redis, None]:
    r = Redis.from_url(Settings().redis_url, decode_responses=True)
    try:
        yield r
    finally:
        await r.aclose()


@pytest.fixture
async def baseline(client: httpx.AsyncClient) -> None:
    resources = _load("baseline")
    plan = await _plan(client, resources)
    await _apply(client, resources, plan)


async def _get_models(client: httpx.AsyncClient) -> list[dict[str, Any]]:
    resp = await client.get("/catalog/v1/models", headers=auth_header(ADMIN_TOKEN))
    assert resp.status_code == 200
    return resp.json()  # type: ignore[no-any-return]


async def test_list_models_runtime(
    client: httpx.AsyncClient, baseline: None, redis: Redis
) -> None:
    """list_models populates ModelRuntime from model_reservations ZSET + model_demand hash."""
    model_name = "meta-llama/llama-3-8b"
    now = 1700000000.0
    for i in range(7):
        await redis.zadd(Keys.model_inflight(model_name), {f"req-{i}": now})
    await redis.hset(
        Keys.model_rejects(model_name),
        mapping={"capacity_rejects_total": "3"},
    )

    models = await _get_models(client)
    assert len(models) == 1
    rt = models[0]["runtime"]
    assert rt["total_inflight"] == 7
    assert rt["capacity_rejects_total"] == 3


async def test_list_static_deployments_runtime(
    client: httpx.AsyncClient, baseline: None, redis: Redis
) -> None:
    """list_static_deployments populates BackendRuntime from Redis."""
    resp = await client.get(
        "/catalog/v1/deployments/static", headers=auth_header(ADMIN_TOKEN)
    )
    assert resp.status_code == 200
    sds = resp.json()
    sd_uid = sds[0]["uid"]
    backend_id = f"static_deployment/{sd_uid}"
    model_name = "meta-llama/llama-3-8b"

    now = 1700000000.0
    for i in range(10):
        await redis.zadd(
            Keys.backend_inflight(model_name, backend_id), {f"req-{i}": now}
        )
    await redis.set(Keys.backend_errors(backend_id), "4")

    resp = await client.get(
        "/catalog/v1/deployments/static", headers=auth_header(ADMIN_TOKEN)
    )
    assert resp.status_code == 200
    sds = resp.json()
    rt = sds[0]["runtime"]
    assert rt["inflight"] == 10
    assert rt["cooldown_errors"] == 4


async def test_get_pilot_deployment_replica_runtime(
    client: httpx.AsyncClient, baseline: None, redis: Redis
) -> None:
    """get_pilot_deployment populates BackendRuntime on each PilotReplica."""
    dep_name = "sophia/pilot/llama-3-8b"
    resp = await client.get(
        f"/catalog/v1/deployments/pilot/{dep_name}",
        headers=auth_header(ADMIN_TOKEN),
    )
    assert resp.status_code == 200
    detail = resp.json()
    # Baseline has no replicas, so runtime merging is a no-op — verify route still works.
    assert detail["replicas"] == []
    assert detail["name"] == dep_name
