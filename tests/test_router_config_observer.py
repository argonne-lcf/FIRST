"""Tests for RouterConfigObserver publish → subscribe flow."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from redis.asyncio import Redis

from first_common.schema.types import OverloadPolicy, RouterParams, UsagePolicy
from first_gateway import Settings
from first_gateway.controllers.workers.router_config_observer import (
    RouterConfigObserver,
)
from first_gateway.database.redis.router_config import (
    BackendConfig,
    DeploymentConfig,
    ModelConfig,
    RouterConfig,
)


@pytest.fixture
async def redis():  # type: ignore[no-untyped-def]
    url = Settings().redis_url
    r = Redis.from_url(url, decode_responses=True)
    await r.flushdb()
    try:
        yield r
    finally:
        await r.aclose()


def _make_client_state(redis: Redis) -> AsyncMock:
    cs = AsyncMock()
    cs.redis = redis
    cs.db_sessionmaker = AsyncMock()
    return cs


def _sample_models() -> list[ModelConfig]:
    return [
        ModelConfig(
            name="llama-70b",
            aliases=["llama"],
            allowed_groups=["all"],
            allowed_domains=[],
            supported_endpoints=["chat"],
            usage_limits=UsagePolicy(),
            overload=OverloadPolicy(),
            deployments=[
                DeploymentConfig(
                    kind="static",
                    name="dep-1",
                    router_params=RouterParams(),
                    prometheus_metrics_path=None,
                    prometheus_scrape_interval_sec=30,
                    backends=[
                        BackendConfig(
                            id="static_deployment/1",
                            model_url="http://localhost:8080/v1",
                            backend_model_name="llama-70b",
                            api_key=None,
                        )
                    ],
                )
            ],
        )
    ]


_PATCH_TARGET = "first_gateway.controllers.workers.router_config_observer.RouterConfigObserver.rebuild"


async def test_subscriber_receives_published_config(redis: Redis) -> None:
    """Observer publishes config; a RouterConfig.subscribe listener wakes and reads it."""
    cs = _make_client_state(redis)
    observer = RouterConfigObserver("router-cfg", cs, MagicMock())

    expected_models = _sample_models()

    received: list[RouterConfig] = []

    async def _collect() -> None:
        async for cfg in RouterConfig.subscribe(redis):
            received.append(cfg)
            break

    subscriber_task = asyncio.create_task(_collect())
    # Give the subscriber a moment to set up the pubsub listener
    await asyncio.sleep(0.05)

    with (
        patch(_PATCH_TARGET, new_callable=AsyncMock, return_value=expected_models),
        patch.object(RouterConfigObserver, "poll_interval", 0.05),
    ):
        observer_task = asyncio.create_task(observer.run())
        # Wait for the subscriber to receive a config
        await asyncio.wait_for(subscriber_task, timeout=2.0)
        observer_task.cancel()
        try:
            await observer_task
        except asyncio.CancelledError:
            pass

    assert len(received) == 1
    assert received[0].models == expected_models
    assert received[0].version == 1


async def test_no_publish_when_config_unchanged(redis: Redis) -> None:
    """Observer does not publish when rebuild returns the same config."""
    cs = _make_client_state(redis)
    observer = RouterConfigObserver("router-cfg", cs, MagicMock())

    models = _sample_models()

    # Seed initial config so the observer sees no diff on its first poll
    initial = RouterConfig(models=models)
    await initial.publish(redis)

    published_versions: list[int] = []
    original_publish = RouterConfig.publish

    async def _tracking_publish(self: RouterConfig, client: Redis) -> int:
        v = await original_publish(self, client)
        published_versions.append(v)
        return v

    with (
        patch(_PATCH_TARGET, new_callable=AsyncMock, return_value=models),
        patch.object(RouterConfig, "publish", _tracking_publish),
        patch.object(RouterConfigObserver, "poll_interval", 0.05),
    ):
        task = asyncio.create_task(observer.run())
        await asyncio.sleep(0.15)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    assert published_versions == []
