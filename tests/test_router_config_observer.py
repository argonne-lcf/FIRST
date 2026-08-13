"""Tests for RouterConfigObserver publish → subscribe flow."""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from redis.asyncio import Redis

from first_common.schema.types import (
    OverloadPolicy,
    ReplicaState,
    RouterParams,
    UsagePolicy,
)
from first_gateway import Settings
from first_gateway.controllers.workers.router_config_observer import (
    RouterConfigObserver,
)
from first_gateway.database.models import PilotDeployment, PilotReplica
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


def test_tara_pilot_json_matches_v1_bridge_contract() -> None:
    """Lock the V2 producer fields consumed by the V1-side bridge.

    The bridge remains in the legacy V1 gateway; its consumer contract was
    introduced on FIRST main by squash 1b5baad625bf803372e98c7e19c1fa46298d927a.
    """
    deployment_name = "tara-production/nemotron-3-ultra"
    backend_url = "https://10.20.30.40:18443/v1"
    served_model_name = "nemotron-3-ultra"
    replica = PilotReplica(
        uid=71,
        name=f"{deployment_name}/replica/00000001",
        pilot_deployment_name=deployment_name,
        state=ReplicaState.ready.value,
        model_url=backend_url,
        observed_served_name=served_model_name,
        scheduled_deletion_at=None,
    )
    deployment = PilotDeployment(
        uid=31,
        name=deployment_name,
        cluster_name="tara-production",
        model_name="nemotron-3-ultra",
        router_params={},
        prometheus_metrics_path="/metrics",
        prometheus_scrape_interval_sec=15,
        min_replicas=0,
        max_replicas=1,
        launch_spec={"served_model_name": served_model_name},
        replicas=[replica],
    )

    config = RouterConfig(
        models=[
            ModelConfig(
                name="nemotron-3-ultra",
                aliases=[],
                allowed_groups=[],
                allowed_domains=[],
                supported_endpoints=["chat/completions"],
                usage_limits=UsagePolicy(),
                overload=OverloadPolicy(),
                deployments=RouterConfigObserver._build_deployments([deployment], []),
            )
        ]
    )

    payload = json.loads(config.model_dump_json())
    model = payload["models"][0]
    pilot = model["deployments"][0]
    backend = pilot["backends"][0]
    assert {
        "model_name": model["name"],
        "allowed_groups": model["allowed_groups"],
        "allowed_domains": model["allowed_domains"],
        "deployment_kind": pilot["kind"],
        "deployment_name": pilot["name"],
        "backend_url": backend["model_url"],
        "served_model_name": backend["backend_model_name"],
    } == {
        "model_name": "nemotron-3-ultra",
        "allowed_groups": [],
        "allowed_domains": [],
        "deployment_kind": "pilot",
        "deployment_name": deployment_name,
        "backend_url": backend_url,
        "served_model_name": served_model_name,
    }
