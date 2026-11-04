"""
Tests for the temporary V1 (`/resource_server`) compatibility shim.

The DB-derived routes (list-endpoints, jobs, models) are exercised end-to-end
against a seeded database; the compat-specific helpers (deployment-name
selection) are unit-tested directly; the inference error paths and cold-start
demand signal are exercised through the routes.
"""

from __future__ import annotations

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from first_common.schema.types import HealthCheckResult, PilotDeploymentState
from first_gateway.apiserver.api import app
from first_gateway.apiserver.routes import v1_compat
from first_gateway.database import models as db
from first_gateway.database.redis.router_config import (
    BackendConfig,
    DeploymentConfig,
    ModelConfig,
    RouterConfig,
)

from .fixtures.auth import USER_TOKEN, auth_header
from .fixtures.db import LAUNCH_TEMPLATE_NAME, launch_template

# --- DB seeding --------------------------------------------------------------


async def _seed(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    allowed_groups: list[str] | None = None,
    allowed_domains: list[str] | None = None,
    pilot_state: str = PilotDeploymentState.healthy.value,
    static_health: str = HealthCheckResult.healthy.value,
) -> None:
    """Seed sophia (one model, static + pilot deployment) and an empty metis."""
    async with sessionmaker() as sess:
        sess.add(
            db.AccessGroup(
                name="open-team",
                allowed_groups=allowed_groups or [],
                allowed_domains=allowed_domains or [],
            )
        )
        sess.add(
            db.Cluster(
                name="sophia",
                health_check={},
                maintenance_notice=None,
                pilot_system=None,
            )
        )
        sess.add(
            db.Cluster(
                name="metis",
                health_check={},
                maintenance_notice=None,
                pilot_system=None,
            )
        )
        sess.add(
            db.Model(
                name="meta-llama/llama-3-8b",
                access_group_name="open-team",
                supported_endpoints=["chat/completions", "completions"],
                max_model_len=8192,
                capabilities={"tool_calling": {"supported": True}},
            )
        )
        sess.add(launch_template())
        sess.add(
            db.StaticDeployment(
                name="sophia/static/llama-3-8b",
                cluster_name="sophia",
                model_name="meta-llama/llama-3-8b",
                api_url="https://sophia.example/v1/",
                api_key=None,
                upstream_model_name="s-llama-3-8b",
                router_params={},
                health_check={"url": "https://sophia.example/health"},
                health=static_health,
                prometheus_metrics_path=None,
                prometheus_scrape_interval_sec=30,
            )
        )
        sess.add(
            db.PilotDeployment(
                name="sophia/pilot/llama-3-8b",
                cluster_name="sophia",
                model_name="meta-llama/llama-3-8b",
                launch_template_name=LAUNCH_TEMPLATE_NAME,
                router_params={},
                prometheus_metrics_path=None,
                prometheus_scrape_interval_sec=30,
                scaling_strategy=None,
                min_replicas=1,
                max_replicas=3,
                launch_spec={},
                state=pilot_state,
            )
        )
        await sess.commit()


# --- health / whoami ---------------------------------------------------------


async def test_health_is_public(client: httpx.AsyncClient) -> None:
    resp = await client.get("/resource_server/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


async def test_whoami_requires_auth(client: httpx.AsyncClient) -> None:
    assert (await client.get("/resource_server/whoami")).status_code == 401

    resp = await client.get("/resource_server/whoami", headers=auth_header(USER_TOKEN))
    assert resp.status_code == 200
    assert resp.json()["username"] == "user@anl.gov"


# --- list-endpoints ----------------------------------------------------------


async def test_list_endpoints_groups_by_cluster(
    client: httpx.AsyncClient, db: async_sessionmaker[AsyncSession]
) -> None:
    await _seed(db)

    resp = await client.get(
        "/resource_server/list-endpoints", headers=auth_header(USER_TOKEN)
    )
    assert resp.status_code == 200
    clusters = resp.json()["clusters"]
    assert set(clusters) == {"sophia"}

    sophia = clusters["sophia"]
    assert sophia["base_url"] == "resource_server/sophia"
    # sophia -> "vllm" framework stand-in.
    fw = sophia["frameworks"]["vllm"]
    assert fw["models"] == ["meta-llama/llama-3-8b"]
    assert fw["endpoints"] == ["/v1/chat/completions", "/v1/completions"]


async def test_list_endpoints_hides_inaccessible_models(
    client: httpx.AsyncClient, db: async_sessionmaker[AsyncSession]
) -> None:
    # Gated to a group nobody in the test set has.
    await _seed(db, allowed_groups=["group-nobody-has"])

    resp = await client.get(
        "/resource_server/list-endpoints", headers=auth_header(USER_TOKEN)
    )
    assert resp.status_code == 200
    assert resp.json()["clusters"] == {}


# --- jobs --------------------------------------------------------------------


async def test_jobs_buckets_by_state(
    client: httpx.AsyncClient, db: async_sessionmaker[AsyncSession]
) -> None:
    # Pilot healthy -> running; static unhealthy -> stopped.
    await _seed(
        db,
        pilot_state=PilotDeploymentState.healthy.value,
        static_health=HealthCheckResult.unhealthy.value,
    )

    resp = await client.get(
        "/resource_server/sophia/jobs", headers=auth_header(USER_TOKEN)
    )
    assert resp.status_code == 200
    body = resp.json()

    assert [j["Models"] for j in body["running"]] == ["meta-llama/llama-3-8b"]
    assert [j["Models"] for j in body["stopped"]] == ["meta-llama/llama-3-8b"]
    assert body["running"][0]["Framework"] == "vllm"
    assert body["cluster_status"] == {
        "cluster": "sophia",
        "total_models": 2,
        "live_models": 1,
        "stopped_models": 1,
    }


async def test_jobs_only_matching_cluster(
    client: httpx.AsyncClient, db: async_sessionmaker[AsyncSession]
) -> None:
    await _seed(db)

    resp = await client.get(
        "/resource_server/metis/jobs", headers=auth_header(USER_TOKEN)
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["running"] == body["stopped"] == body["queued"] == []
    assert body["cluster_status"]["total_models"] == 0


async def test_jobs_unknown_cluster_is_empty(
    client: httpx.AsyncClient, db: async_sessionmaker[AsyncSession]
) -> None:
    await _seed(db)
    resp = await client.get(
        "/resource_server/minerva/jobs", headers=auth_header(USER_TOKEN)
    )
    assert resp.status_code == 200
    assert resp.json()["cluster_status"]["total_models"] == 0


# --- cluster models ----------------------------------------------------------


async def test_cluster_models_v1_shape(
    client: httpx.AsyncClient, db: async_sessionmaker[AsyncSession]
) -> None:
    """The shape V1 `alcf-ai agent configure` reads: id, framework, capabilities."""
    await _seed(db)

    resp = await client.get(
        "/resource_server/sophia/models", headers=auth_header(USER_TOKEN)
    )
    assert resp.status_code == 200
    assert resp.json() == [
        {
            "id": "meta-llama/llama-3-8b",
            "object": "model",
            "cluster": "sophia",
            "framework": "vllm",
            # No display name set: falls back to the id, like /federated/v1/models.
            "display_name": "meta-llama/llama-3-8b",
            "capabilities": {
                "tool_calling": {"supported": True},
                "api_protocols": ["chat_completions", "completions"],
                "context_window_tokens": 8192,
            },
        }
    ]


async def test_cluster_models_hides_inaccessible(
    client: httpx.AsyncClient, db: async_sessionmaker[AsyncSession]
) -> None:
    await _seed(db, allowed_groups=["group-nobody-has"])
    resp = await client.get(
        "/resource_server/sophia/models", headers=auth_header(USER_TOKEN)
    )
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.parametrize("cluster", ["metis", "minerva"])
async def test_cluster_models_empty_or_unknown_cluster_is_empty(
    client: httpx.AsyncClient, db: async_sessionmaker[AsyncSession], cluster: str
) -> None:
    """
    Known-but-empty (metis) and unknown (minerva) clusters both list nothing.
    Not a 404: V1 `agent configure` walks a hard-coded cluster allowlist that
    includes clusters V2 may not have, and aborts on any error.
    """
    await _seed(db)
    resp = await client.get(
        f"/resource_server/{cluster}/models", headers=auth_header(USER_TOKEN)
    )
    assert resp.status_code == 200
    assert resp.json() == []


# --- helper units ------------------------------------------------------------


def test_framework_standin() -> None:
    assert v1_compat._framework("sophia") == "vllm"
    assert v1_compat._framework("metis") == "api"


def _deployment(
    name: str, cluster: str, backends: list[BackendConfig]
) -> DeploymentConfig:
    return DeploymentConfig(
        kind="pilot",
        name=name,
        cluster_name=cluster,
        router_params={},  # type: ignore[arg-type]
        prometheus_metrics_path=None,
        prometheus_scrape_interval_sec=30,
        backends=backends,
    )


def _model(deployments: list[DeploymentConfig]) -> ModelConfig:
    return ModelConfig(
        name="m",
        aliases=["m-alias"],
        allowed_groups=[],
        allowed_domains=[],
        supported_endpoints=["chat/completions"],
        usage_limits={},  # type: ignore[arg-type]
        overload={},  # type: ignore[arg-type]
        deployments=deployments,
    )


def _backend() -> BackendConfig:
    return BackendConfig(
        id="b1",
        model_url="http://localhost:9/v1",
        backend_model_name="upstream",
        api_key=None,
    )


def test_cluster_deployment_prefers_live() -> None:
    model = _model(
        [
            _deployment("sophia/cold", "sophia", []),
            _deployment("sophia/live", "sophia", [_backend()]),
            _deployment("metis/live", "metis", [_backend()]),
        ]
    )
    assert v1_compat._cluster_deployment(model, "sophia") == "sophia/live"


def test_cluster_deployment_falls_back_to_cold() -> None:
    """A cold deployment is still pinned, so admission records the demand."""
    model = _model([_deployment("sophia/cold", "sophia", [])])
    assert v1_compat._cluster_deployment(model, "sophia") == "sophia/cold"


def test_cluster_deployment_none_off_cluster_or_unknown() -> None:
    model = _model([_deployment("sophia/live", "sophia", [_backend()])])
    assert v1_compat._cluster_deployment(model, "metis") is None
    assert v1_compat._cluster_deployment(None, "sophia") is None


# --- inference routes ----------------------------------------------------------

_CHAT = {"messages": [{"role": "user", "content": "hi"}], "max_tokens": 5}


@pytest.fixture
def cold_model_config() -> None:
    """Pin the RouterConfig: model `m` has only a cold deployment on sophia."""
    app.state.router_config_manager._current = RouterConfig(
        version=999, models=[_model([_deployment("sophia/cold", "sophia", [])])]
    )


@pytest.mark.parametrize(
    ("path", "model", "detail"),
    [
        ("sophia", "no-such-model", "does not exist"),
        ("metis", "m", "not deployed on cluster"),
    ],
)
async def test_inference_not_found(
    client: httpx.AsyncClient,
    db: async_sessionmaker[AsyncSession],
    cold_model_config: None,
    path: str,
    model: str,
    detail: str,
) -> None:
    """Unknown cluster / model / model-not-on-cluster are 404s, not 503s."""
    await _seed(db)
    resp = await client.post(
        f"/resource_server/{path}/api/v1/chat/completions",
        json={"model": model, **_CHAT},
        headers=auth_header(USER_TOKEN),
    )
    assert resp.status_code == 404, resp.text
    assert detail in resp.json()["error"]["message"]


async def test_inference_cold_deployment_records_demand(
    client: httpx.AsyncClient,
    db: async_sessionmaker[AsyncSession],
    cold_model_config: None,
) -> None:
    """A cold model on the cluster gets the native cold-start 503, and the
    capacity reject the autoscaler scales on is recorded."""
    await _seed(db)
    repo = app.state.client_state.redis_repo
    before = (await repo.get_model_runtime("m")).capacity_rejects_total

    resp = await client.post(
        "/resource_server/sophia/vllm/v1/chat/completions",
        json={"model": "m-alias", **_CHAT},
        headers=auth_header(USER_TOKEN),
    )
    assert resp.status_code == 503, resp.text
    assert (await repo.get_model_runtime("m")).capacity_rejects_total == before + 1
