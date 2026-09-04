"""
Tests for the temporary V1 (`/resource_server`) compatibility shim.

The DB-derived routes (list-endpoints, jobs, models) are exercised end-to-end
against a seeded database; the compat-specific helpers (deployment-name
resolution and the Globus staging area) are unit-tested directly.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from first_common.errors import ServiceUnavailable
from first_common.schema.types import HealthCheckResult, PilotDeploymentState
from first_gateway.apiserver.routes import v1_compat
from first_gateway.database import models as db
from first_gateway.database.redis.router_config import (
    BackendConfig,
    DeploymentConfig,
    ModelConfig,
    RouterConfig,
)

from .fixtures.auth import ADMIN_TOKEN, USER_TOKEN, auth_header

# --- DB seeding --------------------------------------------------------------


async def _seed(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    allowed_groups: list[str] | None = None,
    allowed_domains: list[str] | None = None,
    pilot_state: str = PilotDeploymentState.healthy.value,
    static_health: str = HealthCheckResult.healthy.value,
) -> None:
    """Seed one cluster (sophia) with a single model, static + pilot deployment."""
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
            db.Model(
                name="meta-llama/llama-3-8b",
                access_group_name="open-team",
                supported_endpoints=["chat/completions", "completions"],
            )
        )
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


# --- cluster models ----------------------------------------------------------


async def test_cluster_models_filtered(
    client: httpx.AsyncClient, db: async_sessionmaker[AsyncSession]
) -> None:
    await _seed(db)

    resp = await client.get(
        "/resource_server/sophia/models", headers=auth_header(ADMIN_TOKEN)
    )
    assert resp.status_code == 200
    [model] = resp.json()
    assert model["name"] == "meta-llama/llama-3-8b"
    assert len(model["static_deployments"]) == 1
    assert len(model["pilot_deployments"]) == 1

    # A cluster with no deployments yields nothing.
    other = await client.get(
        "/resource_server/metis/models", headers=auth_header(ADMIN_TOKEN)
    )
    assert other.status_code == 200
    assert other.json() == []


# --- helper units ------------------------------------------------------------


def test_framework_standin() -> None:
    assert v1_compat._framework("sophia") == "vllm"
    assert v1_compat._framework("metis") == "api"


def _config_with(dep_name: str, backends: list[BackendConfig]) -> RouterConfig:
    return RouterConfig(
        version=1,
        models=[
            ModelConfig(
                name="m",
                aliases=["m-alias"],
                allowed_groups=[],
                allowed_domains=[],
                supported_endpoints=["chat/completions"],
                usage_limits={},  # type: ignore[arg-type]
                overload={},  # type: ignore[arg-type]
                deployments=[
                    DeploymentConfig(
                        kind="static",
                        name=dep_name,
                        router_params={},  # type: ignore[arg-type]
                        prometheus_metrics_path=None,
                        prometheus_scrape_interval_sec=30,
                        backends=backends,
                    )
                ],
            )
        ],
    )


def _backend() -> BackendConfig:
    return BackendConfig(
        id="b1",
        model_url="http://localhost/v1",
        backend_model_name="upstream",
        api_key=None,
    )


def test_deployment_name_picks_cluster_prefixed() -> None:
    config = _config_with("sophia/static/m", [_backend()])
    assert v1_compat._deployment_name("sophia", config, "m") == "sophia/static/m"
    # Alias resolves too.
    assert v1_compat._deployment_name("sophia", config, "m-alias") == "sophia/static/m"


def test_deployment_name_wrong_cluster_unavailable() -> None:
    config = _config_with("sophia/static/m", [_backend()])
    with pytest.raises(ServiceUnavailable):
        v1_compat._deployment_name("metis", config, "m")


def test_deployment_name_no_backends_unavailable() -> None:
    config = _config_with("sophia/static/m", [])
    with pytest.raises(ServiceUnavailable):
        v1_compat._deployment_name("sophia", config, "m")


def test_deployment_name_unknown_model_unavailable() -> None:
    config = _config_with("sophia/static/m", [_backend()])
    with pytest.raises(ServiceUnavailable):
        v1_compat._deployment_name("sophia", config, "does-not-exist")


# --- staging area (mocked Globus TransferClient) -----------------------------


class _FakeTransferError(Exception):
    pass


class _FakeTransferClient:
    """Minimal stand-in for globus_sdk.TransferClient's staging surface."""

    error_class = _FakeTransferError

    def __init__(self, *, existing_dir: bool, existing_rule: bool) -> None:
        self.existing_dir = existing_dir
        self.existing_rule = existing_rule
        self.mkdir_calls: list[tuple[str, str]] = []
        self.added_rules: list[dict[str, Any]] = []

    def operation_mkdir(self, collection_id: str, path: str) -> None:
        self.mkdir_calls.append((collection_id, path))
        if self.existing_dir:
            raise _FakeTransferError("Path already exists")

    def endpoint_acl_list(self, collection_id: str) -> list[dict[str, str]]:
        if self.existing_rule:
            return [
                {
                    "id": "rule-existing",
                    "principal": "user-123",
                    "path": "/user-staging/user-123/",
                }
            ]
        return []

    def add_endpoint_acl_rule(
        self, collection_id: str, rule: dict[str, Any]
    ) -> dict[str, str]:
        self.added_rules.append(rule)
        return {"access_id": "rule-new"}


def test_prep_staging_creates_dir_and_rule() -> None:
    tc = _FakeTransferClient(existing_dir=False, existing_rule=False)
    result = v1_compat._prep_globus_staging_area(tc, "user-123", "coll-1")  # type: ignore[arg-type]

    assert result.path == "/user-staging/user-123/"
    assert result.acl_rule_id == "rule-new"
    assert result.collection_id == "coll-1"
    assert result.principal == "user-123"
    assert tc.mkdir_calls == [("coll-1", "/user-staging/user-123/")]
    assert tc.added_rules[0]["permissions"] == "rw"


def test_prep_staging_idempotent() -> None:
    tc = _FakeTransferClient(existing_dir=True, existing_rule=True)
    result = v1_compat._prep_globus_staging_area(tc, "user-123", "coll-1")  # type: ignore[arg-type]

    # Existing dir + rule: no new rule created, existing rule id returned.
    assert result.acl_rule_id == "rule-existing"
    assert tc.added_rules == []


def test_prep_staging_reraises_unexpected_mkdir_error() -> None:
    tc = _FakeTransferClient(existing_dir=False, existing_rule=False)

    def boom(collection_id: str, path: str) -> None:
        raise _FakeTransferError("permission denied")

    tc.operation_mkdir = boom  # type: ignore[method-assign]
    with pytest.raises(_FakeTransferError, match="permission denied"):
        v1_compat._prep_globus_staging_area(tc, "user-123", "coll-1")  # type: ignore[arg-type]
