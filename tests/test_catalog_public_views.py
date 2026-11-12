"""
The gateway's catalog serves every caller, admins included, narrow public views
without operational detail.  The full resources come only from the admin API on
the loopback-only control server.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from first_common.schema.types import ReplicaState
from first_gateway.database.models import AccessGroup, PilotReplica

from .fixtures.auth import ADMIN_TOKEN, USER_TOKEN, auth_header
from .test_resource_apply import _apply, _load, _plan

PILOT = "sophia/pilot/llama-3-8b"

# Keys that carry operational detail and must never reach an ordinary user.
ADMIN_ONLY_KEYS = {
    # Cluster pilot configuration
    "pilot_system",
    "scheduler_config",
    "endpoint_uuid",
    "submit_script_preamble",
    "ip_allowlist",
    "pilot_env_path",
    "pilot_config_path",
    "pilot_path",
    "workdir",
    "nginx_path",
    "queue",
    "account",
    "health_check",
    "health_detail",
    "pilot_start_failures",
    # Deployments and replicas
    "replicas",
    "state_message",
    "launch_spec",
    "launch_template_name",
    "parameters",
    "env",
    "api_url",
    "api_key",
    "mtls",
    "model_url",
    "log_path",
    "pilot_job_name",
    "upstream_model_name",
    "router_params",
    "scaling_strategy",
    "consecutive_launch_failures",
    # Models
    "usage_limits",
    "overload",
    "demand_signal",
    # Reconcile internals
    "uid",
    "reconcile_last_error",
    "reconcile_last_traceback",
}

# Values from tests/resource_specs/baseline that must not leak either.
ADMIN_ONLY_VALUES = [
    "/eagle/",
    "sophia-endpoint-uuid",
    "infer-svc",
    "inference_svc",
    "10.0.0.0/24",
    "module load",
    "SOPHIA_API_KEY",
    "sophia.alcf.anl.gov",
    "secret-replica-log",
]

USER_ROUTES = [
    "/catalog/v1/models",
    "/catalog/v1/clusters",
    "/catalog/v1/deployments/pilot",
    f"/catalog/v1/deployments/pilot/{PILOT}",
    "/catalog/v1/deployments/static",
]


def all_keys(obj: Any) -> set[str]:
    if isinstance(obj, dict):
        return set(obj) | {k for v in obj.values() for k in all_keys(v)}
    if isinstance(obj, list):
        return {k for v in obj for k in all_keys(v)}
    return set()


@pytest.fixture
async def baseline(
    control_client: httpx.AsyncClient, db: async_sessionmaker[AsyncSession]
) -> None:
    resources = _load("baseline")
    plan = await _plan(control_client, resources)
    await _apply(control_client, resources, plan)

    async with db.begin() as sess:
        # Grant the regular test user (user@anl.gov, no groups) the baseline Model.
        await sess.execute(
            sa.update(AccessGroup).values(
                allowed_groups=[], allowed_domains=["anl.gov"]
            )
        )
        for i, state in enumerate(
            [ReplicaState.ready, ReplicaState.ready, ReplicaState.unhealthy]
        ):
            sess.add(
                PilotReplica(
                    name=f"{PILOT}/replica/{i}",
                    pilot_deployment_name=PILOT,
                    state=state.value,
                    state_message="Serving at http://10.0.0.5:8000",
                    log_path=f"/eagle/inference-service/secret-replica-log-{i}",
                )
            )
        # Soft-deleted replicas are not counted.
        sess.add(
            PilotReplica(
                name=f"{PILOT}/replica/gone",
                pilot_deployment_name=PILOT,
                state=ReplicaState.terminated.value,
                deleted_at=datetime.now(timezone.utc),
            )
        )


async def get(client: httpx.AsyncClient, path: str, token: str) -> Any:
    resp = await client.get(path, headers=auth_header(token))
    assert resp.status_code == 200, resp.text
    return resp.json()


@pytest.mark.parametrize("token", [USER_TOKEN, ADMIN_TOKEN])
@pytest.mark.parametrize("path", USER_ROUTES)
async def test_gateway_serves_no_operational_detail(
    client: httpx.AsyncClient, baseline: None, path: str, token: str
) -> None:
    body = await get(client, path, token)
    assert body, "baseline resources should be visible to the caller"
    assert not all_keys(body) & ADMIN_ONLY_KEYS
    text = json.dumps(body)
    assert not [v for v in ADMIN_ONLY_VALUES if v in text]


async def test_cluster_view(client: httpx.AsyncClient, baseline: None) -> None:
    assert await get(client, "/catalog/v1/clusters", USER_TOKEN) == [
        {
            "kind": "Cluster",
            "name": "sophia",
            "health": "unknown",
            "maintenance_notice": "Sophia is operational",
        }
    ]


async def test_model_view(client: httpx.AsyncClient, baseline: None) -> None:
    [model] = await get(client, "/catalog/v1/models", USER_TOKEN)
    assert model["name"] == "meta-llama/llama-3-8b"
    assert model["supported_endpoints"] == ["chat/completions", "completions"]
    assert model["limits"]["tpm"] > 0
    assert [d["name"] for d in model["pilot_deployments"]] == [PILOT]
    assert [d["health"] for d in model["static_deployments"]] == ["unknown"]
    assert model["maintenance_notices"] == {"sophia": "Sophia is operational"}


async def test_pilot_detail_counts_replicas(
    client: httpx.AsyncClient, baseline: None
) -> None:
    detail = await get(client, f"/catalog/v1/deployments/pilot/{PILOT}", USER_TOKEN)
    assert detail["replica_counts"] == {"ready": 2, "unhealthy": 1}
    assert detail["model_name"] == "meta-llama/llama-3-8b"


async def test_admin_api_keeps_full_views(
    control_client: httpx.AsyncClient, baseline: None
) -> None:
    http = control_client
    [cluster] = await get(http, "/catalog/v1/clusters", ADMIN_TOKEN)
    assert cluster["pilot_system"]["queue"] == "infer-svc"
    assert "uid" in cluster and "health_check" in cluster

    [model] = await get(http, "/catalog/v1/models", ADMIN_TOKEN)
    assert "usage_limits" in model
    assert "upstream_model_name" in model["static_deployments"][0]

    [pilot] = await get(http, "/catalog/v1/deployments/pilot", ADMIN_TOKEN)
    assert {"uid", "router_params", "consecutive_launch_failures"} <= set(pilot)

    detail = await get(http, f"/catalog/v1/deployments/pilot/{PILOT}", ADMIN_TOKEN)
    assert detail["launch_spec"]["parameters"]["weights_path"].startswith("/eagle/")
    assert len(detail["replicas"]) == 4
    assert {r["state_message"] for r in detail["replicas"]} >= {
        "Serving at http://10.0.0.5:8000"
    }
    assert "replica_counts" not in detail

    [static] = await get(http, "/catalog/v1/deployments/static", ADMIN_TOKEN)
    assert static["api_url"] == "https://sophia.alcf.anl.gov/api/v1/"
    assert static["api_key"] is not None
    assert "runtime" in static

    assert "models" in await get(http, "/catalog/v1/router-config", ADMIN_TOKEN)


ADMIN_PATHS = (
    "/catalog/v1/clusters/{name}",
    "/catalog/v1/system-health",
    "/catalog/v1/router-config",
    "/catalog/v1/launch-templates",
    "/catalog/v1/pilot-replicas/{slug}/logs",
    "/control/v1/plan",
)


async def test_gateway_openapi_documents_only_public_views(
    client: httpx.AsyncClient,
) -> None:
    resp = await client.get("/openapi.json")
    assert resp.status_code == 200
    paths = resp.json()["paths"]
    assert not set(ADMIN_PATHS) & set(paths)
    assert "/catalog/v1/models" in paths
    schemas = resp.json()["components"]["schemas"]
    assert {"ClusterPublic", "ModelPublic", "PilotDeploymentPublicDetail"} <= set(
        schemas
    )
    assert not {"ClusterSummary", "ModelSummary", "PilotReplica"} & set(schemas)


async def test_admin_openapi_documents_full_views(
    control_client: httpx.AsyncClient,
) -> None:
    """The admin console SDK is generated from this schema."""
    resp = await control_client.get("/openapi.json")
    assert resp.status_code == 200
    assert set(ADMIN_PATHS) | {"/catalog/v1/models", "/whoami"} <= set(
        resp.json()["paths"]
    )
    schemas = resp.json()["components"]["schemas"]
    assert {"ClusterSummary", "ModelSummary", "PilotDeploymentDetail"} <= set(schemas)
    assert not {"ClusterPublic", "ModelPublic"} & set(schemas)


async def test_control_server_serves_the_admin_console(
    control_client: httpx.AsyncClient, tmp_path: Path
) -> None:
    from first_gateway.controllers import control_server

    assert not control_server.mount_admin_console(None)
    assert not control_server.mount_admin_console(tmp_path)  # no build there
    (tmp_path / "assets").mkdir()
    (tmp_path / "index.html").write_text("<html>console</html>")
    (tmp_path / "assets" / "app.js").write_text("js")
    assert control_server.mount_admin_console(tmp_path)
    try:
        for path in ("/admin-console/", "/admin-console/callback?code=x"):
            resp = await control_client.get(path)
            assert resp.status_code == 200 and "console" in resp.text, path
            assert resp.headers["cache-control"] == "no-store"
        resp = await control_client.get("/admin-console/assets/app.js")
        assert resp.text == "js" and "cache-control" not in resp.headers
        resp = await control_client.get("/admin-console", follow_redirects=True)
        assert resp.status_code == 200
        # The API still wins over the single-page-app fallback.
        assert (await control_client.get("/catalog/v1/models")).status_code == 401
    finally:
        control_server.app.router.routes[:] = [
            r
            for r in control_server.app.router.routes
            if getattr(r, "path", None) != control_server.ADMIN_CONSOLE_PATH
        ]
