"""
Tests for the SDK-compatible GET /v1/models endpoints under the federated and
deployment-scoped inference routes.
"""

from __future__ import annotations

import httpx
import pytest

from first_gateway.apiserver.api import app
from first_gateway.database.redis.router_config import (
    BackendConfig,
    DeploymentConfig,
    ModelConfig,
    RouterConfig,
)

from .fixtures.auth import ADMIN_TOKEN, USER_TOKEN, auth_header


def _backend(dep_name: str) -> DeploymentConfig:
    return DeploymentConfig(
        kind="static",
        name=dep_name,
        router_params={},  # type: ignore[arg-type]
        prometheus_metrics_path=None,
        prometheus_scrape_interval_sec=30,
        backends=[
            BackendConfig(
                id=f"static_deployment/{dep_name}",
                model_url="http://localhost:8080/v1",
                backend_model_name="upstream",
                api_key=None,
            )
        ],
    )


def _seed_config() -> RouterConfig:
    """Two models: one open to everyone, one restricted to a group nobody has."""
    return RouterConfig(
        version=999,
        models=[
            ModelConfig(
                name="open/model",
                aliases=[],
                allowed_groups=[],
                allowed_domains=[],
                supported_endpoints=["chat/completions", "messages"],
                max_model_len=4096,
                display_name="Open Model",
                capabilities={"thinking": {"supported": True}},
                usage_limits={},  # type: ignore[arg-type]
                overload={},  # type: ignore[arg-type]
                deployments=[_backend("dep-open")],
            ),
            ModelConfig(
                name="secret/model",
                aliases=[],
                allowed_groups=["group-nobody-has"],
                allowed_domains=[],
                supported_endpoints=["chat/completions"],
                max_model_len=None,
                display_name=None,
                capabilities={},
                usage_limits={},  # type: ignore[arg-type]
                overload={},  # type: ignore[arg-type]
                deployments=[_backend("dep-secret")],
            ),
        ],
    )


@pytest.fixture
def seeded_config() -> None:
    """Pin the running app's RouterConfig snapshot to a known value."""
    app.state.router_config_manager._current = _seed_config()


async def test_federated_models_filters_by_access(
    client: httpx.AsyncClient, seeded_config: None
) -> None:
    resp = await client.get("/federated/v1/models", headers=auth_header(USER_TOKEN))
    assert resp.status_code == 200
    data = resp.json()["data"]

    # The restricted model is hidden from an ordinary user.
    assert [m["id"] for m in data] == ["open/model"]
    model = data[0]
    assert model["type"] == "model"
    assert model["display_name"] == "Open Model"
    assert model["max_tokens"] == 4096
    assert model["capabilities"] == {"thinking": {"supported": True}}


async def test_federated_models_display_name_defaults_to_id(
    client: httpx.AsyncClient, seeded_config: None
) -> None:
    resp = await client.get("/federated/v1/models", headers=auth_header(ADMIN_TOKEN))
    assert resp.status_code == 200
    by_id = {m["id"]: m for m in resp.json()["data"]}

    # admin@anl.gov is in no restricted group either, so still only the open model.
    assert set(by_id) == {"open/model"}
    assert by_id["open/model"]["display_name"] == "Open Model"


async def test_deployment_models_scoped_to_slug(
    client: httpx.AsyncClient, seeded_config: None
) -> None:
    resp = await client.get(
        "/deployments/dep-open/v1/models", headers=auth_header(USER_TOKEN)
    )
    assert resp.status_code == 200
    assert [m["id"] for m in resp.json()["data"]] == ["open/model"]


async def test_deployment_models_unknown_slug_is_empty(
    client: httpx.AsyncClient, seeded_config: None
) -> None:
    resp = await client.get(
        "/deployments/does-not-exist/v1/models", headers=auth_header(USER_TOKEN)
    )
    assert resp.status_code == 200
    assert resp.json()["data"] == []


async def test_models_requires_auth(
    client: httpx.AsyncClient, seeded_config: None
) -> None:
    resp = await client.get("/federated/v1/models")
    assert resp.status_code == 401
