"""
Retired models and deployments: tombstones in the Model spec that turn a
would-be 404 into a 410 whose message tells the caller what to do instead.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import httpx
import openai
import pytest
from openai import AsyncOpenAI
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from first_common.schema.resources import ResourceManifest
from first_common.schema.resources.spec import (
    ModelSpec,
    PilotDeploymentSpec,
    RetiredDeployment,
)
from first_common.schema.types import ResourceName
from first_gateway.apiserver.api import app
from first_gateway.controllers.workers.router_config_observer import (
    RouterConfigObserver,
)
from first_gateway.database.redis.router_config import (
    BackendConfig,
    DeploymentConfig,
    ModelConfig,
    RouterConfig,
)

from .fixtures.auth import ADMIN_TOKEN, USER_TOKEN, auth_header
from .test_resource_apply import _apply, _load, _plan

MODEL = "meta-llama/llama-3-8b"
STATIC = "sophia/static/llama-3-8b"
PILOT = "sophia/pilot/llama-3-8b"
_CHAT = {"messages": [{"role": "user", "content": "hi"}], "max_tokens": 5}


# --- Spec apply ----------------------------------------------------------------


def _tomb(name: str, cluster_name: str) -> RetiredDeployment:
    return RetiredDeployment.model_validate(
        {"name": name, "cluster_name": cluster_name}
    )


def _model_spec(resources: list[ResourceManifest]) -> ModelSpec:
    [model] = [r for r in resources if r.kind == "Model"]
    assert isinstance(model.spec, ModelSpec)
    return model.spec


def _without(resources: list[ResourceManifest], *names: str) -> list[ResourceManifest]:
    return [r for r in resources if r.name not in names]


async def _plan_error(
    client: httpx.AsyncClient, resources: list[ResourceManifest]
) -> str:
    resp = await client.post(
        "/control/v1/plan",
        json={"resources": [r.model_dump(mode="json") for r in resources]},
        headers=auth_header(ADMIN_TOKEN),
    )
    assert resp.status_code == 400, resp.text
    assert resp.json()["error"]["code"] == "resource_spec_invalid"
    return str(resp.json()["error"]["message"])


async def test_retire_deployment_then_model_round_trips(
    control_client: httpx.AsyncClient, db: async_sessionmaker[AsyncSession]
) -> None:
    """Tombstones survive apply -> DB -> re-plan and reach the RouterConfig."""
    resources = _load("baseline")
    await _apply(control_client, resources, await _plan(control_client, resources))

    # Retire the static deployment; the model stays served by the pilot one.
    resources = _without(resources, STATIC)
    _model_spec(resources).retired_deployments = [_tomb(STATIC, "sophia")]
    plan = await _plan(control_client, resources)
    assert [(p.kind, set(p.patch)) for p in plan.to_update] == [
        ("Model", {"retired_deployments"})
    ]
    assert [(r.kind, r.name) for r in plan.to_delete] == [("StaticDeployment", STATIC)]
    await _apply(control_client, resources, plan)
    replan = await _plan(control_client, resources)
    assert (replan.to_add, replan.to_update, replan.to_delete) == ([], [], [])

    # Retire the whole model.
    resources = _without(resources, PILOT)
    _model_spec(resources).retired = True
    await _apply(control_client, resources, await _plan(control_client, resources))
    replan = await _plan(control_client, resources)
    assert (replan.to_add, replan.to_update, replan.to_delete) == ([], [], [])

    client_state = MagicMock()
    client_state.db_sessionmaker = db
    observer = RouterConfigObserver("router-cfg", client_state, MagicMock())
    [config] = await observer.rebuild()
    assert config.retired is True
    assert config.deployments == []
    assert config.retired_deployments == [_tomb(STATIC, "sophia")]


async def test_retired_model_with_deployments_is_invalid(
    control_client: httpx.AsyncClient,
) -> None:
    resources = _load("baseline")
    _model_spec(resources).retired = True
    message = await _plan_error(control_client, resources)
    assert "is retired but still has deployments" in message
    assert PILOT in message and STATIC in message


async def test_live_deployment_cannot_be_retired(
    control_client: httpx.AsyncClient,
) -> None:
    resources = _load("baseline")
    _model_spec(resources).retired_deployments = [_tomb(STATIC, "sophia")]
    assert STATIC in await _plan_error(control_client, resources)


def test_retired_deployment_names_unique() -> None:
    with pytest.raises(ValueError, match="unique"):
        ModelSpec.model_validate(
            {
                "access_group_name": "g",
                "supported_endpoints": ["chat/completions"],
                "retired_deployments": [
                    {"name": "a/b", "cluster_name": "a"},
                    {"name": "a/b", "cluster_name": "c"},
                ],
            }
        )


# --- Routing -------------------------------------------------------------------


def _deployment(name: str, cluster: str) -> DeploymentConfig:
    return DeploymentConfig(
        kind="static",
        name=name,
        cluster_name=cluster,
        router_params={},  # type: ignore[arg-type]
        prometheus_metrics_path=None,
        prometheus_scrape_interval_sec=30,
        backends=[
            BackendConfig(
                id=f"static_deployment/{name}",
                model_url="http://localhost:9/v1",
                backend_model_name="upstream",
                api_key=None,
            )
        ],
    )


def _model(name: str, **kwargs: object) -> ModelConfig:
    return ModelConfig(
        name=name,
        aliases=[f"{name}-alias"],
        allowed_groups=[],
        allowed_domains=[],
        supported_endpoints=["chat/completions", "messages"],
        usage_limits={},  # type: ignore[arg-type]
        overload={},  # type: ignore[arg-type]
        **kwargs,  # type: ignore[arg-type]
    )


def _cross_model_config(*, private_target: bool = False) -> RouterConfig:
    source = _model(
        "beam-1.0-bf16",
        deployments=[],
        retired_deployments=[
            RetiredDeployment.model_validate(
                {
                    "name": "minerva/beam-1.0-bf16",
                    "cluster_name": "minerva",
                    "replacement": {
                        "cluster_name": "tara",
                        "model_name": "beam-1.0-fp8",
                        "base_url": "https://inference-api.alcf.anl.gov/resource_server/tara/api/v1",
                    },
                }
            )
        ],
    )
    source.supported_endpoints = ["chat/completions", "responses", "messages"]
    target = _model(
        "beam-1.0-fp8", deployments=[_deployment("tara/beam-1.0-fp8", "tara")]
    )
    if private_target:
        target.allowed_groups = ["private-target-group"]
    return RouterConfig(models=[source, target])


@pytest.mark.parametrize("api", ["chat/completions", "responses", "messages"])
@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize(
    "prefix",
    [
        "/resource_server/minerva/api/v1",
        "/deployments/minerva~beam-1.0-bf16/v1",
    ],
)
async def test_cross_model_retirement_before_dispatch(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    api: str,
    stream: bool,
    prefix: str,
) -> None:
    monkeypatch.setattr(
        app.state.router_config_manager, "_current", _cross_model_config()
    )
    body = {"model": "beam-1.0-bf16", "stream": stream}
    if api == "responses":
        body.update(input="hi", max_output_tokens=5)
    else:
        body.update(_CHAT)
    response = await client.post(
        f"{prefix}/{api}", json=body, headers=auth_header(USER_TOKEN)
    )
    error = _error(response, 410, "deployment_retired")
    info = error["info"]
    assert isinstance(info, dict)
    assert info["replacement_model"] == "beam-1.0-fp8"
    assert info["replacement_cluster"] == "tara"
    assert info["replacement_base_url"] == (
        "https://inference-api.alcf.anl.gov/resource_server/tara/api/v1"
    )
    assert "Change both" in str(error["message"])
    assert "beam-1.0-fp8" in str(error["message"])
    assert response.headers["content-type"].startswith("application/json")
    assert "location" not in response.headers
    assert "localhost:9" not in response.text


@pytest.mark.parametrize("api", ["chat/completions", "responses", "messages"])
async def test_cross_model_guidance_filtered_by_target_acl(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    api: str,
) -> None:
    monkeypatch.setattr(
        app.state.router_config_manager,
        "_current",
        _cross_model_config(private_target=True),
    )
    body = {"model": "beam-1.0-bf16", "stream": True}
    body.update(
        {"input": "hi", "max_output_tokens": 5} if api == "responses" else _CHAT
    )
    response = await client.post(
        f"/resource_server/minerva/api/v1/{api}",
        json=body,
        headers=auth_header(USER_TOKEN),
    )
    error = _error(response, 410, "deployment_retired")
    info = error["info"]
    assert isinstance(info, dict)
    assert info["alternatives"] == []
    assert "replacement_model" not in info
    assert "beam-1.0-fp8" not in response.text
    assert "tara" not in str(error["message"])
    response = await client.post(f"/resource_server/minerva/api/v1/{api}", json=body)
    assert response.status_code in (401, 403)
    assert "deployment_retired" not in response.text


@pytest.mark.parametrize(
    "url",
    [
        "http://example.org/v1",
        "https://user:secret@example.org/v1",
        "https://example.org/v1?token=secret",
        "https://example.org/v1#fragment",
        "/relative/v1",
    ],
)
def test_replacement_url_rejects_credentials_or_ambiguous_guidance(url: str) -> None:
    with pytest.raises(ValueError, match="HTTPS URL"):
        RetiredDeployment.model_validate(
            {
                "name": "old/model",
                "cluster_name": "old",
                "replacement": {
                    "cluster_name": "new",
                    "model_name": "new-model",
                    "base_url": url,
                },
            }
        )


async def test_cross_model_catalog_round_trip_and_missing_target_rejected(
    control_client: httpx.AsyncClient,
) -> None:
    resources = _load("baseline")
    target = next(r for r in resources if r.kind == "Model").model_copy(deep=True)
    target.name = ResourceName("replacement-model")
    deployment = next(r for r in resources if r.name == PILOT).model_copy(deep=True)
    deployment.name = ResourceName("sophia/pilot/replacement-model")
    assert isinstance(deployment.spec, PilotDeploymentSpec)
    deployment.spec.model_name = target.name
    resources = _without(resources, STATIC, PILOT)
    _model_spec(resources).retired_deployments = [
        RetiredDeployment.model_validate(
            {
                "name": STATIC,
                "cluster_name": "sophia",
                "replacement": {
                    "cluster_name": "sophia",
                    "model_name": target.name,
                    "base_url": "https://example.org/resource_server/sophia/api/v1",
                },
            }
        )
    ]
    assert "retirement replacement" in await _plan_error(control_client, resources)
    resources.extend([target, deployment])
    await _apply(control_client, resources, await _plan(control_client, resources))
    replan = await _plan(control_client, resources)
    assert (replan.to_add, replan.to_update, replan.to_delete) == ([], [], [])


@pytest.fixture
def retirement_config() -> None:
    """
    - `gone`: a retired model (once deployed on sophia).
    - `moved`: retired from sophia, still live on metis.
    - `stranded`: retired from sophia, with nothing else online.
    """
    app.state.router_config_manager._current = RouterConfig(
        version=999,
        models=[
            _model(
                "gone",
                deployments=[],
                retired=True,
                retired_deployments=[_tomb("sophia/gone", "sophia")],
            ),
            _model(
                "moved",
                deployments=[_deployment("metis/static/moved", "metis")],
                retired_deployments=[_tomb("sophia/static/moved", "sophia")],
            ),
            _model(
                "stranded",
                deployments=[],
                retired_deployments=[_tomb("sophia/stranded", "sophia")],
            ),
        ],
    )


async def _chat(client: httpx.AsyncClient, path: str, model: str) -> httpx.Response:
    return await client.post(
        path, json={"model": model, **_CHAT}, headers=auth_header(USER_TOKEN)
    )


def _error(resp: httpx.Response, status: int, code: str) -> dict[str, object]:
    assert resp.status_code == status, resp.text
    body = resp.json()
    # Parses as both an OpenAI and an Anthropic error.
    assert body["type"] == "error"
    assert body["error"]["type"] == body["error"]["code"] == code
    assert isinstance(body["error"]["message"], str)
    return dict(body["error"])


@pytest.mark.parametrize(
    "path",
    [
        "/federated/v1/chat/completions",
        "/federated/v1/messages",
        "/deployments/sophia~gone/v1/chat/completions",
        "/deployments/never-existed/v1/chat/completions",
        "/resource_server/sophia/vllm/v1/chat/completions",
    ],
)
@pytest.mark.parametrize("model", ["gone", "gone-alias"])
async def test_retired_model_is_gone_on_every_path(
    client: httpx.AsyncClient, retirement_config: None, path: str, model: str
) -> None:
    error = _error(await _chat(client, path, model), 410, "model_retired")
    assert error["message"] == (
        "Model 'gone' has been retired and is no longer available. Please switch "
        "to a different model; GET .../federated/v1/models lists the models "
        "available to you."
    )


async def test_pinned_retired_deployment_points_to_alternatives(
    client: httpx.AsyncClient, retirement_config: None
) -> None:
    resp = await _chat(
        client, "/deployments/sophia~static~moved/v1/chat/completions", "moved"
    )
    error = _error(resp, 410, "deployment_retired")
    assert error["message"] == (
        "Deployment 'sophia/static/moved' has been retired, but model 'moved' is "
        "still available. Change your client's base URL from "
        ".../deployments/sophia~static~moved/v1 to .../federated/v1 to use any "
        "available deployment automatically, or to one of: "
        ".../deployments/metis~static~moved/v1."
    )
    assert error["info"] == {
        "model": "moved",
        "retired_deployment": "sophia/static/moved",
        "retired_cluster": "sophia",
        "federated_base_path": "/federated/v1",
        "alternatives": [
            {
                "deployment": "metis/static/moved",
                "cluster": "metis",
                "base_path": "/deployments/metis~static~moved/v1",
            }
        ],
    }


async def test_pinned_retired_deployment_without_alternatives(
    client: httpx.AsyncClient, retirement_config: None
) -> None:
    resp = await _chat(
        client, "/deployments/sophia~stranded/v1/chat/completions", "stranded"
    )
    error = _error(resp, 410, "deployment_retired")
    assert "has no other deployment online right now" in str(error["message"])
    assert ".../federated/v1" in str(error["message"])


async def test_unknown_deployment_is_still_404(
    client: httpx.AsyncClient, retirement_config: None
) -> None:
    resp = await _chat(
        client, "/deployments/never-existed/v1/chat/completions", "moved"
    )
    _error(resp, 404, "not_found")


async def test_unknown_model_is_still_404(
    client: httpx.AsyncClient, retirement_config: None
) -> None:
    resp = await _chat(client, "/federated/v1/chat/completions", "never-existed")
    _error(resp, 404, "not_found")


async def test_v1_cluster_retired_points_to_other_clusters(
    client: httpx.AsyncClient, retirement_config: None
) -> None:
    resp = await _chat(
        client, "/resource_server/sophia/api/v1/chat/completions", "moved"
    )
    error = _error(resp, 410, "deployment_retired")
    assert error["message"] == (
        "Model 'moved' on cluster 'sophia' has been retired, but model 'moved' is "
        "still available. Change your client's base URL from "
        ".../resource_server/sophia/api/v1 to .../federated/v1 to use any "
        "available deployment automatically, or to one of: "
        ".../resource_server/metis/api/v1."
    )


async def test_v1_never_on_cluster_is_still_404(
    client: httpx.AsyncClient, retirement_config: None
) -> None:
    resp = await _chat(
        client, "/resource_server/polaris/api/v1/chat/completions", "moved"
    )
    _error(resp, 404, "not_found")


async def test_openai_sdk_surfaces_retirement_message(
    client: httpx.AsyncClient, retirement_config: None
) -> None:
    oai = AsyncOpenAI(
        api_key=USER_TOKEN,
        base_url=f"{client.base_url}/deployments/sophia~static~moved/v1",
        http_client=client,
        max_retries=0,
    )
    with pytest.raises(openai.APIStatusError) as exc_info:
        await oai.chat.completions.create(model="moved", **_CHAT)  # type: ignore[call-overload]
    exc = exc_info.value
    assert exc.status_code == 410
    assert exc.code == "deployment_retired"
    assert ".../federated/v1" in str(exc)


async def test_retired_model_is_not_listed(
    client: httpx.AsyncClient, retirement_config: None
) -> None:
    resp = await client.get("/federated/v1/models", headers=auth_header(USER_TOKEN))
    assert resp.status_code == 200
    assert [m["id"] for m in resp.json()["data"]] == ["moved", "stranded"]
