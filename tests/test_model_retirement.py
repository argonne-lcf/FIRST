"""Retirement survives deployment removal without becoming a routing target."""

from dataclasses import replace
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi.responses import JSONResponse

from first_common.schema.endpoints.base import BasePayload
from first_gateway.apiserver.api import app
from first_gateway.apiserver.inference import InferenceService
from first_gateway.apiserver.retirements import MODEL_RETIREMENTS
from first_gateway.database.redis.router_config import (
    BackendConfig,
    DeploymentConfig,
    ModelConfig,
    RouterConfig,
)

from .fixtures.auth import INVALID_TOKEN, USER_TOKEN, auth_header
from .test_inference_routes import _seed_config

KEY = ("minerva", "inkling-bf16")
PROTOCOLS = ["chat/completions", "responses", "messages"]
EXPECTED = {
    "error": {
        "code": "model_retired",
        "message": "Inkling has been retired on Minerva. Use inkling-bf16 on Tara.",
        "info": {
            "replacement_cluster": "tara",
            "replacement_model": "inkling-bf16",
            "replacement_api_url": (
                "https://inference-api.alcf.anl.gov/resource_server/tara/api/v1"
            ),
        },
    }
}


def _body(protocol: str, stream: bool, model: str = "inkling-bf16") -> dict[str, Any]:
    body = {"model": model, "stream": stream}
    if protocol == "responses":
        body.update(input="hi", max_output_tokens=32)
    else:
        body.update(messages=[{"role": "user", "content": "hi"}], max_tokens=32)
    return body


def _config(
    *, include_model: bool = True, include_minerva: bool = False
) -> RouterConfig:
    if not include_model:
        return RouterConfig()
    model = _seed_config().models[0]
    model.name = "inkling-bf16"
    model.aliases = ["inkling-alias"]
    model.supported_endpoints = PROTOCOLS
    tara = model.deployments[0]
    tara.name, tara.cluster_name = "tara/inkling-bf16", "tara"
    if include_minerva:
        minerva = tara.model_copy(deep=True)
        minerva.name, minerva.cluster_name = "minerva/inkling-bf16", "minerva"
        model.deployments.append(minerva)
    return RouterConfig(models=[model])


@pytest.mark.parametrize("protocol", PROTOCOLS)
@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize(
    "prefix",
    ["/resource_server/minerva/api/v1", "/deployments/minerva~inkling-bf16/v1"],
)
@pytest.mark.parametrize("state", ["before", "after", "no-model"])
async def test_retired_before_dispatch_even_after_original_identity_removed(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    protocol: str,
    stream: bool,
    prefix: str,
    state: str,
) -> None:
    app.state.router_config_manager._current = _config(
        include_model=state != "no-model", include_minerva=state == "before"
    )
    dispatch = AsyncMock(side_effect=AssertionError("must not dispatch"))
    monkeypatch.setattr(InferenceService, "_get_upstream_handler", dispatch)
    admission = AsyncMock(side_effect=AssertionError("must not admit"))
    monkeypatch.setattr(app.state.admission_controller, "get_token_stats", admission)
    response = await client.post(
        f"{prefix}/{protocol}",
        json=_body(protocol, stream),
        headers=auth_header(USER_TOKEN),
    )
    assert response.status_code == 410, response.text
    assert response.json() == EXPECTED
    assert response.headers["content-type"] == "application/json"
    assert "location" not in response.headers
    dispatch.assert_not_called()
    admission.assert_not_awaited()


@pytest.mark.parametrize("protocol", PROTOCOLS)
@pytest.mark.parametrize("token", [None, INVALID_TOKEN])
@pytest.mark.parametrize(
    "prefix",
    ["/resource_server/minerva/api/v1", "/deployments/minerva~inkling-bf16/v1"],
)
async def test_retirement_requires_authentication(
    client: httpx.AsyncClient,
    protocol: str,
    token: str | None,
    prefix: str,
) -> None:
    app.state.router_config_manager._current = RouterConfig()
    response = await client.post(
        f"{prefix}/{protocol}",
        json=_body(protocol, True),
        headers=auth_header(token) if token else {},
    )
    assert response.status_code == 401
    assert "replacement_cluster" not in response.text


@pytest.mark.parametrize(
    ("policy", "include_model"),
    [
        ("retained-group", False),
        ("retained-group", True),
        ("retained-domain", False),
        ("retained-domain", True),
        ("live-model", True),
    ],
)
async def test_retirement_preserves_authorization(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    policy: str,
    include_model: bool,
) -> None:
    config = _config(include_model=include_model)
    if policy == "retained-group":
        monkeypatch.setitem(
            MODEL_RETIREMENTS,
            KEY,
            replace(MODEL_RETIREMENTS[KEY], allowed_groups=["denied"]),
        )
    elif policy == "retained-domain":
        monkeypatch.setitem(
            MODEL_RETIREMENTS,
            KEY,
            replace(MODEL_RETIREMENTS[KEY], allowed_domains=["denied.test"]),
        )
    elif include_model:
        config.models[0].allowed_groups = ["denied"]
    app.state.router_config_manager._current = config
    response = await client.post(
        "/resource_server/minerva/api/v1/chat/completions",
        json=_body("chat/completions", False),
        headers=auth_header(USER_TOKEN),
    )
    assert response.status_code == 403
    assert "replacement_cluster" not in response.text


@pytest.mark.parametrize(
    "prefix",
    [
        "/resource_server/tara/api/v1",
        "/deployments/tara~inkling-bf16/v1",
        "/federated/v1",
    ],
)
@pytest.mark.parametrize("protocol", PROTOCOLS)
async def test_tara_and_federated_inkling_continue_through_normal_dispatch(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    prefix: str,
    protocol: str,
) -> None:
    app.state.router_config_manager._current = _config()
    seen: list[str] = []

    async def handler(
        _self: InferenceService,
        _client: httpx.AsyncClient,
        payload: BasePayload,
        _model: ModelConfig,
        _deployment: DeploymentConfig,
        _backend: BackendConfig,
    ) -> JSONResponse:
        seen.append(payload.model)
        return JSONResponse({"tara": True})

    monkeypatch.setattr(InferenceService, "_handle_unary", handler)
    # Exercise real resolution, authorization, admission and backend selection;
    # replace only the final network call, never retirement/dispatch itself.
    backend = (
        app.state.router_config_manager.current.models[0].deployments[0].backends[0]
    )
    app.state.backend_client_manager.clients[backend.id] = httpx.AsyncClient()
    try:
        response = await client.post(
            f"{prefix}/{protocol}",
            json=_body(protocol, False),
            headers=auth_header(USER_TOKEN),
        )
        assert response.status_code == 200, response.text
        assert response.json() == {"tara": True}
        assert seen
    finally:
        await app.state.backend_client_manager.clients.pop(backend.id).aclose()


async def test_unrelated_model_and_unknown_target_are_not_retired(
    client: httpx.AsyncClient,
) -> None:
    app.state.router_config_manager._current = _config()
    for prefix, model in (
        ("/resource_server/minerva/api/v1", "unknown"),
        ("/resource_server/unknown/api/v1", "inkling-bf16"),
    ):
        response = await client.post(
            f"{prefix}/chat/completions",
            json=_body("chat/completions", False, model),
            headers=auth_header(USER_TOKEN),
        )
        assert response.status_code == 404
        assert response.json()["error"]["code"] == "not_found"


async def test_additional_retirement_is_mapping_data(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app.state.router_config_manager._current = RouterConfig()
    monkeypatch.setitem(
        MODEL_RETIREMENTS,
        ("minerva", "another-retired-model"),
        replace(MODEL_RETIREMENTS[KEY], replacement_model="new-model"),
    )
    response = await client.post(
        "/resource_server/minerva/api/v1/chat/completions",
        json=_body("chat/completions", False, "another-retired-model"),
        headers=auth_header(USER_TOKEN),
    )
    assert response.status_code == 410
    assert response.json()["error"]["info"]["replacement_model"] == "new-model"


async def test_alias_resolves_retirement_without_advertising_fake_deployment(
    client: httpx.AsyncClient,
) -> None:
    app.state.router_config_manager._current = _config()
    response = await client.post(
        "/resource_server/minerva/api/v1/chat/completions",
        json=_body("chat/completions", False, "inkling-alias"),
        headers=auth_header(USER_TOKEN),
    )
    assert response.status_code == 410
    listing = await client.get(
        "/deployments/minerva~inkling-bf16/v1/models", headers=auth_header(USER_TOKEN)
    )
    assert listing.status_code == 200
    assert listing.json()["data"] == []
