import asyncio
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from first_common.schema.endpoints.llm import OpenAIChatCompletionsPayload
from first_gateway.apiserver import backend_client_manager as bcm
from first_gateway.apiserver.inference import InferenceService
from first_gateway.database.redis.router_config import (
    BackendConfig,
    DeploymentConfig,
    ModelConfig,
    RouterConfig,
)
from first_gateway.settings import Settings


def _config(model_url: str) -> RouterConfig:
    return RouterConfig(
        version=1,
        models=[
            ModelConfig(
                name="m",
                aliases=[],
                allowed_groups=[],
                allowed_domains=[],
                supported_endpoints=["chat/completions"],
                usage_limits={},  # type: ignore[arg-type]
                overload={},  # type: ignore[arg-type]
                deployments=[
                    DeploymentConfig(
                        kind="static",
                        name="dep",
                        cluster_name="test-cluster",
                        router_params={},  # type: ignore[arg-type]
                        prometheus_metrics_path=None,
                        prometheus_scrape_interval_sec=30,
                        backends=[
                            BackendConfig(
                                id="b1",
                                model_url=model_url,
                                backend_model_name="upstream",
                                api_key=None,
                            )
                        ],
                    )
                ],
            )
        ],
    )


async def test_recreated_client_stays_open_for_grace_period(
    pilot_mtls_settings: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(bcm, "CLOSE_GRACE_SEC", 0)
    manager = bcm.BackendClientManager(Settings())
    try:
        await manager.on_config_swap(_config("http://old/v1"))
        old = manager.get("b1")
        assert old is not None

        await manager.on_config_swap(_config("http://new/v1"))
        new = manager.get("b1")
        assert new is not None and new is not old
        assert str(new.base_url) == "http://new/v1/"
        # In-flight requests holding the old client can still finish
        closed_on_swap = old.is_closed
        assert not closed_on_swap

        await asyncio.gather(*bcm._background_tasks)
        assert old.is_closed
        assert not new.is_closed
    finally:
        await manager.close_all()


@pytest.mark.parametrize("kind", ["static", "pilot"])
async def test_backend_stream_request_allows_long_prefill(
    pilot_mtls_settings: None, kind: str
) -> None:
    config = _config("https://backend.example/v1")
    config.models[0].deployments[0] = (
        config.models[0].deployments[0].model_copy(update={"kind": kind})
    )
    manager = bcm.BackendClientManager(Settings())
    try:
        await manager.on_config_swap(config)
        client = manager.get("b1")
        assert client is not None
        expected = {"connect": 5.0, "read": 900.0, "write": 60.0, "pool": 5.0}
        assert client.timeout.as_dict() == expected
        request = client.build_request(
            "POST", "chat/completions", json={"stream": True}
        )
        assert request.extensions["timeout"] == expected
    finally:
        await manager.close_all()


@pytest.mark.parametrize("stream", [False, True])
async def test_inference_request_uses_long_read_budget(
    stream: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Inspect real httpx request extensions; no timed sleep or upstream socket."""
    observed: list[dict[str, float | None]] = []

    def upstream(request: httpx.Request) -> httpx.Response:
        observed.append(request.extensions["timeout"])
        if stream:
            return httpx.Response(200, stream=httpx.ByteStream(b"data: [DONE]\n\n"))
        return httpx.Response(200, json={"choices": [], "usage": {}})

    config = _config("https://backend.example/v1")
    model = config.models[0]
    deployment = model.deployments[0]
    backend = deployment.backends[0]
    service = InferenceService.__new__(InferenceService)
    service.request_id = "long-prefill-test"
    service.admission_controller = AsyncMock()
    monkeypatch.setattr(service, "_record_token_stats", AsyncMock())
    monkeypatch.setattr(service, "_emit_inference_log", Mock())
    payload = OpenAIChatCompletionsPayload(model="m", messages=[], stream=stream)
    # Unary must override a deliberately short client default; streaming uses
    # the same default as the manager's actual client tested above.
    default = bcm.STREAM_TIMEOUT if stream else httpx.Timeout(0.01)
    async with httpx.AsyncClient(
        base_url=backend.model_url,
        transport=httpx.MockTransport(upstream),
        timeout=default,
    ) as client:
        if stream:
            response = await service._handle_streaming(
                client, payload, model, deployment, backend
            )
            assert [chunk async for chunk in response.body_iterator] == [
                b"data: [DONE]\n\n"
            ]
        else:
            await service._handle_unary(client, payload, model, deployment, backend)
    assert observed == [{"connect": 5.0, "read": 900.0, "write": 60.0, "pool": 5.0}]


async def test_bad_backend_does_not_block_others(
    pilot_mtls_settings: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("FIRST_TEST_UNSET_KEY", raising=False)
    cfg = _config("http://good/v1")
    backends = cfg.models[0].deployments[0].backends
    backends.insert(
        0,
        BackendConfig(
            id="bad",
            model_url="http://bad/v1",
            backend_model_name="upstream",
            api_key="env_var://FIRST_TEST_UNSET_KEY",
        ),
    )
    manager = bcm.BackendClientManager(Settings())
    try:
        with pytest.raises(RuntimeError, match="bad"):
            await manager.on_config_swap(cfg)
        assert manager.get("b1") is not None
        assert manager.get("bad") is None
    finally:
        await manager.close_all()


async def test_failed_update_keeps_previous_client(
    pilot_mtls_settings: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("FIRST_TEST_UNSET_KEY", raising=False)
    manager = bcm.BackendClientManager(Settings())
    try:
        await manager.on_config_swap(_config("http://old/v1"))
        old = manager.get("b1")
        assert old is not None

        cfg = _config("http://old/v1")
        cfg.models[0].deployments[0].backends[
            0
        ].api_key = "env_var://FIRST_TEST_UNSET_KEY"
        with pytest.raises(RuntimeError):
            await manager.on_config_swap(cfg)
        assert manager.get("b1") is old
        assert not old.is_closed
    finally:
        await manager.close_all()
