import asyncio
import json
import socket
from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest
from aiohttp import web
from fastapi.responses import StreamingResponse

from first_common.schema.endpoints.base import SESSION_HEADER
from first_common.schema.endpoints.generic import (
    TASK_USER_ID_HEADER,
    GenericTaskPayload,
)
from first_common.schema.endpoints.llm import OpenAIChatCompletionsPayload
from first_common.schema.endpoints.token_estimation import TokenEstimate
from first_gateway.apiserver import backend_client_manager as bcm
from first_gateway.apiserver.inference import InferenceService, _UpstreamFailure
from first_gateway.database.redis.router_config import (
    BackendConfig,
    DeploymentConfig,
    ModelConfig,
    RouterConfig,
)
from first_gateway.settings import Settings

from .fixtures import backend as fake

pytestmark = pytest.mark.usefixtures("stop_backends")


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
        assert new.base_url == "http://new/v1"
        # In-flight requests holding the old client can still finish
        closed_on_swap = old.is_closed
        assert not closed_on_swap

        await asyncio.gather(*bcm._background_tasks)
        assert old.is_closed
        assert not new.is_closed
    finally:
        await manager.close_all()


@pytest.mark.parametrize("kind", ["static", "pilot"])
async def test_backend_client_allows_long_prefill(
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
        # No end-to-end deadline: only connecting and silence are bounded.
        assert client.timeout.total is None
        assert client.timeout.sock_connect == 5.0
        assert client.timeout.sock_read == 900.0
    finally:
        await manager.close_all()


def _service(monkeypatch: pytest.MonkeyPatch) -> InferenceService:
    service = InferenceService.__new__(InferenceService)
    service.request_id = "handler-test"
    service.user = Mock(id="user-uuid")
    service.admission_controller = AsyncMock()
    service.estimate = TokenEstimate()
    service.session_id = None
    service.sticky = None
    monkeypatch.setattr(service, "_emit_inference_log", Mock())
    return service


async def test_unary_relays_backend_body_as_received(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[tuple[str, dict[str, object]]] = []
    # Not what json.dumps would produce: the bytes must pass through untouched.
    raw = b'{"choices": [],   "usage": {"total_tokens": 7}}'

    async def upstream(request: web.Request) -> web.StreamResponse:
        seen.append((request.path, await request.json()))
        return web.Response(body=raw, content_type="application/json")

    model = _config("https://backend.example/v1").models[0]
    deployment = model.deployments[0]
    payload = OpenAIChatCompletionsPayload(model="m", messages=[])
    client = await fake.serve_backend(upstream)
    response = await _service(monkeypatch)._handle_unary(
        client, payload, model, deployment, deployment.backends[0]
    )
    assert response.body == raw
    assert response.media_type == "application/json"
    assert seen == [("/v1/chat/completions", {"model": "m", "messages": []})]


async def test_stream_relays_backend_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    model = _config("https://backend.example/v1").models[0]
    deployment = model.deployments[0]
    payload = OpenAIChatCompletionsPayload(model="m", messages=[], stream=True)
    client = await fake.serve_backend(fake.stream([b"data: [DONE]\n\n"]))
    service = _service(monkeypatch)
    response = await service._handle_streaming(
        client, payload, model, deployment, deployment.backends[0]
    )
    assert isinstance(response, StreamingResponse)
    relayed = b"".join([chunk async for chunk in response.body_iterator])  # type: ignore[misc]
    assert relayed == b"data: [DONE]\n\n"
    settle = service.admission_controller.settle
    assert isinstance(settle, AsyncMock)
    settle.assert_awaited_once()


async def test_task_sends_caller_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[tuple[str | None, bytes]] = []

    async def upstream(request: web.Request) -> web.StreamResponse:
        seen.append((request.headers.get(TASK_USER_ID_HEADER), await request.read()))
        return web.json_response({})

    model = _config("https://backend.example/v1").models[0]
    deployment = model.deployments[0]
    payload = GenericTaskPayload(model="m", endpoint="segmentation", input_dir="/x")  # type: ignore[call-arg]
    client = await fake.serve_backend(upstream)
    await _service(monkeypatch)._handle_unary(
        client, payload, model, deployment, deployment.backends[0]
    )
    ((user_id, body),) = seen
    assert user_id == "user-uuid"
    assert "endpoint" not in json.loads(body)


async def test_session_header_is_forwarded(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[tuple[str | None, str | None]] = []

    async def upstream(request: web.Request) -> web.StreamResponse:
        seen.append(
            (
                request.headers.get(SESSION_HEADER),
                request.headers.get(TASK_USER_ID_HEADER),
            )
        )
        if (await request.json()).get("stream"):
            resp = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
            await resp.prepare(request)
            await resp.write(b"data: [DONE]\n\n")
            return resp
        return web.json_response({})

    model = _config("https://backend.example/v1").models[0]
    deployment = model.deployments[0]
    backend = deployment.backends[0]
    client = await fake.serve_backend(upstream)
    service = _service(monkeypatch)
    service.session_id = "sess-42"

    unary = OpenAIChatCompletionsPayload(model="m", messages=[])
    await service._handle_unary(client, unary, model, deployment, backend)
    task = GenericTaskPayload(model="m", endpoint="segmentation", input_dir="/x")  # type: ignore[call-arg]
    await service._handle_unary(client, task, model, deployment, backend)
    stream = OpenAIChatCompletionsPayload(model="m", messages=[], stream=True)
    response = await service._handle_streaming(
        client, stream, model, deployment, backend
    )
    assert isinstance(response, StreamingResponse)
    _ = [chunk async for chunk in response.body_iterator]

    assert seen == [
        ("sess-42", None),
        ("sess-42", "user-uuid"),
        ("sess-42", None),
    ]


async def test_timed_out_task_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    async def upstream(request: web.Request) -> web.StreamResponse:
        await asyncio.sleep(1.0)
        return web.json_response({})

    model = _config("https://backend.example/v1").models[0]
    deployment = model.deployments[0]
    payload = GenericTaskPayload(model="m", endpoint="segmentation")
    client = await fake.serve_backend(
        upstream, timeout=aiohttp.ClientTimeout(sock_read=0.05)
    )
    with pytest.raises(_UpstreamFailure) as exc:
        await _service(monkeypatch)._handle_unary(
            client, payload, model, deployment, deployment.backends[0]
        )
    assert exc.value.retryable is False


async def test_refused_connection_is_retried_then_fails_over(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(bcm, "CONNECT_RETRY_DELAY_SEC", 0)
    with socket.socket() as sock:  # a port with nothing listening
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    client = bcm.BackendClient(f"http://127.0.0.1:{port}/v1")
    attempts = 0
    post = client._session.post

    def counting_post(*args: object, **kwargs: object) -> object:
        nonlocal attempts
        attempts += 1
        return post(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(client._session, "post", counting_post)
    model = _config("https://backend.example/v1").models[0]
    deployment = model.deployments[0]
    payload = OpenAIChatCompletionsPayload(model="m", messages=[])
    try:
        with pytest.raises(_UpstreamFailure) as exc:
            await _service(monkeypatch)._handle_unary(
                client, payload, model, deployment, deployment.backends[0]
            )
    finally:
        await client.aclose()
    assert attempts == 1 + bcm.CONNECT_RETRIES
    assert exc.value.retryable is True


async def test_bad_backend_does_not_block_others(
    pilot_mtls_settings: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("FIRST_UPSTREAM_KEY_TEST_UNSET", raising=False)
    cfg = _config("http://good/v1")
    backends = cfg.models[0].deployments[0].backends
    backends.insert(
        0,
        BackendConfig(
            id="bad",
            model_url="http://bad/v1",
            backend_model_name="upstream",
            api_key="env_var://FIRST_UPSTREAM_KEY_TEST_UNSET",
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
    monkeypatch.delenv("FIRST_UPSTREAM_KEY_TEST_UNSET", raising=False)
    manager = bcm.BackendClientManager(Settings())
    try:
        await manager.on_config_swap(_config("http://old/v1"))
        old = manager.get("b1")
        assert old is not None

        cfg = _config("http://old/v1")
        cfg.models[0].deployments[0].backends[
            0
        ].api_key = "env_var://FIRST_UPSTREAM_KEY_TEST_UNSET"
        with pytest.raises(RuntimeError):
            await manager.on_config_swap(cfg)
        assert manager.get("b1") is old
        assert not old.is_closed
    finally:
        await manager.close_all()


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://login.cluster-a.example:9443/models/m", None),
        ("https://other.example/v1", "socks5h://tunnel:1080"),
        ("http://localhost:8000/v1", None),
        # CIDR entries exempt IP-addressed hosts inside the network only.
        ("https://10.124.165.41:9443/replicas/r/", None),
        ("https://10.125.0.1:9443/replicas/r/", "socks5h://tunnel:1080"),
        ("https://[fd00::5]:9443/replicas/r/", None),
    ],
)
def test_egress_proxy_honours_no_proxy(
    monkeypatch: pytest.MonkeyPatch, url: str, expected: str | None
) -> None:
    for var in ("HTTPS_PROXY", "https_proxy", "no_proxy"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("ALL_PROXY", "socks5h://tunnel:1080")
    monkeypatch.setenv(
        "NO_PROXY",
        "localhost,127.0.0.1,login.cluster-a.example,10.124.0.0/16,fd00::/64",
    )
    assert bcm._egress_proxy(url) == expected


def test_egress_proxy_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in ("ALL_PROXY", "all_proxy", "HTTPS_PROXY", "https_proxy"):
        monkeypatch.delenv(var, raising=False)
    assert bcm._egress_proxy("https://other.example/v1") is None


async def _socks5_proxy(seen: list[str]) -> asyncio.Server:
    """A minimal SOCKS5 proxy (no auth, CONNECT only) that records the
    destinations it was asked for."""

    async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while data := await reader.read(65536):
                writer.write(data)
                await writer.drain()
        finally:
            writer.close()

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        _, n_methods = await reader.readexactly(2)
        await reader.readexactly(n_methods)
        writer.write(b"\x05\x00")
        _, _, _, addr_type = await reader.readexactly(4)
        if addr_type == 3:  # a name, for the proxy to resolve
            host = (await reader.readexactly((await reader.readexactly(1))[0])).decode()
        else:
            host = socket.inet_ntoa(await reader.readexactly(4))
        port = int.from_bytes(await reader.readexactly(2))
        seen.append(f"{host}:{port}")
        up_reader, up_writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"\x05\x00\x00\x01" + bytes(6))
        await asyncio.gather(pipe(reader, up_writer), pipe(up_reader, writer))

    return await asyncio.start_server(handle, "127.0.0.1", 0)


async def test_backend_reached_through_socks_proxy() -> None:
    direct = await fake.serve_backend(fake.respond(json={"ok": True}))
    port = direct.base_url.rsplit(":", 1)[1]
    seen: list[str] = []
    proxy = await _socks5_proxy(seen)
    proxy_port = proxy.sockets[0].getsockname()[1]
    # A name only the proxy can resolve, as with a cluster behind the tunnel
    client = bcm.BackendClient(
        f"http://backend.cluster.invalid:{port}",
        proxy=f"socks5h://127.0.0.1:{proxy_port}",
    )
    try:
        async with await client.post("/v1/chat/completions", b"{}") as response:
            assert response.status == 200
            assert await response.json() == {"ok": True}
    finally:
        await client.aclose()
        proxy.close()
    assert seen == [f"backend.cluster.invalid:{port}"]


def test_backend_sockets_bound_unacknowledged_writes() -> None:
    addr_info = (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ())
    with bcm._keepalive_socket(addr_info) as sock:
        assert sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE)
        if not hasattr(socket, "TCP_USER_TIMEOUT"):
            pytest.skip("TCP_USER_TIMEOUT is Linux-only")
        value = sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_USER_TIMEOUT)
        assert value == bcm.WRITE_STALL_TIMEOUT_MS
