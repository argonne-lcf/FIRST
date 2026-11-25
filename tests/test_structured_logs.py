import asyncio
import inspect
import json
import logging
import os
import stat
import time
from pathlib import Path
from typing import Any, Callable, Iterator, MutableMapping

import httpx
import pytest
from aiohttp import web
from fastapi.dependencies.utils import get_flat_dependant
from fastapi.routing import APIRoute

from first_common.schema.structured_logs import (
    INLINE_BODY_LIMIT,
    InferenceLog,
    RequestLog,
    ResponseLog,
    _background_tasks,
)
from first_gateway.apiserver import inference
from first_gateway.apiserver.api import app
from first_gateway.apiserver.context import _request_id_ctx, get_request_id
from first_gateway.apiserver.dependencies import _INFERENCE_PATH_PREFIXES
from first_gateway.apiserver.inference import (
    DEPLOYMENT_HEADER,
    get_inference_service,
)
from first_gateway.database.redis.keys import Keys
from first_gateway.database.redis.router_config import (
    BackendConfig,
    DeploymentConfig,
    ModelConfig,
    RouterConfig,
)
from first_gateway.log_config import RequestIdFilter

from .fixtures import backend
from .fixtures.auth import INVALID_TOKEN, USER_TOKEN, auth_header

pytestmark = pytest.mark.usefixtures("stop_backends")

# --------------------------------------------------------------------------
# Body inline-vs-file decision
# --------------------------------------------------------------------------


def _request_log(request_id: str = "rid-1") -> RequestLog:
    return RequestLog(
        request_id=request_id,
        user_id="u1",
        user_name="User One",
        username="user@anl.gov",
        user_group_uuids=[],
        authorized_group_uuids=None,
        idp_id="idp",
        idp_name="IdP",
        auth_service="globus",
        method="POST",
        path="/federated/v1/chat/completions",
        origin_ip="1.2.3.4",
        content_length=10,
    )


async def test_small_body_inlined_no_file(tmp_path: Path) -> None:
    log = _request_log()
    raw = b"hello world"
    log.emit(raw_body=raw, storage_dir=tmp_path)

    assert log.body == "hello world"
    assert log.body_stored is False
    # No file written for a small body.
    assert list(tmp_path.rglob("*.json")) == []


async def test_large_body_written_to_file(tmp_path: Path) -> None:
    log = _request_log(request_id="rid-big")
    raw = b"x" * (INLINE_BODY_LIMIT + 5)
    log.emit(raw_body=raw, storage_dir=tmp_path)

    assert log.body is None
    assert log.body_stored is True

    # The write is fire-and-forget; await the scheduled task(s).
    await asyncio.gather(*list(_background_tasks))

    files = list(tmp_path.rglob("*.json"))
    assert len(files) == 1
    written = files[0]
    # Canonical layout: large-requests/YYYY/MM/DD/{uuid}.json
    assert written.name == "rid-big.json"
    assert written.parent.parent.parent.parent.name == "large-requests"
    assert written.read_bytes() == raw


async def test_large_body_file_is_owner_only(tmp_path: Path) -> None:
    """Bodies are user content: 0600 files in 0700 directories, at every
    level the gateway creates and whatever the process umask."""
    old_umask = os.umask(0o022)
    try:
        log = _request_log(request_id="rid-mode")
        log.emit(raw_body=b"x" * INLINE_BODY_LIMIT, storage_dir=tmp_path)
        await asyncio.gather(*list(_background_tasks))
    finally:
        os.umask(old_umask)

    [written] = tmp_path.rglob("*.json")
    assert stat.S_IMODE(written.stat().st_mode) == 0o600
    created_dirs = [p for p in tmp_path.rglob("*") if p.is_dir()]
    assert len(created_dirs) == 4  # large-requests/YYYY/MM/DD
    assert {stat.S_IMODE(d.stat().st_mode) for d in created_dirs} == {0o700}


async def test_large_inference_body_uses_responses_dir(tmp_path: Path) -> None:
    log = InferenceLog(
        request_id="rid-resp",
        user_id="u",
        endpoint="chat/completions",
        model="m",
        deployment="d",
        cluster="c",
        backend_id="b",
        backend_model_url="http://x/v1",
        latency_sec=0.5,
        input_tokens=None,
        output_tokens=None,
        total_tokens=None,
        cache_read_tokens=None,
        cache_write_tokens=None,
        reasoning_tokens=None,
    )
    raw = b"e" * (INLINE_BODY_LIMIT + 1)
    log.emit(raw_body=raw, storage_dir=tmp_path)
    await asyncio.gather(*list(_background_tasks))

    files = list(tmp_path.rglob("*.json"))
    assert len(files) == 1
    assert files[0].parent.parent.parent.parent.name == "large-responses"


async def test_large_body_dropped_without_storage_dir() -> None:
    log = ResponseLog(
        request_id="rid-drop", status_code=500, duration_ms=1.5, streaming=False
    )
    log.emit(raw_body=b"e" * (INLINE_BODY_LIMIT + 1))

    assert log.body is None
    assert log.body_stored is False
    assert not _background_tasks


async def test_small_body_inlined_without_storage_dir() -> None:
    log = ResponseLog(
        request_id="rid-err", status_code=404, duration_ms=1.5, streaming=False
    )
    log.emit(raw_body=b'{"detail":"nope"}')

    assert log.body == '{"detail":"nope"}'
    assert log.body_stored is False


# --------------------------------------------------------------------------
# RequestIdFilter
# --------------------------------------------------------------------------


def _make_record() -> logging.LogRecord:
    return logging.LogRecord(
        name="first_common",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="x",
        args=(),
        exc_info=None,
    )


def test_filter_stamps_request_id_when_set() -> None:
    token = _request_id_ctx.set("abc-123")
    try:
        record = _make_record()
        assert RequestIdFilter().filter(record) is True
        assert record.request_id == "abc-123"  # type: ignore[attr-defined]
    finally:
        _request_id_ctx.reset(token)


def test_filter_absent_when_unset() -> None:
    assert get_request_id() is None
    record = _make_record()
    assert RequestIdFilter().filter(record) is True
    assert not hasattr(record, "request_id")


# --------------------------------------------------------------------------
# Per-stream serialization
# --------------------------------------------------------------------------


def test_stream_discriminators_and_fields() -> None:
    req = _request_log().model_dump(mode="json")
    assert req["stream"] == "request"
    assert req["username"] == "user@anl.gov"

    resp = ResponseLog(
        request_id="r", status_code=200, duration_ms=2.0, streaming=True
    ).model_dump(mode="json")
    assert resp["stream"] == "response"
    assert resp["streaming"] is True

    inf = InferenceLog(
        request_id="r",
        user_id="u",
        endpoint="chat/completions",
        model="m",
        deployment="d",
        cluster="c",
        backend_id="b",
        backend_model_url="http://x/v1",
        latency_sec=0.5,
        input_tokens=1,
        output_tokens=2,
        total_tokens=3,
        cache_read_tokens=None,
        cache_write_tokens=None,
        reasoning_tokens=None,
    ).model_dump(mode="json")
    assert inf["stream"] == "inference"
    assert inf["outcome"] == "success"
    assert inf["attempt"] == 1
    assert inf["error"] is None
    assert inf["cluster"] == "c"


# --------------------------------------------------------------------------
# End-to-end correlation
# --------------------------------------------------------------------------


class _CaptureHandler(logging.Handler):
    """Collects the structured-log records emitted under `first_common`."""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[dict[str, Any]] = []

    def emit(self, record: logging.LogRecord) -> None:
        # `stream` is the log msg for our events; capture only those.
        payload = getattr(record, "__dict__", {})
        if getattr(record, "stream", None):
            self.records.append(dict(payload))


@pytest.fixture
def capture_events() -> Iterator[list[dict[str, Any]]]:
    handler = _CaptureHandler()
    # Attach to the exact module logger the events use.
    logger = logging.getLogger("first_common.schema.structured_logs")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        yield handler.records
    finally:
        logger.removeHandler(handler)


def _seed_streaming_config(
    usage_limits: dict[str, Any] | None = None,
    with_backend: bool = True,
    endpoints: tuple[str, ...] = ("chat/completions",),
    backend_ids: tuple[str, ...] = ("static_deployment/dep-open",),
) -> None:
    backends = [
        BackendConfig(
            id=backend_id,
            model_url="http://backend.invalid/v1",
            backend_model_name="upstream",
            api_key=None,
        )
        for backend_id in backend_ids
    ]
    cfg = RouterConfig(
        version=1,
        models=[
            ModelConfig(
                name="open/model",
                aliases=[],
                allowed_groups=[],
                allowed_domains=[],
                supported_endpoints=list(endpoints),
                usage_limits=usage_limits or {},  # type: ignore[arg-type]
                overload={},  # type: ignore[arg-type]
                deployments=[
                    DeploymentConfig(
                        kind="static",
                        name="dep-open",
                        cluster_name="test-cluster",
                        router_params={},  # type: ignore[arg-type]
                        prometheus_metrics_path=None,
                        prometheus_scrape_interval_sec=30,
                        backends=backends if with_backend else [],
                    )
                ],
            )
        ],
    )
    app.state.router_config_manager._current = cfg


_SSE_CHUNKS = [
    b'data: {"choices":[{"delta":{"content":"hi"}}],"usage":null}\n\n',
    b'data: {"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":5,'
    b'"total_tokens":8}}\n\n',
    b"data: [DONE]\n\n",
]


async def _inject_backend(handler: backend.Handler) -> None:
    """Point the seeded deployment at a local server running ``handler``."""
    app.state.backend_client_manager._clients[
        "static_deployment/dep-open"
    ] = await backend.serve_backend(handler)


async def _inject_streaming_backend() -> None:
    await _inject_backend(backend.stream(_SSE_CHUNKS))


async def test_three_events_share_request_id_streaming(
    client: httpx.AsyncClient, capture_events: list[dict[str, Any]]
) -> None:
    _seed_streaming_config()
    await _inject_streaming_backend()

    async with client.stream(
        "POST",
        "/federated/v1/chat/completions",
        headers=auth_header(USER_TOKEN),
        json={
            "model": "open/model",
            "messages": [{"role": "user", "content": "hi"}],
            "stream": True,
        },
    ) as resp:
        assert resp.status_code == 200
        relayed = b"".join([chunk async for chunk in resp.aiter_raw()])

    # The SSE stream is relayed intact.
    assert b'"content":"hi"' in relayed
    assert b"[DONE]" in relayed

    # Let the relay's finally-block (InferenceLog emit) and the middleware run.
    await asyncio.sleep(0.05)

    by_stream = {r["stream"]: r for r in capture_events}
    assert {"request", "response", "inference"} <= set(by_stream)

    rid = by_stream["request"]["request_id"]
    assert by_stream["response"]["request_id"] == rid
    assert by_stream["inference"]["request_id"] == rid

    # Streaming response: middleware reports streaming, buffers no body.
    assert by_stream["response"]["streaming"] is True
    assert by_stream["response"]["body"] is None

    # The streaming *content* is persisted by the InferenceLog path (inline here,
    # since the assembled body is small).
    inf = by_stream["inference"]
    assert inf["cluster"] == "test-cluster"
    assert inf["total_tokens"] == 8
    assert inf["body_stored"] is False
    assert inf["body"] is not None
    assert "[DONE]" in inf["body"]

    # RequestLog carries the flattened user identity.
    assert by_stream["request"]["username"] == "user@anl.gov"
    # The request body was small -> inlined.
    assert json.loads(by_stream["request"]["body"])["model"] == "open/model"


async def test_messages_stream_logs_total_tokens(
    client: httpx.AsyncClient, capture_events: list[dict[str, Any]]
) -> None:
    """Anthropic reports no total; the relay computes one (cache tokens
    included) so the attempt is logged and settled at its real size."""
    _seed_streaming_config(endpoints=("chat/completions", "messages"))
    await _inject_backend(
        backend.stream(
            [
                b"event: message_start\ndata: "
                b'{"type":"message_start","message":{"usage":{"input_tokens":3,'
                b'"cache_read_input_tokens":4,"output_tokens":1}}}\n\n',
                b"event: content_block_delta\ndata: "
                b'{"type":"content_block_delta","delta":{"text":"hi"}}\n\n',
                b"event: message_delta\ndata: "
                b'{"type":"message_delta","usage":{"output_tokens":5}}\n\n',
                b'event: message_stop\ndata: {"type":"message_stop"}\n\n',
            ]
        )
    )

    async with client.stream(
        "POST",
        "/federated/v1/messages",
        headers=auth_header(USER_TOKEN),
        json={**_CHAT, "max_tokens": 16, "stream": True},
    ) as resp:
        assert resp.status_code == 200
        relayed = b"".join([chunk async for chunk in resp.aiter_raw()])
    assert b"message_stop" in relayed
    await asyncio.sleep(0.05)

    (inf,) = [r for r in capture_events if r["stream"] == "inference"]
    assert inf["outcome"] == "success"
    assert inf["input_tokens"] == 3
    assert inf["output_tokens"] == 5
    assert inf["total_tokens"] == 12


async def test_inference_log_carries_token_estimation_calibration(
    client: httpx.AsyncClient, capture_events: list[dict[str, Any]]
) -> None:
    """The first request is estimated from static defaults (no EWMA yet); the
    second carries the EWMA learned from the first's actual usage."""
    _seed_streaming_config()
    await _inject_streaming_backend()
    content = "x" * 30  # 30 chars against 3 actual prompt tokens -> 10 cpt

    async def send() -> dict[str, Any]:
        capture_events.clear()
        async with client.stream(
            "POST",
            "/federated/v1/chat/completions",
            headers=auth_header(USER_TOKEN),
            json={
                "model": "open/model",
                "messages": [{"role": "user", "content": content}],
                "max_tokens": 100,
                "stream": True,
            },
        ) as resp:
            assert resp.status_code == 200
            await resp.aread()
        await asyncio.sleep(0.05)
        return next(r for r in capture_events if r["stream"] == "inference")

    first = await send()
    assert first["input_chars"] == 30
    assert first["est_chars_per_token"] is None
    assert first["est_output_tokens"] is None
    # Static defaults: 30 chars / 4 cpt -> 7 input + max_tokens 100 output.
    assert first["estimated_tokens"] == 107

    second = await send()
    assert second["est_chars_per_token"] == pytest.approx(10.0)
    assert second["est_output_tokens"] == pytest.approx(5.0)
    # Learned: 30 / 10 cpt -> 3 input + learned 5 output.
    assert second["estimated_tokens"] == 8


async def test_non_inference_request_body_not_logged(
    client: httpx.AsyncClient, capture_events: list[dict[str, Any]]
) -> None:
    resp = await client.get("/whoami", headers=auth_header(USER_TOKEN))
    assert resp.status_code == 200
    await asyncio.sleep(0.05)

    by_stream = {r["stream"]: r for r in capture_events}
    assert by_stream["request"]["path"] == "/whoami"
    assert by_stream["request"]["body"] is None
    assert by_stream["request"]["body_stored"] is False
    assert by_stream["response"]["body"] is None


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        # The fronting NGINX's X-Real-IP wins over a spoofed X-Forwarded-For.
        ({"X-Real-IP": "192.0.2.7", "X-Forwarded-For": "203.0.113.9"}, "192.0.2.7"),
        # X-Forwarded-For alone is ignored: fall back to the socket peer
        # (httpx.ASGITransport's default client address).
        ({"X-Forwarded-For": "203.0.113.9"}, "127.0.0.1"),
        ({}, "127.0.0.1"),
    ],
)
async def test_request_log_origin_ip(
    client: httpx.AsyncClient,
    capture_events: list[dict[str, Any]],
    headers: dict[str, str],
    expected: str,
) -> None:
    resp = await client.get("/whoami", headers={**auth_header(USER_TOKEN), **headers})
    assert resp.status_code == 200
    requests = [r for r in capture_events if r["stream"] == "request"]
    assert len(requests) == 1
    assert requests[0]["origin_ip"] == expected


@pytest.mark.parametrize(
    "path",
    [
        "/federated/v1/chat/completions",
        "/deployments/test/v1/responses",
        "/resource_server/minerva/api/v1/messages",
    ],
)
@pytest.mark.parametrize("body_size", [100, 3072])
async def test_rejected_auth_drops_body_without_storage(
    client: httpx.AsyncClient,
    capture_events: list[dict[str, Any]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    body_size: int,
) -> None:
    monkeypatch.setattr(app.state.client_state.settings, "prompt_storage_dir", tmp_path)
    # Valid JSON of exactly body_size bytes, above the file-spill threshold at 3 KB.
    body = b'{"private":"' + b"x" * (body_size - 14) + b'"}'
    assert len(body) == body_size
    response = await client.post(
        path,
        headers={
            **auth_header(INVALID_TOKEN),
            "Content-Type": "application/json",
            "X-Real-IP": "192.0.2.42",
        },
        content=body,
    )
    assert response.status_code == 401
    # Await any mistakenly scheduled spill, so checking the filesystem cannot race it.
    await asyncio.gather(*list(_background_tasks))
    assert list(tmp_path.rglob("*")) == []
    requests = [r for r in capture_events if r["stream"] == "request"]
    assert len(requests) == 1
    event = requests[0]
    assert event["body"] is None
    assert event["body_stored"] is False
    assert event["user_id"] is None
    assert event["request_id"]
    assert event["method"] == "POST"
    assert event["path"] == path
    assert event["origin_ip"] == "192.0.2.42"
    assert event["content_length"] == body_size
    assert not any(r["stream"] == "inference" for r in capture_events)


def test_inference_routes_match_body_logging_prefixes() -> None:
    """Every route using InferenceService must fall under a prefix whose request
    bodies get_auth_user logs; otherwise its bodies are silently not logged."""
    inference_paths = [
        route.path
        for route in app.routes
        if isinstance(route, APIRoute)
        and any(
            dep.call is get_inference_service
            for dep in get_flat_dependant(route.dependant).dependencies
        )
    ]
    assert inference_paths
    # Built on the event loop: a sync dependency would cost a thread hop.
    assert inspect.iscoroutinefunction(get_inference_service)
    unmatched = [
        p for p in inference_paths if not p.startswith(_INFERENCE_PATH_PREFIXES)
    ]
    assert unmatched == []


# --------------------------------------------------------------------------
# Deployment header on every relayed response
# --------------------------------------------------------------------------


_CHAT = {"model": "open/model", "messages": [{"role": "user", "content": "hi"}]}


async def test_streaming_response_names_deployment(client: httpx.AsyncClient) -> None:
    _seed_streaming_config()
    await _inject_streaming_backend()
    async with client.stream(
        "POST",
        "/federated/v1/chat/completions",
        headers=auth_header(USER_TOKEN),
        json={**_CHAT, "stream": True},
    ) as resp:
        assert resp.status_code == 200
        assert resp.headers[DEPLOYMENT_HEADER] == "dep-open"
        await resp.aread()


async def test_unary_response_names_deployment(client: httpx.AsyncClient) -> None:
    _seed_streaming_config()
    body = {
        "id": "x",
        "object": "chat.completion",
        "choices": [],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
    }
    await _inject_backend(backend.respond(json=body))
    resp = await client.post(
        "/federated/v1/chat/completions", headers=auth_header(USER_TOKEN), json=_CHAT
    )
    assert resp.status_code == 200
    assert resp.headers[DEPLOYMENT_HEADER] == "dep-open"
    # The body is relayed untouched.
    assert resp.json() == body


async def test_upstream_4xx_names_deployment(client: httpx.AsyncClient) -> None:
    _seed_streaming_config()
    await _inject_backend(backend.respond(400, text="bad request"))
    resp = await client.post(
        "/federated/v1/chat/completions", headers=auth_header(USER_TOKEN), json=_CHAT
    )
    assert resp.status_code == 400
    assert resp.headers[DEPLOYMENT_HEADER] == "dep-open"


# --------------------------------------------------------------------------
# Server-Timing header on every response
# --------------------------------------------------------------------------


def _server_timing(resp: httpx.Response) -> dict[str, float]:
    """Parse ``name;dur=<ms>[;desc=...]`` entries into {name: ms}."""
    out: dict[str, float] = {}
    for entry in resp.headers["server-timing"].split(","):
        name, *params = (part.strip() for part in entry.split(";"))
        out[name] = next(float(p[4:]) for p in params if p.startswith("dur="))
    return out


async def test_unary_response_reports_gateway_and_upstream_time(
    client: httpx.AsyncClient,
) -> None:
    _seed_streaming_config()
    body = {
        "id": "x",
        "object": "chat.completion",
        "choices": [],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
    }

    async def slow_backend(request: web.Request) -> web.StreamResponse:
        await asyncio.sleep(0.05)
        return web.json_response(body)

    await _inject_backend(slow_backend)
    resp = await client.post(
        "/federated/v1/chat/completions", headers=auth_header(USER_TOKEN), json=_CHAT
    )
    assert resp.status_code == 200
    timing = _server_timing(resp)
    assert 50 <= timing["upstream"] <= timing["gateway"]
    assert 'desc="attempts=1"' in resp.headers["server-timing"]
    assert "queue" not in timing  # no proxy stamp on this request


async def test_streaming_response_reports_time_to_upstream_headers(
    client: httpx.AsyncClient,
) -> None:
    _seed_streaming_config()
    await _inject_streaming_backend()
    async with client.stream(
        "POST",
        "/federated/v1/chat/completions",
        headers=auth_header(USER_TOKEN),
        json={**_CHAT, "stream": True},
    ) as resp:
        timing = _server_timing(resp)
        assert 0 <= timing["upstream"] <= timing["gateway"]
        await resp.aread()


async def test_upstream_error_still_reports_upstream_time(
    client: httpx.AsyncClient,
) -> None:
    _seed_streaming_config()
    await _inject_backend(backend.respond(400, text="bad request"))
    resp = await client.post(
        "/federated/v1/chat/completions", headers=auth_header(USER_TOKEN), json=_CHAT
    )
    assert resp.status_code == 400
    assert {"gateway", "upstream"} <= _server_timing(resp).keys()


@pytest.mark.parametrize(
    "stamp, expect_queue",
    [
        (lambda: f"t={time.time() - 0.25:.3f}", True),
        (lambda: "t=garbage", False),
        (lambda: f"t={time.time() + 60:.3f}", False),  # proxy clock ahead of ours
    ],
)
async def test_queue_time_from_proxy_stamp(
    client: httpx.AsyncClient, stamp: Callable[[], str], expect_queue: bool
) -> None:
    resp = await client.get("/health", headers={"X-Request-Start": stamp()})
    timing = _server_timing(resp)
    assert "upstream" not in timing  # no backend was called
    assert ("queue" in timing) is expect_queue
    if expect_queue:
        assert 240 <= timing["queue"] < 5000


# --------------------------------------------------------------------------
# Admission rejections are logged
# --------------------------------------------------------------------------


async def _post_and_get_log(
    client: httpx.AsyncClient,
    capture_events: list[dict[str, Any]],
    json_body: dict[str, Any],
) -> tuple[httpx.Response, dict[str, Any]]:
    capture_events.clear()
    resp = await client.post(
        "/federated/v1/chat/completions",
        headers=auth_header(USER_TOKEN),
        json=json_body,
    )
    await asyncio.sleep(0.05)
    inference = [r for r in capture_events if r["stream"] == "inference"]
    assert len(inference) == 1
    return resp, inference[0]


async def test_request_over_burst_tokens_rejected_and_logged(
    client: httpx.AsyncClient, capture_events: list[dict[str, Any]]
) -> None:
    _seed_streaming_config(usage_limits={"default": {"burst_tokens": 50}})
    await _inject_streaming_backend()

    resp, log = await _post_and_get_log(
        client, capture_events, {**_CHAT, "max_tokens": 100}
    )
    assert resp.status_code == 400
    assert "Retry-After" not in resp.headers
    error = resp.json()["error"]
    assert error["code"] == "request_too_large"
    assert "per-request limit of 50 tokens" in error["message"]

    assert log["outcome"] == "admission_rejected"
    assert log["error"] == "request_too_large"
    assert log["estimated_tokens"] == 101
    assert log["deployment"] is None and log["backend_id"] is None


async def test_tpm_rejection_logged_with_rounded_up_retry_after(
    client: httpx.AsyncClient, capture_events: list[dict[str, Any]]
) -> None:
    # 60 tpm = 1 token/sec into a 150-token bucket.
    _seed_streaming_config(usage_limits={"default": {"tpm": 60, "burst_tokens": 150}})
    usage = {"prompt_tokens": 1, "completion_tokens": 139, "total_tokens": 140}
    await _inject_backend(backend.respond(json={"choices": [], "usage": usage}))
    request = {**_CHAT, "max_tokens": 100}

    first, log = await _post_and_get_log(client, capture_events, request)
    assert first.status_code == 200
    assert log["outcome"] == "success"
    assert log["input_images"] == 0

    # The first request settles at 140 tokens; the second is estimated at
    # 1 + 100, so it must wait (140 + 101 - 150) = 91s less a few ms elapsed.
    second, log = await _post_and_get_log(client, capture_events, request)
    assert second.status_code == 429
    # Rounded up (never truncated to 90), plus at most 30% upward jitter.
    retry_after = int(second.headers["Retry-After"])
    assert 91 <= retry_after <= 119
    assert f"Retry in {retry_after} seconds." in second.json()["error"]["message"]
    assert log["outcome"] == "admission_rejected"
    assert log["error"] == "quota:user_tpm"
    assert log["attempt"] == 1
    assert log["estimated_tokens"] == 101


async def test_capacity_rejection_logged(
    client: httpx.AsyncClient, capture_events: list[dict[str, Any]]
) -> None:
    _seed_streaming_config(with_backend=False)
    resp, log = await _post_and_get_log(client, capture_events, _CHAT)
    assert resp.status_code == 503
    assert int(resp.headers["Retry-After"]) >= 15
    assert resp.json()["error"]["info"]["reject_reason"] == "capacity:no_candidates"
    assert log["outcome"] == "admission_rejected"
    assert log["error"] == "capacity:no_candidates"
    assert log["model"] == "open/model"
    assert log["cluster"] is None


async def test_stream_broken_by_backend_logged_as_upstream_error(
    client: httpx.AsyncClient, capture_events: list[dict[str, Any]]
) -> None:
    """A backend that dies after the 200 cuts the client's stream short (no
    clean end), and the attempt is logged as upstream_error, not success, and
    counted toward the backend's cooldown."""
    _seed_streaming_config()
    backend_id = "static_deployment/dep-open"
    redis = app.state.admission_controller.client
    await redis.delete(Keys.backend_errors(backend_id))

    await _inject_backend(backend.stream(_SSE_CHUNKS, break_after=1))

    relayed = b""
    with pytest.raises(Exception):
        async with client.stream(
            "POST",
            "/federated/v1/chat/completions",
            headers=auth_header(USER_TOKEN),
            json={**_CHAT, "stream": True},
        ) as resp:
            assert resp.status_code == 200
            async for chunk in resp.aiter_raw():
                relayed += chunk
    assert b"[DONE]" not in relayed
    await asyncio.sleep(0.05)

    (inf,) = [r for r in capture_events if r["stream"] == "inference"]
    assert inf["outcome"] == "upstream_error"
    assert inf["error"] == "ClientPayloadError"
    assert inf["total_tokens"] is None
    assert int(await redis.get(Keys.backend_errors(backend_id))) == 1
    await redis.delete(Keys.backend_errors(backend_id))


# --------------------------------------------------------------------------
# ResponseLog is the request's summary row
# --------------------------------------------------------------------------

_INFERENCE_SUMMARY_KEYS = (
    "endpoint",
    "model",
    "deployment",
    "cluster",
    "outcome",
    "attempt",
    "latency_sec",
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "cache_read_tokens",
    "cache_write_tokens",
    "reasoning_tokens",
)


def _only(events: list[dict[str, Any]], stream: str) -> dict[str, Any]:
    (row,) = [r for r in events if r["stream"] == stream]
    return row


def _assert_summarizes(response: dict[str, Any], inference: dict[str, Any]) -> None:
    """The ResponseLog repeats the final InferenceLog's fields."""
    for key in _INFERENCE_SUMMARY_KEYS:
        assert response[key] == inference[key], key


async def test_response_summary_without_auth(
    client: httpx.AsyncClient, capture_events: list[dict[str, Any]]
) -> None:
    resp = await client.get("/no/such/route", headers={"X-Real-IP": "192.0.2.5"})
    assert resp.status_code == 404
    await asyncio.sleep(0.05)

    row = _only(capture_events, "response")
    assert row["method"] == "GET"
    assert row["path"] == "/no/such/route"
    assert row["origin_ip"] == "192.0.2.5"
    assert row["auth_attempted"] is False
    assert row["user_id"] is None
    for key in _INFERENCE_SUMMARY_KEYS:
        assert row[key] is None, key


async def test_response_summary_non_inference(
    client: httpx.AsyncClient, capture_events: list[dict[str, Any]]
) -> None:
    resp = await client.get("/whoami", headers=auth_header(USER_TOKEN))
    assert resp.status_code == 200
    await asyncio.sleep(0.05)

    request, row = _only(capture_events, "request"), _only(capture_events, "response")
    assert row["request_id"] == request["request_id"]
    assert row["method"] == "GET"
    assert row["path"] == "/whoami"
    assert row["origin_ip"] == request["origin_ip"] == "127.0.0.1"
    assert row["auth_attempted"] is True
    assert row["user_id"] == request["user_id"]
    assert row["user_id"]
    for key in _INFERENCE_SUMMARY_KEYS:
        assert row[key] is None, key


async def test_response_summary_failed_auth(
    client: httpx.AsyncClient, capture_events: list[dict[str, Any]]
) -> None:
    resp = await client.post(
        "/federated/v1/chat/completions",
        headers=auth_header(INVALID_TOKEN),
        json=_CHAT,
    )
    assert resp.status_code == 401
    await asyncio.sleep(0.05)

    row = _only(capture_events, "response")
    assert row["status_code"] == 401
    assert row["method"] == "POST"
    assert row["path"] == "/federated/v1/chat/completions"
    assert row["auth_attempted"] is True
    assert row["user_id"] is None
    assert row["model"] is None and row["attempt"] is None


async def test_response_summary_unary_success(
    client: httpx.AsyncClient, capture_events: list[dict[str, Any]]
) -> None:
    _seed_streaming_config()
    usage = {"prompt_tokens": 2, "completion_tokens": 7, "total_tokens": 9}
    await _inject_backend(backend.respond(json={"choices": [], "usage": usage}))

    resp, inference = await _post_and_get_log(client, capture_events, _CHAT)
    assert resp.status_code == 200

    row = _only(capture_events, "response")
    assert row["request_id"] == inference["request_id"]
    assert row["status_code"] == 200
    assert row["streaming"] is False
    assert row["auth_attempted"] is True
    assert row["user_id"] == inference["user_id"]
    _assert_summarizes(row, inference)
    assert row["model"] == "open/model"
    assert row["cluster"] == "test-cluster"
    assert row["outcome"] == "success"
    assert row["attempt"] == 1
    assert (row["input_tokens"], row["output_tokens"], row["total_tokens"]) == (
        2,
        7,
        9,
    )


async def test_response_summary_retried_request(
    client: httpx.AsyncClient, capture_events: list[dict[str, Any]]
) -> None:
    """The first backend tried fails with a retryable 503 and the second
    succeeds: the summary carries the final (second) attempt."""
    backend_ids = ("static_deployment/dep-open", "static_deployment/dep-open-2")
    _seed_streaming_config(backend_ids=backend_ids)
    redis = app.state.admission_controller.client
    calls = 0
    usage = {"prompt_tokens": 4, "completion_tokens": 6, "total_tokens": 10}

    async def first_fails(request: web.Request) -> web.StreamResponse:
        nonlocal calls
        calls += 1
        if calls == 1:
            return web.Response(status=503, text="busy")
        return web.json_response({"choices": [], "usage": usage})

    for backend_id in backend_ids:
        app.state.backend_client_manager._clients[
            backend_id
        ] = await backend.serve_backend(first_fails)

    try:
        capture_events.clear()
        resp = await client.post(
            "/federated/v1/chat/completions",
            headers=auth_header(USER_TOKEN),
            json=_CHAT,
        )
        assert resp.status_code == 200
        await asyncio.sleep(0.05)
    finally:
        for backend_id in backend_ids:
            await redis.delete(Keys.backend_errors(backend_id))

    attempts = sorted(
        (r for r in capture_events if r["stream"] == "inference"),
        key=lambda r: r["attempt"],
    )
    assert [(a["attempt"], a["outcome"]) for a in attempts] == [
        (1, "upstream_error"),
        (2, "success"),
    ]
    row = _only(capture_events, "response")
    _assert_summarizes(row, attempts[1])
    assert row["attempt"] == 2
    assert row["outcome"] == "success"
    assert row["total_tokens"] == 10


async def test_response_summary_admission_rejected(
    client: httpx.AsyncClient, capture_events: list[dict[str, Any]]
) -> None:
    _seed_streaming_config(with_backend=False)
    resp, inference = await _post_and_get_log(client, capture_events, _CHAT)
    assert resp.status_code == 503

    row = _only(capture_events, "response")
    assert row["status_code"] == 503
    _assert_summarizes(row, inference)
    assert row["outcome"] == "admission_rejected"
    assert row["model"] == "open/model"
    assert row["cluster"] is None
    assert row["attempt"] == 1


async def test_response_summary_streaming_success(
    client: httpx.AsyncClient, capture_events: list[dict[str, Any]]
) -> None:
    """The streaming attempt is logged when its relay ends, which is before
    the middleware emits the ResponseLog."""
    _seed_streaming_config()
    await _inject_streaming_backend()

    async with client.stream(
        "POST",
        "/federated/v1/chat/completions",
        headers=auth_header(USER_TOKEN),
        json={**_CHAT, "stream": True},
    ) as resp:
        assert resp.status_code == 200
        await resp.aread()
    await asyncio.sleep(0.05)

    inference, row = (
        _only(capture_events, "inference"),
        _only(capture_events, "response"),
    )
    assert row["streaming"] is True
    _assert_summarizes(row, inference)
    assert row["outcome"] == "success"
    assert row["total_tokens"] == 8


async def test_response_summary_broken_stream(
    client: httpx.AsyncClient, capture_events: list[dict[str, Any]]
) -> None:
    """A stream the backend cuts short still summarizes its failed attempt."""
    _seed_streaming_config()
    await _inject_backend(backend.stream(_SSE_CHUNKS, break_after=1))
    redis = app.state.admission_controller.client

    try:
        with pytest.raises(Exception):
            async with client.stream(
                "POST",
                "/federated/v1/chat/completions",
                headers=auth_header(USER_TOKEN),
                json={**_CHAT, "stream": True},
            ) as resp:
                async for _ in resp.aiter_raw():
                    pass
        await asyncio.sleep(0.05)
    finally:
        await redis.delete(Keys.backend_errors("static_deployment/dep-open"))

    row = _only(capture_events, "response")
    assert row["status_code"] == 200
    assert row["outcome"] == "upstream_error"
    assert row["model"] == "open/model"


# --------------------------------------------------------------------------
# ResponseLogMiddleware status on a failure
# --------------------------------------------------------------------------


async def _run_middleware(fail_after_start: bool) -> None:
    from first_gateway.apiserver.log_middleware import ResponseLogMiddleware

    async def app(scope: Any, receive: Any, send: Any) -> None:
        if fail_after_start:
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [(b"content-type", b"text/event-stream")],
                }
            )
            await send(
                {
                    "type": "http.response.body",
                    "body": b"data: {}\n\n",
                    "more_body": True,
                }
            )
        raise RuntimeError("upstream broke")

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: Any) -> None:
        pass

    scope = {"type": "http", "path": "/federated/v1/chat/completions"}
    with pytest.raises(RuntimeError):
        await ResponseLogMiddleware(app)(scope, receive, send)


@pytest.mark.parametrize(
    ("fail_after_start", "status", "streaming"),
    [(True, 200, True), (False, 500, False)],
)
async def test_response_log_status_when_app_raises(
    capture_events: list[dict[str, Any]],
    fail_after_start: bool,
    status: int,
    streaming: bool,
) -> None:
    """A stream that breaks after its 200 went out is logged as the 200 the
    client received; an exception before any response is logged as 500."""
    await _run_middleware(fail_after_start)
    (row,) = [r for r in capture_events if "status_code" in r]
    assert row["status_code"] == status
    assert row["streaming"] is streaming


# --------------------------------------------------------------------------
# A long stream that yields no usage is logged
# --------------------------------------------------------------------------

_NO_USAGE_CHUNKS = [
    b'data: {"choices":[{"delta":{"content":"hi"}}]}\n\n',
    b'data: {"choices":[{"delta":{"content":" there"}}]}\n\n',
    b"data: [DONE]\n\n",
]


@pytest.fixture
def no_usage_log(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> Callable[[], list[logging.LogRecord]]:
    """Treat any stream as long; return the 'No token usage' records."""
    # The apiserver lifespan stops first_gateway propagating to caplog's root.
    monkeypatch.setattr(logging.getLogger("first_gateway"), "propagate", True)
    monkeypatch.setattr(inference, "NO_USAGE_LOG_BYTES", 1)
    caplog.set_level(logging.WARNING, logger="first_gateway.apiserver.inference")
    return lambda: [r for r in caplog.records if "No token usage" in r.message]


async def _stream_chat(client: httpx.AsyncClient) -> bytes:
    async with client.stream(
        "POST",
        "/federated/v1/chat/completions",
        headers=auth_header(USER_TOKEN),
        json={**_CHAT, "stream": True},
    ) as resp:
        assert resp.status_code == 200
        relayed = b"".join([chunk async for chunk in resp.aiter_raw()])
    await asyncio.sleep(0.05)  # let the relay's finally block run
    return relayed


async def test_completed_stream_without_usage_logs_error(
    client: httpx.AsyncClient,
    capture_events: list[dict[str, Any]],
    no_usage_log: Callable[[], list[logging.LogRecord]],
) -> None:
    """A backend that finishes a long stream with no usage leaves the request
    charged nothing: an error, so it shows up if it starts happening."""
    _seed_streaming_config()
    await _inject_backend(backend.stream(_NO_USAGE_CHUNKS))
    assert b"[DONE]" in await _stream_chat(client)

    (record,) = no_usage_log()
    assert record.levelno == logging.ERROR
    assert "chat/completions stream" in record.message
    assert "static_deployment/dep-open" in record.message
    assert "(completed)" in record.message
    assert "usage scan took" in record.message


async def test_stream_with_usage_not_logged(
    client: httpx.AsyncClient,
    capture_events: list[dict[str, Any]],
    no_usage_log: Callable[[], list[logging.LogRecord]],
) -> None:
    _seed_streaming_config()
    await _inject_streaming_backend()
    await _stream_chat(client)
    assert no_usage_log() == []


async def test_short_stream_without_usage_not_logged(
    client: httpx.AsyncClient,
    capture_events: list[dict[str, Any]],
    no_usage_log: Callable[[], list[logging.LogRecord]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(inference, "NO_USAGE_LOG_BYTES", 1 << 20)
    _seed_streaming_config()
    await _inject_backend(backend.stream(_NO_USAGE_CHUNKS))
    await _stream_chat(client)
    assert no_usage_log() == []


async def test_broken_stream_without_usage_logs_error(
    client: httpx.AsyncClient,
    capture_events: list[dict[str, Any]],
    no_usage_log: Callable[[], list[logging.LogRecord]],
) -> None:
    _seed_streaming_config()
    await _inject_backend(backend.stream(_SSE_CHUNKS, break_after=1))
    with pytest.raises(Exception):
        await _stream_chat(client)
    await asyncio.sleep(0.05)

    (record,) = no_usage_log()
    assert record.levelno == logging.ERROR
    assert "(broken: ClientPayloadError)" in record.message


async def test_client_disconnect_without_usage_logs_warning(
    client: httpx.AsyncClient,
    capture_events: list[dict[str, Any]],
    no_usage_log: Callable[[], list[logging.LogRecord]],
) -> None:
    """A client that hangs up mid-stream (Stop in a chat UI) is routine: a
    warning, not an error. httpx's ASGITransport buffers whole responses, so
    drive the app directly and send http.disconnect after the first chunk."""
    _seed_streaming_config()
    # aiohttp does not cancel a handler whose client left; release it at the end.
    release_backend = asyncio.Event()

    async def first_chunk_then_hang(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        await response.write(_NO_USAGE_CHUNKS[0])
        await release_backend.wait()  # the rest of a long generation
        return response

    await _inject_backend(first_chunk_then_hang)

    transport = client._transport
    assert isinstance(transport, httpx.ASGITransport)
    body = json.dumps({**_CHAT, "stream": True}).encode()
    first_chunk = asyncio.Event()
    sent: list[MutableMapping[str, Any]] = []
    body_sent = False

    async def receive() -> dict[str, Any]:
        nonlocal body_sent
        if not body_sent:
            body_sent = True
            return {"type": "http.request", "body": body, "more_body": False}
        # Must block: Starlette polls receive() for a disconnect in a loop.
        await first_chunk.wait()
        return {"type": "http.disconnect"}

    async def send(message: MutableMapping[str, Any]) -> None:
        sent.append(message)
        if message["type"] == "http.response.body" and message.get("body"):
            first_chunk.set()

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/federated/v1/chat/completions",
        "raw_path": b"/federated/v1/chat/completions",
        "query_string": b"",
        "root_path": "",
        "headers": [
            (b"host", b"testserver"),
            (b"content-type", b"application/json"),
            (b"content-length", str(len(body)).encode()),
            *[
                (k.lower().encode(), v.encode())
                for k, v in auth_header(USER_TOKEN).items()
            ],
        ],
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
    }
    try:
        await asyncio.wait_for(transport.app(scope, receive, send), timeout=10)
    finally:
        release_backend.set()
    await asyncio.sleep(0.05)

    assert sent[0]["status"] == 200
    (record,) = no_usage_log()
    assert record.levelno == logging.WARNING
    assert "(client disconnected)" in record.message
