import asyncio
import json
import logging
from pathlib import Path
from typing import Any, Iterator

import httpx
import pytest

from first_common.schema.structured_logs import (
    INLINE_BODY_LIMIT,
    InferenceLog,
    RequestLog,
    ResponseLog,
    _background_tasks,
)
from first_gateway.apiserver.api import app
from first_gateway.apiserver.context import _request_id_ctx, get_request_id
from first_gateway.database.redis.router_config import (
    BackendConfig,
    DeploymentConfig,
    ModelConfig,
    RouterConfig,
)
from first_gateway.log_config import RequestIdFilter

from .fixtures.auth import USER_TOKEN, auth_header

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


async def test_large_response_body_uses_responses_dir(tmp_path: Path) -> None:
    log = ResponseLog(
        request_id="rid-resp",
        status_code=500,
        duration_ms=1.5,
        streaming=False,
    )
    raw = b"e" * (INLINE_BODY_LIMIT + 1)
    log.emit(raw_body=raw, storage_dir=tmp_path)
    await asyncio.gather(*list(_background_tasks))

    files = list(tmp_path.rglob("*.json"))
    assert len(files) == 1
    assert files[0].parent.parent.parent.parent.name == "large-responses"


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


def _seed_streaming_config() -> None:
    cfg = RouterConfig(
        version=1,
        models=[
            ModelConfig(
                name="open/model",
                aliases=[],
                allowed_groups=[],
                allowed_domains=[],
                supported_endpoints=["chat/completions"],
                usage_limits={},  # type: ignore[arg-type]
                overload={},  # type: ignore[arg-type]
                deployments=[
                    DeploymentConfig(
                        kind="static",
                        name="dep-open",
                        cluster_name="test-cluster",
                        router_params={},  # type: ignore[arg-type]
                        prometheus_metrics_path=None,
                        prometheus_scrape_interval_sec=30,
                        backends=[
                            BackendConfig(
                                id="static_deployment/dep-open",
                                model_url="http://backend.invalid/v1",
                                backend_model_name="upstream",
                                api_key=None,
                            )
                        ],
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


def _inject_streaming_backend() -> None:
    """Swap in a MockTransport httpx client that streams SSE chunks."""

    async def handler(request: httpx.Request) -> httpx.Response:
        async def stream() -> Any:
            for chunk in _SSE_CHUNKS:
                yield chunk

        return httpx.Response(200, content=stream())

    transport = httpx.MockTransport(handler)
    mock_client = httpx.AsyncClient(
        transport=transport, base_url="http://backend.invalid/v1"
    )
    app.state.backend_client_manager._clients["static_deployment/dep-open"] = (
        mock_client
    )


async def test_three_events_share_request_id_streaming(
    client: httpx.AsyncClient, capture_events: list[dict[str, Any]]
) -> None:
    _seed_streaming_config()
    _inject_streaming_backend()

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
