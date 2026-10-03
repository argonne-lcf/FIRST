import json
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from io import StringIO
from types import SimpleNamespace
from typing import AsyncGenerator
from unittest.mock import AsyncMock, Mock, patch

from django.http import HttpResponse
from django.test import SimpleTestCase

from inference_gateway.log_config import GatewayJsonFormatter
from resource_server_async.endpoints import BaseEndpoint, first_v2
from resource_server_async.endpoints.direct_api import (
    DirectAPIEndpoint,
    StreamingState,
)
from resource_server_async.endpoints.first_v2 import (
    FirstV2Endpoint,
    FirstV2EndpointConfig,
)
from resource_server_async.logging import (
    RequestContext,
    _request_context,
    write_logs,
)
from resource_server_async.schemas.structured_logs import (
    AccessLogPydantic,
    RequestLogPydantic,
    UsageTokens,
)
from resource_server_async.streaming import update_streaming_log_async

ACCESS_ID = "access-a"
REQUEST_ID = "request-a"

CONTENT_BODY = '{"choices": [{"delta": {"content": "hi"}}]}'
USAGE_BODY = '{"usage":{"prompt_tokens":11,"completion_tokens":22,"total_tokens":33}}'
USAGE_TOKENS = UsageTokens(prompt_tokens=11, completion_tokens=22, total_tokens=33)

CONTENT_CHUNK = f"data: {CONTENT_BODY}\n\n"
USAGE_CHUNK = f"data: {USAGE_BODY}\n\n"


def make_context() -> RequestContext:
    return RequestContext(
        access_log=AccessLogPydantic(
            id=ACCESS_ID,
            timestamp_request=datetime(2026, 8, 7, tzinfo=timezone.utc),
            api_route="/cluster/framework/v1/chat/completions",
            origin_ip="127.0.0.1",
        )
    )


def make_request_log() -> RequestLogPydantic:
    return RequestLogPydantic(
        id=REQUEST_ID,
        access_log_id=ACCESS_ID,
        user_id="user-a",
        cluster="cluster",
        framework="framework",
        model="model",
        openai_endpoint="chat/completions",
        prompt="hello",
        timestamp_compute_request=datetime(2026, 8, 7, tzinfo=timezone.utc),
    )


def make_streaming_state() -> StreamingState:
    return {
        "chunks": [],
        "total_chunks": 0,
        "completed": False,
        "error": None,
        "start_time": time.time(),
        "usage": None,
    }


class RequestLogStatusGuardTests(SimpleTestCase):
    def test_known_status_survives_none_re_emit(self) -> None:
        request_log = make_request_log()

        request_log.emit('{"first": true}', 200)
        request_log.emit('{"second": true}', None)

        self.assertEqual(request_log.status_code, 200)


class WriteLogsAccessIdTests(SimpleTestCase):
    async def test_access_id_present_after_middleware_context_reset(self) -> None:
        context = make_context()
        token = _request_context.set(context)
        _request_context.reset(token)

        stream = StringIO()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(GatewayJsonFormatter())
        access_logger = logging.getLogger("resource_server_async.structured.access_log")
        previous_level = access_logger.level
        access_logger.setLevel(logging.INFO)
        access_logger.addHandler(handler)
        try:
            await write_logs(context, HttpResponse(b"ok"))
        finally:
            access_logger.removeHandler(handler)
            access_logger.setLevel(previous_level)

        line = json.loads(stream.getvalue().strip())
        self.assertEqual(line["stream"], "access_log")
        self.assertEqual(line["access_id"], ACCESS_ID)


class StreamingMetricsUsageTests(SimpleTestCase):
    async def test_streaming_metrics_carry_prompt_and_completion(self) -> None:
        context = make_context()
        context.request_log = make_request_log()
        adapter = SimpleNamespace(record_token_usage=Mock())
        complete_response = {
            "usage": {
                "prompt_tokens": 11,
                "completion_tokens": 22,
                "total_tokens": 33,
            }
        }

        with (
            patch.object(
                BaseEndpoint, "load_adapter", new=AsyncMock(return_value=adapter)
            ),
            self.assertLogs(
                "resource_server_async.structured.request_metrics", level="INFO"
            ) as logs,
        ):
            await update_streaming_log_async(
                context, {"final_status": "completed"}, complete_response
            )

        metrics = logs.records[-1].__dict__
        self.assertEqual(metrics["prompt_tokens"], 11)
        self.assertEqual(metrics["completion_tokens"], 22)
        self.assertEqual(metrics["total_tokens"], 33)
        adapter.record_token_usage.assert_called_once_with("user-a", 33)


class DirectAPIStreamingChunkCollectionTests(SimpleTestCase):
    def test_usage_chunk_sets_usage_and_appends_body(self) -> None:
        state = make_streaming_state()

        DirectAPIEndpoint._collect_streaming_chunk(state, USAGE_CHUNK)

        self.assertEqual(state["usage"], USAGE_TOKENS)
        self.assertEqual(state["chunks"], [USAGE_BODY])

    def test_done_and_non_data_chunks_leave_state_untouched(self) -> None:
        state = make_streaming_state()
        untouched = {**state, "chunks": list(state["chunks"])}

        DirectAPIEndpoint._collect_streaming_chunk(state, "data: [DONE]\n\n")
        DirectAPIEndpoint._collect_streaming_chunk(state, ": keep-alive\n\n")

        self.assertEqual(state, untouched)


class DirectAPIStreamingMetricsTests(SimpleTestCase):
    async def test_streaming_update_awaits_emit_metrics(self) -> None:
        endpoint = object.__new__(DirectAPIEndpoint)
        setattr(endpoint, "_BaseEndpoint__endpoint_slug", "direct-test")
        request_log = Mock(emit_metrics=AsyncMock())
        context = make_context()
        context.request_log = request_log
        streaming_state = make_streaming_state()
        streaming_state["chunks"] = ["data: x"]
        streaming_state["completed"] = True
        streaming_state["usage"] = USAGE_TOKENS

        update_streaming_log = getattr(endpoint, "_update_streaming_log")
        with self.assertLogs(
            "resource_server_async.endpoints.direct_api", level="INFO"
        ):
            await update_streaming_log(context, streaming_state)

        request_log.emit_metrics.assert_awaited_once_with(USAGE_TOKENS)


class FirstV2StreamingFinalLogTests(SimpleTestCase):
    async def test_streaming_emits_final_log_and_metrics(self) -> None:
        chunks = [CONTENT_CHUNK, USAGE_CHUNK, "data: [DONE]\n\n"]

        @asynccontextmanager
        async def fake_stream(*args: object, **kwargs: object):
            """Stand-in for httpx's stream(): status 200 plus the given chunks."""

            async def aiter_text() -> AsyncGenerator[str, None]:
                for chunk in chunks:
                    yield chunk

            yield SimpleNamespace(status_code=200, aiter_text=aiter_text)

        endpoint = object.__new__(FirstV2Endpoint)
        endpoint._cfg = FirstV2EndpointConfig(
            model_urls=["https://v2.example"], backend_model_name="backend-model"
        )
        setattr(endpoint, "_BaseEndpoint__model", "model")
        setattr(endpoint, "_BaseEndpoint__endpoint_slug", "v2-test")
        setattr(endpoint, "_client", SimpleNamespace(stream=fake_stream))
        request_log = Mock(emit_metrics=AsyncMock())
        context = make_context()
        context.request_log = request_log
        create_task = Mock()
        with (
            patch.object(first_v2, "get_request_context", return_value=context),
            patch.object(first_v2.asyncio, "create_task", create_task),
        ):
            result = await endpoint.submit_streaming_task(
                {"model_params": {"model": "model"}}
            )

        streamed = [chunk async for chunk in result.response]

        create_task.assert_called_once()
        final_log = create_task.call_args.args[0]
        # The final-log coroutine is created with the shared streaming state.
        streaming_state = final_log.cr_frame.f_locals["streaming_state"]
        await final_log

        self.assertEqual(streamed, [chunk.encode() for chunk in chunks])
        self.assertTrue(streaming_state["completed"])
        request_log.emit.assert_called_once_with(
            f"{CONTENT_BODY}\n{USAGE_BODY}", status_code=None
        )
        request_log.emit_metrics.assert_awaited_once_with(USAGE_TOKENS)
