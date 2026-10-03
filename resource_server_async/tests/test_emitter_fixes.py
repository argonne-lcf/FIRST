import json
import logging
import time
from datetime import datetime, timezone
from io import StringIO
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from django.http import HttpResponse
from django.test import SimpleTestCase

from inference_gateway.log_config import GatewayJsonFormatter
from resource_server_async.endpoints import BaseEndpoint
from resource_server_async.endpoints.direct_api import DirectAPIEndpoint
from resource_server_async.logging import (
    RequestContext,
    _request_context,
    write_logs,
)
from resource_server_async.schemas.structured_logs import (
    AccessLogPydantic,
    RequestLogPydantic,
)
from resource_server_async.streaming import update_streaming_log_async

ACCESS_ID = "access-a"
REQUEST_ID = "request-a"


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


class DirectAPIStreamingMetricsTests(SimpleTestCase):
    async def test_streaming_update_awaits_emit_metrics(self) -> None:
        endpoint = object.__new__(DirectAPIEndpoint)
        setattr(endpoint, "_BaseEndpoint__endpoint_slug", "direct-test")
        request_log = Mock(emit_metrics=AsyncMock())
        context = make_context()
        context.request_log = request_log
        streaming_state = {
            "chunks": ["data: x"],
            "total_chunks": 1,
            "completed": True,
            "error": None,
            "start_time": time.time(),
        }

        update_streaming_log = getattr(
            endpoint, "_DirectAPIEndpoint__update_streaming_log"
        )
        with self.assertLogs(
            "resource_server_async.endpoints.direct_api", level="INFO"
        ):
            await update_streaming_log(context, streaming_state)

        request_log.emit_metrics.assert_awaited_once()
