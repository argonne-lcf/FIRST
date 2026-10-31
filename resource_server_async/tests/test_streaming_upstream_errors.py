from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

from django.test import SimpleTestCase

from resource_server_async.endpoints.direct_api import DirectAPIEndpoint
from resource_server_async.endpoints.first_v2 import FirstV2Endpoint, FirstV2EndpointConfig
from resource_server_async.errors import EndpointError


class StreamingUpstreamErrorTests(SimpleTestCase):
    async def test_first_upstream_error_propagates_http_status_before_streaming(
        self,
    ) -> None:
        endpoint = object.__new__(DirectAPIEndpoint)
        config = type("Config", (), {"api_url": "https://api.example/v1"})()
        setattr(endpoint, "_DirectAPIEndpoint__config", config)
        setattr(
            endpoint,
            "_DirectAPIEndpoint__httpx_client",
            SimpleNamespace(headers={"Content-Type": "application/json"}),
        )
        setattr(endpoint, "_BaseEndpoint__endpoint_slug", "direct-test")
        setattr(endpoint, "_BaseEndpoint__model", "model")

        async def failed_stream(*args: object, **kwargs: object):
            raise EndpointError("Upstream inference request failed.", status_code=400)
            yield ""  # Make this an async generator.

        setattr(endpoint, "_DirectAPIEndpoint__get_stream_chunks", failed_stream)

        with self.assertRaises(EndpointError) as raised:
            await endpoint._submit_streaming_task_with_headers(
                {"openai_endpoint": "responses", "model": "model"}
            )

        self.assertEqual(raised.exception.status_code, 400)

    async def test_first_v2_upstream_error_keeps_http_status(self) -> None:
        endpoint = object.__new__(FirstV2Endpoint)
        endpoint._cfg = FirstV2EndpointConfig(
            model_urls=["https://v2.example"], backend_model_name="backend-model"
        )
        setattr(endpoint, "_BaseEndpoint__model", "model")
        upstream_response = SimpleNamespace(
            status_code=400,
            aread=AsyncMock(return_value=b"internal host details"),
        )

        @asynccontextmanager
        async def fake_stream(*args: object, **kwargs: object):
            yield upstream_response

        endpoint._client = SimpleNamespace(stream=fake_stream)

        with self.assertRaises(EndpointError) as raised:
            await endpoint.submit_streaming_task(
                {"model_params": {"model": "model", "openai_endpoint": "responses"}}
            )

        self.assertEqual(raised.exception.status_code, 400)
        self.assertNotIn("internal host", str(raised.exception))

    def test_sse_errors_are_not_answer_text_or_internal_hostnames(self) -> None:
        for api in ("chat/completions", "responses", "messages"):
            with self.subTest(api=api):
                chunk = DirectAPIEndpoint._sse_error_chunk(
                    "model",
                    api,
                    "The upstream inference request failed while streaming.",
                )
                self.assertNotIn("chatcmpl-api-error", chunk)
                self.assertNotIn("[ERROR]", chunk)
                self.assertNotIn("minerva-login-01", chunk)
