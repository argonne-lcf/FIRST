from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

from django.test import RequestFactory, SimpleTestCase

from resource_server_async.auth import get_access_token_header
from resource_server_async.endpoints.direct_api import DirectAPIEndpoint
from resource_server_async.endpoints.first_v2 import (
    FirstV2Endpoint,
    FirstV2EndpointConfig,
)
from resource_server_async.errors import EndpointError
from resource_server_async.services import _adapt_mistral_developer_role


class MistralDeveloperRoleTests(SimpleTestCase):
    def test_chat_completions_maps_developer_role_to_system(self) -> None:
        payload = {
            "messages": [
                {"role": "developer", "content": "Be brief."},
                {"role": "user", "content": "Say hi."},
            ]
        }

        _adapt_mistral_developer_role(payload, "chat/completions")

        self.assertEqual(payload["messages"][0]["role"], "system")
        self.assertEqual(payload["messages"][1]["role"], "user")

    def test_responses_maps_developer_role_without_changing_other_parts(self) -> None:
        payload = {
            "input": [
                {"role": "developer", "content": "Be brief."},
                {"role": "user", "content": [{"type": "input_text", "text": "hi"}]},
            ]
        }

        _adapt_mistral_developer_role(payload, "responses")

        self.assertEqual(payload["input"][0]["role"], "system")
        self.assertEqual(
            payload["input"][1]["content"], [{"type": "input_text", "text": "hi"}]
        )


class ResponsesPassthroughTests(SimpleTestCase):
    def test_multimodal_input_and_format_are_forwarded_unchanged(self) -> None:
        payload = {
            "model": "mistral-large-4-nvfp4",
            "input": [
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": "Say hi."},
                        {"type": "input_image", "image_url": "data:image/png;base64,AA=="},
                    ],
                }
            ],
            "text": {"format": {"type": "json_object"}},
        }
        endpoint = object.__new__(DirectAPIEndpoint)

        outbound = endpoint._prepare_request_body(
            {"model_params": payload}, stream=False
        )

        self.assertEqual(
            {key: value for key, value in outbound.items() if key != "stream"}, payload
        )
        self.assertFalse(outbound["stream"])


class MessagesApiKeyTests(SimpleTestCase):
    def setUp(self) -> None:
        self.factory = RequestFactory()

    def test_x_api_key_is_accepted_only_on_messages_route(self) -> None:
        messages = self.factory.post(
            "/resource_server/minerva/vllm/v1/messages", HTTP_X_API_KEY="globus-token"
        )
        chat = self.factory.post(
            "/resource_server/minerva/vllm/v1/chat/completions",
            HTTP_X_API_KEY="globus-token",
        )

        self.assertEqual(get_access_token_header(messages), "Bearer globus-token")
        self.assertIsNone(get_access_token_header(chat))

    def test_authorization_header_takes_precedence(self) -> None:
        request = self.factory.post(
            "/resource_server/minerva/vllm/v1/messages",
            HTTP_AUTHORIZATION="Bearer authorization-token",
            HTTP_X_API_KEY="api-key-token",
        )

        self.assertEqual(
            get_access_token_header(request), "Bearer authorization-token"
        )


class StreamingUpstreamErrorTests(SimpleTestCase):
    async def test_first_upstream_error_propagates_http_status_before_streaming(self) -> None:
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
