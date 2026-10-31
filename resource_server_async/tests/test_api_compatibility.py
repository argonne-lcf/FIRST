from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from django.http import StreamingHttpResponse
from django.test import RequestFactory, SimpleTestCase, TestCase
from django.urls import resolve

from resource_server_async.auth import get_access_token_header
from resource_server_async.endpoints.direct_api import (
    DirectAPIEndpoint,
    DirectAPIEndpointConfig,
)
from resource_server_async.endpoints.first_v2 import (
    FirstV2Endpoint,
    FirstV2EndpointConfig,
)
from resource_server_async.errors import EndpointError, ResponseNotFound
from resource_server_async.models import StoredResponse
from resource_server_async.services import (
    _adapt_mistral_developer_role,
    _prepare_openai_request,
    _prepare_responses_continuation,
    _store_responses_result,
    _store_streamed_responses_result,
    delete_stored_response,
)


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
                        {
                            "type": "input_image",
                            "image_url": "data:image/png;base64,AA==",
                        },
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


class TokenCountRouteTests(SimpleTestCase):
    def test_token_count_routes_are_registered(self) -> None:
        for path in (
            "/resource_server/minerva/vllm/v1/messages/count_tokens",
            "/resource_server/minerva/vllm/v1/responses/input_tokens",
            "/resource_server/minerva/vllm/v1/responses/resp_test",
        ):
            with self.subTest(path=path):
                self.assertIsNotNone(resolve(path))
        self.assertIsNotNone(
            resolve("/resource_server/minerva/vllm/v1/responses/resp_test")
        )

    def test_token_count_payloads_use_the_existing_request_controls(self) -> None:
        cases = (
            (
                "messages/count_tokens",
                {"model": "model", "messages": [{"role": "user", "content": "hi"}]},
                [{"role": "user", "content": "hi"}],
            ),
            (
                "responses/input_tokens",
                {"model": "model", "input": "hi"},
                "hi",
            ),
        )
        for route, payload, prompt in cases:
            with self.subTest(route=route):
                _control, outbound, actual_prompt = _prepare_openai_request(
                    payload, route
                )
                self.assertEqual(actual_prompt, prompt)
                self.assertEqual(outbound["openai_endpoint"], route)

    async def test_token_count_route_is_forwarded_to_model_server(self) -> None:
        endpoint = object.__new__(DirectAPIEndpoint)
        setattr(
            endpoint,
            "_DirectAPIEndpoint__config",
            DirectAPIEndpointConfig(
                api_url="https://model.example/v1", api_key_env_name="MISSING"
            ),
        )
        post = AsyncMock(return_value={"input_tokens": 1})
        setattr(
            endpoint,
            "_DirectAPIEndpoint__httpx_client",
            SimpleNamespace(headers={}, post=post),
        )

        await endpoint._submit_task_with_headers(
            {
                "openai_endpoint": "responses/input_tokens",
                "model": "model",
                "input": "hi",
            }
        )

        post.assert_awaited_once_with(
            "https://model.example/v1/responses/input_tokens",
            data={"model": "model", "input": "hi"},
            headers=None,
        )


class StoredResponsesTests(TestCase):
    async def test_store_and_continue_responses_for_the_same_user(self) -> None:
        initial_input = "Remember 42."
        result = await _store_responses_result(
            {
                "id": "resp_test",
                "output": [
                    {
                        "type": "message",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": "I will."}],
                    }
                ],
            },
            store=True,
            user_id="user-a",
            cluster="minerva",
            framework="vllm",
            model="model-a",
            history=[],
            request_input=[
                {
                    "role": "user",
                    "content": [{"type": "input_text", "text": initial_input}],
                }
            ],
        )
        payload = {"input": "Which number?", "previous_response_id": "resp_test"}

        history, current_input = await _prepare_responses_continuation(
            "user-a", "minerva", "vllm", "model-a", payload
        )

        self.assertEqual(result["id"], "resp_test")
        self.assertEqual(await StoredResponse.objects.acount(), 1)
        self.assertNotIn("previous_response_id", payload)
        self.assertEqual(payload["input"], [*history, *current_input])
        self.assertEqual(payload["input"][0]["content"][0]["text"], initial_input)
        self.assertEqual(payload["input"][-1]["content"][0]["text"], "Which number?")

    async def test_streamed_completed_response_is_stored(self) -> None:
        event = (
            b"event: response.completed\ndata: "
            b'{"type":"response.completed","response":{"id":"resp_stream",'
            b'"output":[]}}\n\n'
        )

        async def stream():
            yield event[:30]
            yield event[30:]

        response = StreamingHttpResponse(stream(), content_type="text/event-stream")
        wrapped = _store_streamed_responses_result(
            response,
            user_id="user-a",
            cluster="minerva",
            framework="vllm",
            model="model-a",
            history=[],
            request_input=[{"role": "user", "content": "hello"}],
        )

        sent = [chunk async for chunk in wrapped]

        self.assertEqual(b"".join(sent), event)
        self.assertTrue(
            await StoredResponse.objects.filter(response_id="resp_stream").aexists()
        )

    async def test_stored_responses_are_private_to_their_owner(self) -> None:
        await StoredResponse.objects.acreate(
            response_id="resp_private",
            user_id="owner",
            cluster="minerva",
            framework="vllm",
            model="model-a",
            response={"id": "resp_private"},
            conversation=[],
        )

        with self.assertRaises(ResponseNotFound):
            await _prepare_responses_continuation(
                "other-user",
                "minerva",
                "vllm",
                "model-a",
                {"input": "hello", "previous_response_id": "resp_private"},
            )

    async def test_owner_can_delete_a_stored_response(self) -> None:
        await StoredResponse.objects.acreate(
            response_id="resp_delete",
            user_id="user-a",
            cluster="minerva",
            framework="vllm",
            model="model-a",
            response={"id": "resp_delete"},
            conversation=[],
        )
        cluster = SimpleNamespace(frameworks=["vllm"], check_permission=Mock())
        endpoint = SimpleNamespace(check_permission=Mock())

        with (
            patch(
                "resource_server_async.services.BaseCluster.load_adapter",
                new=AsyncMock(return_value=cluster),
            ),
            patch(
                "resource_server_async.services.BaseEndpoint.load_adapter",
                new=AsyncMock(return_value=endpoint),
            ),
        ):
            result = await delete_stored_response(
                SimpleNamespace(id="user-a"), "minerva", "vllm", "resp_delete"
            )

        self.assertEqual(result["deleted"], True)
        self.assertFalse(
            await StoredResponse.objects.filter(response_id="resp_delete").aexists()
        )


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

        self.assertEqual(get_access_token_header(request), "Bearer authorization-token")


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
