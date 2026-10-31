from types import SimpleNamespace
from unittest.mock import AsyncMock

from django.test import SimpleTestCase
from django.urls import resolve

from resource_server_async.endpoints.direct_api import (
    DirectAPIEndpoint,
    DirectAPIEndpointConfig,
)
from resource_server_async.services import _prepare_openai_request


class TokenCountRouteTests(SimpleTestCase):
    def test_token_count_routes_are_registered(self) -> None:
        for path in (
            "/resource_server/minerva/vllm/v1/messages/count_tokens",
            "/resource_server/minerva/vllm/v1/responses/input_tokens",
        ):
            with self.subTest(path=path):
                self.assertIsNotNone(resolve(path))

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
