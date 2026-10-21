import json
from typing import Any
from unittest.mock import patch

import httpx
from django.test import SimpleTestCase

from resource_server_async.endpoints import first_v2
from resource_server_async.endpoints.endpoint import BaseEndpoint
from resource_server_async.endpoints.first_v2 import FirstV2Endpoint
from resource_server_async.errors import EndpointError
from resource_server_async.schemas.structured_logs import UserPydantic


class FirstV2ClientTests(SimpleTestCase):
    def make_endpoint(self, **config: Any) -> FirstV2Endpoint:
        with patch.object(BaseEndpoint, "build_token_limiter", return_value=None):
            return FirstV2Endpoint(
                id="test-endpoint",
                endpoint_slug="tara-vllm-model",
                cluster="tara",
                framework="vllm",
                model="model",
                endpoint_adapter="resource_server_async.endpoints.first_v2.FirstV2Endpoint",
                tpm_model=1_000_000,
                tpm_user=1_000_000,
                config={
                    "model_urls": ["https://pilot.example"],
                    "backend_model_name": "backend-model",
                    **config,
                },
                allowed_globus_groups=["restricted-model-group"],
                allowed_domains=["anl.gov"],
            )

    async def test_actual_unary_and_streaming_requests_use_bounded_long_read(
        self,
    ) -> None:
        requests: list[httpx.Request] = []

        def handle(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if json.loads(request.content)["stream"]:
                return httpx.Response(
                    200,
                    text='data: {"usage":{"prompt_tokens":11,"completion_tokens":2,"total_tokens":13}}\n\ndata: [DONE]\n\n',
                    headers={"Content-Type": "text/event-stream"},
                )
            return httpx.Response(200, json={"answer": "complete"})

        # Exercise real HTTPX request construction without network access.
        real_client = httpx.AsyncClient
        with patch.object(
            first_v2.httpx,
            "AsyncClient",
            side_effect=lambda **kwargs: real_client(
                transport=httpx.MockTransport(handle), **kwargs
            ),
        ):
            endpoint = self.make_endpoint()
        try:
            result = await endpoint.submit_task({"model_params": {"model": "alias"}})
            self.assertEqual(result.result, {"answer": "complete"})
            with patch.object(first_v2, "get_request_context", side_effect=LookupError):
                streamed = await endpoint.submit_streaming_task(
                    {"model_params": {"model": "alias"}}
                )
                chunks = [chunk async for chunk in streamed.response]
            self.assertIn(b"data: [DONE]", b"".join(chunks))
            self.assertEqual(len(requests), 2)
            for request in requests:
                self.assertEqual(
                    request.extensions["timeout"],
                    {"connect": 5.0, "read": 900, "write": 60.0, "pool": 5.0},
                )
                self.assertNotIn("Authorization", request.headers)
                self.assertEqual(json.loads(request.content)["model"], "backend-model")
            self.assertEqual(
                json.loads(requests[1].content)["stream_options"],
                {"include_usage": True},
            )
        finally:
            await endpoint._client.aclose()

    async def test_tls_proxy_and_permission_configuration_is_preserved(self) -> None:
        with (
            patch.object(first_v2, "create_ssl_context", return_value=True) as tls,
            patch.object(first_v2.httpx, "AsyncClient") as client,
        ):
            endpoint = self.make_endpoint(
                ca_cert_path="/certs/ca.crt",
                client_cert_path="/certs/client.crt",
                client_key_path="/certs/client.key",
                proxy_url="socks5://proxy.example:1080",
                check_hostname=True,
            )
        tls.assert_called_once_with(
            ca_cert_path="/certs/ca.crt",
            client_cert_path="/certs/client.crt",
            client_key_path="/certs/client.key",
            check_hostname=True,
        )
        self.assertIs(client.call_args.kwargs["verify"], True)
        self.assertEqual(
            client.call_args.kwargs["proxy"], "socks5://proxy.example:1080"
        )
        self.assertIs(client.call_args.kwargs["trust_env"], False)
        self.assertEqual(endpoint.allowed_globus_groups, ["restricted-model-group"])
        self.assertEqual(endpoint.allowed_domains, ["anl.gov"])
        user = UserPydantic(
            id="user",
            name="User",
            username="user@anl.gov",
            user_group_uuids=["restricted-model-group"],
            idp_id="idp",
            idp_name="ANL",
            auth_service="globus",
        )
        self.assertTrue(endpoint.check_permission(user))
        self.assertFalse(
            endpoint.check_permission(
                user.model_copy(update={"user_group_uuids": []}), raise_exc=False
            )
        )
        self.assertFalse(
            endpoint.check_permission(
                user.model_copy(update={"username": "user@other.example"}),
                raise_exc=False,
            )
        )

    async def test_unary_read_timeout_retains_gateway_error(self) -> None:
        def timeout(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("no response data", request=request)

        real_client = httpx.AsyncClient
        with patch.object(
            first_v2.httpx,
            "AsyncClient",
            side_effect=lambda **kwargs: real_client(
                transport=httpx.MockTransport(timeout), **kwargs
            ),
        ):
            endpoint = self.make_endpoint()
        try:
            with self.assertRaises(EndpointError) as raised:
                await endpoint.submit_task({"model_params": {}})
            self.assertEqual(raised.exception.status_code, 504)
            self.assertEqual(raised.exception.info, {"timeout": 900})
        finally:
            await endpoint._client.aclose()
