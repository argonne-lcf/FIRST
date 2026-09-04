import asyncio
import json
from concurrent.futures import Future
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import httpx
from django.http import StreamingHttpResponse
from django.test import SimpleTestCase

from resource_server_async.clusters import BaseCluster
from resource_server_async.endpoints import BaseEndpoint
from resource_server_async.endpoints.direct_api import (
    DirectAPIEndpoint,
    DirectAPIEndpointConfig,
)
from resource_server_async.endpoints.globus_compute import (
    GlobusComputeEndpoint,
    GlobusComputeEndpointConfig,
)
from resource_server_async.endpoints.metis import MetisEndpoint
from resource_server_async.errors import EndpointError, InvalidRequest
from resource_server_async.logging import RequestContext
from resource_server_async.schemas.endpoints import (
    SubmitStreamingTaskResponse,
    SubmitTaskResult,
)
from resource_server_async.schemas.openai_control import (
    FIRST_RESERVED_OPENAI_FIELDS,
    ChatCompletionsControl,
)
from resource_server_async.schemas.structured_logs import (
    AccessLogPydantic,
    UserPydantic,
)
from resource_server_async.services import (
    _prepare_openai_request,
    submit_openai_inference_request,
)

from . import (
    CLIENT,
    KWARGS,
    PREMIUM_HEADERS,
    ResourceServerTestCase,
    get_response_json,
)

OPAQUE_VALUE = "opaque-extension-value-that-must-not-be-logged"


def request_context() -> RequestContext:
    return RequestContext(
        access_log=AccessLogPydantic(
            id="request-a",
            timestamp_request="2026-08-07T00:00:00Z",
            api_route="/cluster/framework/v1/chat/completions",
            origin_ip="127.0.0.1",
        ),
        user=UserPydantic(
            id="user-a",
            name="Test User",
            username="user-a@example.test",
            user_group_uuids=[],
            idp_id="test-idp",
            idp_name="Test IDP",
            auth_service="test",
        ),
    )


def backend_fields() -> dict[str, object]:
    return {
        "unknown_scalar": OPAQUE_VALUE,
        "unknown_object": {"nested": [1, False, None]},
        "unknown_list": ["a", {"b": 2}],
        "chat_template_kwargs": {"enable_thinking": False},
        "extra_body": {
            "future_backend_option": True,
            "cache_salt": "nested-is-backend-data",
            "openai_endpoint": "nested-is-not-a-route",
        },
    }


class OpenAIRequestBoundaryTests(SimpleTestCase):
    def test_backend_fields_survive_without_named_schema_entries(self) -> None:
        payload = {
            "model": "model-alias",
            "messages": [{"role": "user", "content": "hello"}],
            **backend_fields(),
        }
        original = json.loads(json.dumps(payload))

        control, outbound, prompt = _prepare_openai_request(payload, "chat/completions")

        self.assertEqual(control.model, "model-alias")
        self.assertEqual(prompt, payload["messages"])
        self.assertEqual(payload, original)
        self.assertEqual(
            {key: outbound[key] for key in backend_fields()}, backend_fields()
        )
        self.assertNotIn("chat_template_kwargs", ChatCompletionsControl.model_fields)
        self.assertIn("extra_body", outbound)
        self.assertNotIn("future_backend_option", outbound)
        self.assertEqual(outbound["extra_body"], payload["extra_body"])
        self.assertEqual(outbound["openai_endpoint"], "chat/completions")
        self.assertFalse(outbound["stream"])

    def test_all_route_control_projections_accept_opaque_backend_fields(self) -> None:
        cases = {
            "chat/completions": {
                "model": "model",
                "messages": [{"role": "user", "content": "hello"}],
            },
            "completions": {"model": "model", "prompt": [1, 2, 3]},
            "embeddings": {"model": "model", "input": ["one", "two"]},
            "responses": {
                "model": "model",
                "input": [{"role": "user", "content": "hello"}],
            },
            "messages": {
                "model": "model",
                "messages": [{"role": "user", "content": "hello"}],
                "max_tokens": 1024,
                "system": [{"type": "text", "text": "be brief"}],
                "temperature": 1.0,
            },
        }
        for route, control_fields in cases.items():
            with self.subTest(route=route):
                payload = {**control_fields, **backend_fields()}
                _control, outbound, _prompt = _prepare_openai_request(payload, route)
                for name, value in backend_fields().items():
                    self.assertEqual(outbound[name], value)

    def test_control_fields_are_strictly_validated_without_echoing_values(self) -> None:
        invalid = {
            "model": "model",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": OPAQUE_VALUE,
        }
        with self.assertRaises(InvalidRequest) as raised:
            _prepare_openai_request(invalid, "chat/completions")

        self.assertEqual(raised.exception.status_code, 422)
        self.assertNotIn(OPAQUE_VALUE, str(raised.exception))
        self.assertNotIn(OPAQUE_VALUE, repr(raised.exception.info))
        self.assertEqual(raised.exception.info["errors"][0]["field"], "stream")

    def test_each_top_level_reserved_field_is_rejected_without_value_echo(self) -> None:
        base = {
            "model": "model",
            "messages": [{"role": "user", "content": "hello"}],
        }
        for field in FIRST_RESERVED_OPENAI_FIELDS:
            with self.subTest(field=field), self.assertRaises(InvalidRequest) as raised:
                _prepare_openai_request(
                    {**base, field: OPAQUE_VALUE}, "chat/completions"
                )
            self.assertEqual(raised.exception.info, {"fields": [field]})
            self.assertNotIn(OPAQUE_VALUE, str(raised.exception))
            self.assertNotIn(OPAQUE_VALUE, repr(raised.exception.info))


class OpenAIServiceDispatchTests(SimpleTestCase):
    async def _dispatch(self, stream: bool) -> tuple[dict[str, object], object]:
        maintenance = SimpleNamespace(raise_if_down=Mock())
        cluster = SimpleNamespace(
            cluster_name="cluster",
            frameworks=["framework"],
            openai_endpoints=["chat/completions"],
            check_maintenance=AsyncMock(return_value=maintenance),
        )
        response = StreamingHttpResponse([])
        endpoint = SimpleNamespace(
            endpoint_slug="cluster-framework-canonical-model",
            model="canonical-model",
            check_permission=Mock(),
            check_token_rate_limit=Mock(return_value=SimpleNamespace(allow=True)),
            submit_task=AsyncMock(
                return_value=SubmitTaskResult(result={"ok": True}, task_id="task-a")
            ),
            submit_streaming_task=AsyncMock(
                return_value=SubmitStreamingTaskResponse(
                    response=response, task_id="task-b"
                )
            ),
        )
        payload: dict[str, object] = {
            "model": "model-alias",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": stream,
            **backend_fields(),
        }
        original = json.loads(json.dumps(payload))

        with (
            patch.object(
                BaseCluster, "load_adapter", new=AsyncMock(return_value=cluster)
            ),
            patch.object(
                BaseEndpoint, "load_adapter", new=AsyncMock(return_value=endpoint)
            ) as load_endpoint,
            self.assertLogs("resource_server_async.services", level="DEBUG") as logs,
        ):
            result = await submit_openai_inference_request(
                request_context(),
                "cluster",
                "framework",
                payload,
                openai_endpoint="chat/completions",
            )

        self.assertEqual(payload, original)
        load_endpoint.assert_awaited_once_with("cluster", "framework", "model-alias")
        joined_logs = "\n".join(logs.output)
        self.assertNotIn(OPAQUE_VALUE, joined_logs)
        self.assertNotIn("enable_thinking", joined_logs)

        submit = endpoint.submit_streaming_task if stream else endpoint.submit_task
        submit.assert_awaited_once()
        sent = submit.await_args.args[0]["model_params"]
        expected = {
            **original,
            "model": "canonical-model",
            "stream": stream,
            "openai_endpoint": "chat/completions",
        }
        self.assertEqual(sent, expected)
        other_submit = (
            endpoint.submit_task if stream else endpoint.submit_streaming_task
        )
        other_submit.assert_not_awaited()
        return sent, result

    async def test_nonstreaming_preserves_backend_fields_and_canonicalizes_model(
        self,
    ) -> None:
        sent, result = await self._dispatch(stream=False)
        self.assertFalse(sent["stream"])
        self.assertEqual(result, {"ok": True})

    async def test_streaming_preserves_backend_fields_and_controls_dispatch(
        self,
    ) -> None:
        sent, result = await self._dispatch(stream=True)
        self.assertTrue(sent["stream"])
        self.assertIsInstance(result, StreamingHttpResponse)


def direct_endpoint() -> DirectAPIEndpoint:
    endpoint = object.__new__(DirectAPIEndpoint)
    endpoint._DirectAPIEndpoint__config = DirectAPIEndpointConfig(
        api_url="https://api.example/v1",
        api_key_env_name="TEST_API_KEY",
        trust_env=False,
    )
    endpoint._DirectAPIEndpoint__httpx_client = SimpleNamespace(
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer configured-key",
        },
        post=AsyncMock(),
    )
    endpoint._BaseEndpoint__model = "model"
    endpoint._BaseEndpoint__endpoint_slug = "direct-test"
    return endpoint


class AdapterBoundaryTests(SimpleTestCase):
    def test_direct_api_unwraps_shared_envelope_without_changing_backend_fields(
        self,
    ) -> None:
        endpoint = direct_endpoint()
        params = {
            "model": "model",
            "messages": [{"role": "user", "content": "hello"}],
            "openai_endpoint": "chat/completions",
            **backend_fields(),
        }
        envelope = {"model_params": params}

        nonstream = endpoint._prepare_request_body(envelope, stream=False)
        streaming = endpoint._prepare_request_body(envelope, stream=True)

        self.assertIsNot(nonstream, params)
        self.assertFalse(nonstream["stream"])
        self.assertTrue(streaming["stream"])
        for name, value in backend_fields().items():
            self.assertEqual(nonstream[name], value)
            self.assertEqual(streaming[name], value)

    async def test_backend_extension_rejection_keeps_backend_status(self) -> None:
        endpoint = direct_endpoint()
        request = httpx.Request("POST", "https://api.example/v1/chat/completions")
        response = httpx.Response(
            422,
            request=request,
            content=b'{"error":"unsupported extension"}',
        )
        endpoint.httpx_client.post.side_effect = httpx.HTTPStatusError(
            "unprocessable", request=request, response=response
        )
        data = {
            "model_params": {
                "model": "model",
                "messages": [{"role": "user", "content": "hello"}],
                "openai_endpoint": "chat/completions",
                "future_backend_option": True,
            }
        }

        with self.assertRaises(EndpointError) as raised:
            await endpoint.submit_task(data)

        self.assertEqual(raised.exception.status_code, 422)
        self.assertIn("Upstream endpoint returned 422", str(raised.exception))

    async def test_metis_preserves_fields_on_both_dispatch_paths(self) -> None:
        endpoint = object.__new__(MetisEndpoint)
        endpoint._BaseEndpoint__model = "model"
        endpoint._BaseEndpoint__endpoint_slug = "metis-test"
        endpoint.check_endpoint_status = AsyncMock(return_value=True)
        data = {
            "model_params": {
                "model": "model",
                "messages": [{"role": "user", "content": "hello"}],
                "openai_endpoint": "chat/completions",
                **backend_fields(),
            }
        }

        with patch.object(
            DirectAPIEndpoint,
            "_submit_task_with_headers",
            new=AsyncMock(
                return_value=SubmitTaskResult(result={"ok": True}, task_id=None)
            ),
        ) as submit:
            await endpoint.submit_task(data)
            sent = submit.await_args.args[0]
            self.assertFalse(sent["stream"])
            for name, value in backend_fields().items():
                self.assertEqual(sent[name], value)

        with patch.object(
            DirectAPIEndpoint,
            "_submit_streaming_task_with_headers",
            new=AsyncMock(
                return_value=SubmitStreamingTaskResponse(
                    response=StreamingHttpResponse([]), task_id=None
                )
            ),
        ) as submit_stream:
            await endpoint.submit_streaming_task(data)
            sent = submit_stream.await_args.args[0]
            self.assertTrue(sent["stream"])
            for name, value in backend_fields().items():
                self.assertEqual(sent[name], value)

    async def test_globus_adds_only_protocol_metadata_to_a_copy(self) -> None:
        endpoint = object.__new__(GlobusComputeEndpoint)
        endpoint._GlobusComputeEndpoint__config = GlobusComputeEndpointConfig(
            api_port=8000,
            endpoint_uuid="endpoint-id",
            function_uuid="function-id",
        )
        endpoint._BaseEndpoint__endpoint_slug = "globus-test"
        endpoint._BaseEndpoint__model = "model"
        endpoint.prepare_executor = AsyncMock(return_value=object())
        params = {
            "model": "model",
            "messages": [{"role": "user", "content": "hello"}],
            "openai_endpoint": "chat/completions",
            **backend_fields(),
        }
        data = {"model_params": params}
        original = json.loads(json.dumps(data))

        with patch(
            "resource_server_async.endpoints.globus_compute.globus_utils.submit_and_get_result",
            new=AsyncMock(
                return_value=SubmitTaskResult(result={"ok": True}, task_id="task")
            ),
        ) as submit:
            await endpoint.submit_task(data)

        sent = submit.await_args.kwargs["data"]
        self.assertEqual(data, original)
        self.assertEqual(sent["model_params"]["api_port"], 8000)
        for name, value in backend_fields().items():
            self.assertEqual(sent["model_params"][name], value)

    async def test_globus_streaming_metadata_does_not_mutate_input(self) -> None:
        endpoint = object.__new__(GlobusComputeEndpoint)
        endpoint._GlobusComputeEndpoint__config = GlobusComputeEndpointConfig(
            api_port=8000,
            endpoint_uuid="endpoint-id",
            function_uuid="function-id",
        )
        endpoint._BaseEndpoint__endpoint_slug = "globus-test"
        endpoint._BaseEndpoint__model = "model"
        future = Future()
        future.task_id = "task-id"
        executor = SimpleNamespace(
            endpoint_id=None,
            submit_to_registered_function=Mock(return_value=future),
        )
        endpoint.prepare_executor = AsyncMock(return_value=executor)
        data = {
            "model_params": {
                "model": "model",
                "messages": [{"role": "user", "content": "hello"}],
                "openai_endpoint": "chat/completions",
                **backend_fields(),
            }
        }
        original = json.loads(json.dumps(data))
        captured: dict[str, object] = {}

        def add_streaming_metadata(
            request_data: dict[str, object], stream_task_id: str
        ) -> dict[str, object]:
            captured["data"] = request_data
            request_data["model_params"].update(  # type: ignore[union-attr]
                {
                    "streaming_server_host": "first.example",
                    "streaming_server_port": 443,
                    "streaming_server_protocol": "https",
                    "stream_task_id": stream_task_id,
                    "stream_task_token": "server-token",
                }
            )
            return request_data

        with (
            patch(
                "resource_server_async.endpoints.globus_compute.prepare_streaming_task_data",
                side_effect=add_streaming_metadata,
            ),
            patch(
                "resource_server_async.endpoints.globus_compute.asyncio.wrap_future",
                return_value=asyncio.Future(),
            ),
            patch(
                "resource_server_async.endpoints.globus_compute.asyncio.wait_for",
                new=AsyncMock(side_effect=asyncio.TimeoutError),
            ),
            patch(
                "resource_server_async.endpoints.globus_compute.cache_item_async",
                new=AsyncMock(),
            ),
            patch(
                "resource_server_async.endpoints.globus_compute.get_request_context",
                side_effect=LookupError,
            ),
        ):
            await endpoint.submit_streaming_task(data)

        sent = captured["data"]
        self.assertEqual(data, original)
        self.assertEqual(sent["model_params"]["api_port"], 8000)  # type: ignore[index]
        for name, value in backend_fields().items():
            self.assertEqual(sent["model_params"][name], value)  # type: ignore[index]


class OpenAIResponsibilitySeparationViewTests(ResourceServerTestCase):
    url = "/your-other-cluster/api/v1/chat/completions"

    async def test_raw_backend_fields_reach_direct_api(self) -> None:
        captured: dict[str, object] = {}
        payload = {
            "model": "Your-Model-120B",
            "messages": [{"role": "user", "content": "hello"}],
            **backend_fields(),
        }

        async def capture_post(
            _client: object,
            url: str,
            data: object = None,
            headers: dict[str, str] | None = None,
        ) -> dict[str, bool]:
            captured.update({"url": url, "data": data, "headers": headers})
            return {"ok": True}

        with patch(
            "resource_server_async.httpx_client.AsyncHttpClient.post", new=capture_post
        ):
            response = await CLIENT.post(
                self.url,
                data=json.dumps(payload).encode(),
                headers=PREMIUM_HEADERS,
                **KWARGS,
            )

        self.assertEqual(response.status_code, 200, get_response_json(response))
        expected = {
            **payload,
            "model": "Your-Model-120B",
            "stream": False,
        }
        self.assertEqual(captured["data"], expected)
        self.assertEqual(captured["url"], "http://127.0.0.1:8080/chat/completions")

    async def test_backend_422_is_not_a_first_schema_error(self) -> None:
        async def reject_post(
            _client: object,
            url: str,
            data: object = None,
            headers: dict[str, str] | None = None,
        ) -> None:
            request = httpx.Request("POST", url)
            response = httpx.Response(
                422,
                request=request,
                content=b'{"error":"unsupported future_backend_option"}',
            )
            raise httpx.HTTPStatusError(
                "unprocessable", request=request, response=response
            )

        payload = {
            "model": "Your-Model-120B",
            "messages": [{"role": "user", "content": "hello"}],
            "future_backend_option": True,
        }
        with patch(
            "resource_server_async.httpx_client.AsyncHttpClient.post", new=reject_post
        ):
            response = await CLIENT.post(
                self.url,
                data=json.dumps(payload).encode(),
                headers=PREMIUM_HEADERS,
                **KWARGS,
            )

        body = get_response_json(response)
        self.assertEqual(response.status_code, 422)
        self.assertEqual(body["error"]["code"], "internal_endpoint_error")
        self.assertIn("Upstream endpoint returned 422", body["error"]["message"])

    async def test_json_body_must_be_an_object(self) -> None:
        response = await CLIENT.post(
            self.url,
            data=json.dumps(["not", "an", "object"]).encode(),
            headers=PREMIUM_HEADERS,
            **KWARGS,
        )
        self.assertEqual(response.status_code, 422)
