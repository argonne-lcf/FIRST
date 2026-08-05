import asyncio
import re
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from django.http import JsonResponse
from django.test import RequestFactory, SimpleTestCase, override_settings

from resource_server_async.endpoints.direct_api import (
    DirectAPIEndpoint,
    DirectAPIEndpointConfig,
)
from resource_server_async.endpoints.minerva import MinervaEndpoint
from resource_server_async.errors import EndpointError
from resource_server_async.logging import (
    AccessLogMiddleware,
    RequestContext,
    _request_context,
    get_request_context,
)
from resource_server_async.minerva_affinity import (
    AFFINITY_HEADER,
    CACHE_SALT_ENDPOINTS,
    REQUEST_ID_HEADER,
    derive_affinity_key,
    derive_cache_salt,
    derive_minerva_request_values,
    validate_session_id,
)
from resource_server_async.schemas.structured_logs import (
    AccessLogPydantic,
    UserPydantic,
)

TEST_SECRET = "0123456789abcdef0123456789abcdef-test-secret"


def request_context(
    *,
    user_id: str = "user-a",
    session_id: str | None = "session-a",
    request_id: str = "request-a",
) -> RequestContext:
    return RequestContext(
        access_log=AccessLogPydantic(
            id=request_id,
            timestamp_request="2026-08-04T00:00:00Z",
            api_route="/minerva/api/v1/chat/completions",
            origin_ip="127.0.0.1",
        ),
        user=UserPydantic(
            id=user_id,
            name="Test User",
            username=f"{user_id}@example.test",
            user_group_uuids=[],
            idp_id="test-idp",
            idp_name="Test IDP",
            auth_service="test",
        ),
        minerva_session_id=session_id,
    )


def minerva_endpoint(model: str = "model-a") -> MinervaEndpoint:
    endpoint = object.__new__(MinervaEndpoint)
    endpoint._BaseEndpoint__model = model
    return endpoint


class SessionIdentifierChecks(SimpleTestCase):
    def test_visible_ascii_and_empty_fallback(self) -> None:
        self.assertEqual(
            validate_session_id("conversation-42/branch_a"), "conversation-42/branch_a"
        )
        self.assertIsNone(validate_session_id(None))
        self.assertIsNone(validate_session_id(""))

    def test_invalid_or_overlength_values_are_rejected_without_echo(self) -> None:
        for value in ("contains space", "control\tvalue", "é", "x" * 129):
            with (
                self.subTest(value=repr(value)),
                self.assertRaises(ValueError) as raised,
            ):
                validate_session_id(value)
            self.assertNotIn(value, str(raised.exception))

    async def test_middleware_captures_valid_value_and_rejects_invalid_value(
        self,
    ) -> None:
        observed: list[str | None] = []

        async def get_response(_request):
            observed.append(get_request_context().minerva_session_id)
            return JsonResponse({"ok": True})

        middleware = AccessLogMiddleware(get_response)
        factory = RequestFactory()
        with patch(
            "resource_server_async.logging.should_skip_logging",
            new=AsyncMock(return_value=True),
        ):
            valid = factory.get("/test", HTTP_X_ALCF_SESSION_ID="session-valid")
            response = await middleware(valid)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(observed, ["session-valid"])

            invalid = factory.get("/test")
            invalid.META["HTTP_X_ALCF_SESSION_ID"] = "invalid\tvalue"
            response = await middleware(invalid)
            self.assertEqual(response.status_code, 400)
            self.assertEqual(observed, ["session-valid"])
            self.assertNotIn("invalid", response.content.decode("utf-8"))

    @override_settings(MINERVA_AFFINITY_HMAC_KEY=TEST_SECRET)
    async def test_caller_affinity_header_is_not_forwarded(self) -> None:
        observed: list[str] = []

        async def get_response(_request):
            context = get_request_context()
            context.user = request_context().user
            headers, _salt = derive_minerva_request_values(context, "model-a")
            observed.append(headers[AFFINITY_HEADER])
            return JsonResponse({"ok": True})

        middleware = AccessLogMiddleware(get_response)
        request = RequestFactory().get(
            "/resource_server/v1/chat/completions",
            HTTP_X_ALCF_SESSION_ID="session-valid",
            HTTP_X_MINERVA_AFFINITY_KEY="caller-must-not-win",
        )
        with patch(
            "resource_server_async.logging.should_skip_logging",
            new=AsyncMock(return_value=True),
        ):
            response = await middleware(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(observed), 1)
        self.assertNotEqual(observed[0], "caller-must-not-win")
        self.assertRegex(observed[0], re.compile(r"^[A-Za-z0-9_-]{43}$"))


@override_settings(MINERVA_AFFINITY_HMAC_KEY=TEST_SECRET)
class DerivationChecks(SimpleTestCase):
    def test_affinity_namespaces_user_model_and_session(self) -> None:
        secret = TEST_SECRET.encode("utf-8")
        base = derive_affinity_key(secret, "user-a", "model-a", "session-a")
        self.assertEqual(
            base,
            derive_affinity_key(secret, "user-a", "model-a", "session-a"),
        )
        self.assertNotEqual(
            base,
            derive_affinity_key(secret, "user-a", "model-a", "session-b"),
        )
        self.assertNotEqual(
            base,
            derive_affinity_key(secret, "user-b", "model-a", "session-a"),
        )
        self.assertNotEqual(
            base,
            derive_affinity_key(secret, "user-a", "model-b", "session-a"),
        )

    def test_missing_session_has_stable_user_level_fallback(self) -> None:
        secret = TEST_SECRET.encode("utf-8")
        first = derive_affinity_key(secret, "user-a", "model-a", None)
        second = derive_affinity_key(secret, "user-a", "model-a", None)
        self.assertEqual(first, second)
        self.assertNotEqual(
            first, derive_affinity_key(secret, "user-b", "model-a", None)
        )

    def test_cache_salt_is_stable_across_sessions_and_isolated(self) -> None:
        first_headers, first_salt = derive_minerva_request_values(
            request_context(session_id="session-a"), "model-a"
        )
        second_headers, second_salt = derive_minerva_request_values(
            request_context(session_id="session-b"), "model-a"
        )
        other_user_headers, other_user_salt = derive_minerva_request_values(
            request_context(user_id="user-b", session_id="session-a"), "model-a"
        )
        _, other_model_salt = derive_minerva_request_values(
            request_context(session_id="session-a"), "model-b"
        )
        self.assertEqual(first_salt, second_salt)
        self.assertNotEqual(
            first_headers[AFFINITY_HEADER], second_headers[AFFINITY_HEADER]
        )
        self.assertNotEqual(
            first_headers[AFFINITY_HEADER], other_user_headers[AFFINITY_HEADER]
        )
        self.assertNotEqual(first_salt, other_user_salt)
        self.assertNotEqual(first_salt, other_model_salt)

    def test_values_are_256_bit_unpadded_base64url_and_request_id_is_preserved(
        self,
    ) -> None:
        headers, salt = derive_minerva_request_values(
            request_context(request_id="first-access-request-id"), "model-a"
        )
        for value in (headers[AFFINITY_HEADER], salt):
            self.assertEqual(len(value), 43)
            self.assertRegex(value, re.compile(r"^[A-Za-z0-9_-]{43}$"))
            self.assertNotIn("=", value)
        self.assertEqual(headers[REQUEST_ID_HEADER], "first-access-request-id")


@override_settings(MINERVA_AFFINITY_HMAC_KEY=TEST_SECRET)
class MinervaRequestPreparationChecks(SimpleTestCase):
    def setUp(self) -> None:
        self.endpoint = minerva_endpoint()
        self.context_token = _request_context.set(request_context())

    def tearDown(self) -> None:
        _request_context.reset(self.context_token)

    def test_supported_protocols_receive_server_derived_cache_salt(self) -> None:
        expected_salt = derive_cache_salt(
            TEST_SECRET.encode("utf-8"), "user-a", "model-a"
        )
        for endpoint in sorted(CACHE_SALT_ENDPOINTS):
            with self.subTest(endpoint=endpoint):
                body, headers = self.endpoint._prepare_request(
                    {
                        "model_params": {
                            "model": "model-a",
                            "openai_endpoint": endpoint,
                            "cache_salt": "caller-must-not-win",
                        }
                    },
                    stream=False,
                )
                self.assertEqual(body["cache_salt"], expected_salt)
                self.assertNotEqual(headers[AFFINITY_HEADER], "caller-must-not-win")

    def test_messages_health_metrics_and_unknown_protocols_omit_salt(self) -> None:
        for endpoint in ("messages", "health", "metrics", "future-protocol"):
            with self.subTest(endpoint=endpoint):
                body, headers = self.endpoint._prepare_request(
                    {
                        "model_params": {
                            "model": "model-a",
                            "openai_endpoint": endpoint,
                            "cache_salt": "caller-must-be-removed",
                        }
                    },
                    stream=True,
                )
                self.assertNotIn("cache_salt", body)
                self.assertTrue(body["stream"])
                self.assertIn(AFFINITY_HEADER, headers)

    @override_settings(MINERVA_AFFINITY_HMAC_KEY=None)
    def test_missing_key_fails_closed_only_when_minerva_is_prepared(self) -> None:
        with self.assertRaises(EndpointError) as raised:
            self.endpoint._prepare_request(
                {"model_params": {"model": "model-a"}}, stream=False
            )
        self.assertEqual(raised.exception.status_code, 500)
        self.assertIn("MINERVA_AFFINITY_HMAC_KEY", str(raised.exception))

    async def test_streaming_and_nonstreaming_pass_local_headers_and_body(self) -> None:
        self.endpoint.check_endpoint_status = AsyncMock(return_value=True)
        data = {
            "model_params": {
                "model": "model-a",
                "messages": [{"role": "user", "content": "hello"}],
                "openai_endpoint": "chat/completions",
            }
        }
        with patch.object(
            DirectAPIEndpoint,
            "_submit_task_with_headers",
            new=AsyncMock(return_value="non-stream-result"),
        ) as submit_task:
            result = await self.endpoint.submit_task(data)
            self.assertEqual(result, "non-stream-result")
            body = submit_task.await_args.args[0]
            headers = submit_task.await_args.kwargs["request_headers"]
            self.assertFalse(body["stream"])
            self.assertIn("cache_salt", body)
            self.assertIn(AFFINITY_HEADER, headers)

        with patch.object(
            DirectAPIEndpoint,
            "_submit_streaming_task_with_headers",
            new=AsyncMock(return_value="stream-result"),
        ) as submit_stream:
            result = await self.endpoint.submit_streaming_task(data)
            self.assertEqual(result, "stream-result")
            body = submit_stream.await_args.args[0]
            headers = submit_stream.await_args.kwargs["request_headers"]
            self.assertTrue(body["stream"])
            self.assertIn("cache_salt", body)
            self.assertIn(AFFINITY_HEADER, headers)

    async def test_concurrent_users_do_not_contaminate_shared_endpoint_headers(
        self,
    ) -> None:
        self.endpoint.check_endpoint_status = AsyncMock(return_value=True)
        captures: dict[str, tuple[dict, dict]] = {}

        async def capture(_endpoint, body, request_headers=None):
            await asyncio.sleep(0)
            user = get_request_context().user
            assert user is not None
            captures[user.id] = (dict(body), dict(request_headers or {}))
            return user.id

        data = {
            "model_params": {
                "model": "model-a",
                "messages": [{"role": "user", "content": "hello"}],
                "openai_endpoint": "chat/completions",
            }
        }

        async def invoke(user_id: str):
            token = _request_context.set(
                request_context(user_id=user_id, session_id="same-session")
            )
            try:
                return await self.endpoint.submit_task(data)
            finally:
                _request_context.reset(token)

        with patch.object(DirectAPIEndpoint, "_submit_task_with_headers", new=capture):
            await asyncio.gather(invoke("user-one"), invoke("user-two"))

        self.assertEqual(set(captures), {"user-one", "user-two"})
        first_body, first_headers = captures["user-one"]
        second_body, second_headers = captures["user-two"]
        self.assertNotEqual(
            first_headers[AFFINITY_HEADER], second_headers[AFFINITY_HEADER]
        )
        self.assertNotEqual(first_body["cache_salt"], second_body["cache_salt"])


class FakeStreamResponse:
    status_code = 200

    async def aread(self):
        return b""

    async def aiter_text(self):
        yield "data: captured\n\n"
        yield "data: [DONE]\n\n"


class FakeAsyncContext:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *_args):
        return False


class FakeStreamingClient:
    def __init__(self, capture: dict, **_kwargs):
        self.capture = capture

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    def stream(self, method, url, *, json, headers):
        self.capture.update(
            {
                "method": method,
                "url": url,
                "json": json,
                "headers": headers,
            }
        )
        return FakeAsyncContext(FakeStreamResponse())


class DirectAPIIsolationChecks(SimpleTestCase):
    def direct_endpoint(self) -> DirectAPIEndpoint:
        endpoint = object.__new__(DirectAPIEndpoint)
        endpoint._DirectAPIEndpoint__config = DirectAPIEndpointConfig(
            api_url="https://api.example/v1",
            api_key_env_name="TEST_API_KEY",
            trust_env=False,
        )
        endpoint._DirectAPIEndpoint__httpx_client = SimpleNamespace(
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer test-key",
            }
        )
        endpoint._BaseEndpoint__model = "model-a"
        endpoint._BaseEndpoint__endpoint_slug = "direct-test"
        return endpoint

    @staticmethod
    def header_value(headers: dict[str, str], name: str) -> str:
        return {key.casefold(): value for key, value in headers.items()}[
            name.casefold()
        ]

    async def test_stream_captures_top_level_body_and_headers_before_iteration(
        self,
    ) -> None:
        endpoint = self.direct_endpoint()
        endpoint.httpx_client.headers["x-rEqUeSt-Id"] = "configured-request-id"
        capture: dict = {}
        data = {
            "model": "model-a",
            "openai_endpoint": "chat/completions",
            "messages": [{"role": "user", "content": "original"}],
            "cache_salt": "original-salt",
        }
        affinity_name = "x-MiNeRvA-aFfInItY-kEy"
        headers = {
            affinity_name: "original-affinity",
            "X-ReQuEsT-ID": "request-local-id",
        }
        with patch(
            "resource_server_async.endpoints.direct_api.httpx.AsyncClient",
            side_effect=lambda **kwargs: FakeStreamingClient(capture, **kwargs),
        ):
            result = await endpoint._submit_streaming_task_with_headers(
                data, request_headers=headers
            )
            data["messages"] = [{"role": "user", "content": "mutated"}]
            data["cache_salt"] = "mutated-salt"
            headers[affinity_name] = "mutated-affinity"
            chunks = [chunk async for chunk in result.response.streaming_content]

        self.assertEqual(chunks, [b"data: captured\n\n", b"data: [DONE]\n\n"])
        self.assertEqual(capture["json"]["messages"][0]["content"], "original")
        self.assertEqual(capture["json"]["cache_salt"], "original-salt")
        self.assertEqual(
            self.header_value(capture["headers"], AFFINITY_HEADER),
            "original-affinity",
        )
        self.assertEqual(
            self.header_value(capture["headers"], REQUEST_ID_HEADER),
            "configured-request-id",
        )
        self.assertEqual(
            self.header_value(capture["headers"], "Authorization"),
            "Bearer test-key",
        )

    async def test_nonstream_copies_top_level_body_and_headers(self) -> None:
        endpoint = self.direct_endpoint()
        endpoint.httpx_client.headers["x-rEqUeSt-Id"] = "configured-request-id"
        endpoint.httpx_client.post = AsyncMock(return_value={"ok": True})
        data = {
            "model": "model-a",
            "openai_endpoint": "chat/completions",
            "messages": [{"role": "user", "content": "original"}],
        }
        affinity_name = "x-MiNeRvA-aFfInItY-kEy"
        headers = {
            affinity_name: "original-affinity",
            "X-ReQuEsT-ID": "request-local-id",
        }

        await endpoint._submit_task_with_headers(data, request_headers=headers)
        data["messages"] = [{"role": "user", "content": "mutated"}]
        headers[affinity_name] = "mutated-affinity"

        sent = endpoint.httpx_client.post.await_args
        self.assertIsNot(sent.kwargs["data"], data)
        self.assertEqual(sent.kwargs["data"]["messages"][0]["content"], "original")
        self.assertEqual(
            self.header_value(sent.kwargs["headers"], AFFINITY_HEADER),
            "original-affinity",
        )
        self.assertEqual(
            self.header_value(sent.kwargs["headers"], REQUEST_ID_HEADER),
            "configured-request-id",
        )
        self.assertEqual(
            self.header_value(sent.kwargs["headers"], "Authorization"),
            "Bearer test-key",
        )
        self.assertNotIn(AFFINITY_HEADER, endpoint.httpx_client.headers)

    async def test_nonstream_rejects_mixed_case_default_header_overrides(
        self,
    ) -> None:
        endpoint = self.direct_endpoint()
        endpoint.httpx_client.post = AsyncMock(return_value={"ok": True})
        data = {"model": "model-a"}

        for name in ("aUtHoRiZaTiOn", "cOnTeNt-TyPe"):
            with self.subTest(name=name):
                supplied_value = f"caller-value-for-{name}"
                with self.assertRaises(EndpointError) as raised:
                    await endpoint._submit_task_with_headers(
                        data, request_headers={name: supplied_value}
                    )
                self.assertEqual(raised.exception.status_code, 400)
                self.assertNotIn(supplied_value, str(raised.exception))
        endpoint.httpx_client.post.assert_not_awaited()

    async def test_stream_rejects_mixed_case_default_header_overrides(
        self,
    ) -> None:
        endpoint = self.direct_endpoint()
        data = {"model": "model-a"}

        for name in ("aUtHoRiZaTiOn", "cOnTeNt-TyPe"):
            with self.subTest(name=name):
                supplied_value = f"caller-value-for-{name}"
                with self.assertRaises(EndpointError) as raised:
                    await endpoint._submit_streaming_task_with_headers(
                        data, request_headers={name: supplied_value}
                    )
                self.assertEqual(raised.exception.status_code, 400)
                self.assertNotIn(supplied_value, str(raised.exception))

    async def test_non_minerva_direct_request_gets_no_minerva_values(self) -> None:
        endpoint = self.direct_endpoint()
        endpoint.httpx_client.post = AsyncMock(return_value={"ok": True})
        data = {
            "model": "model-a",
            "openai_endpoint": "chat/completions",
            "messages": [{"role": "user", "content": "hello"}],
        }
        await endpoint.submit_task(data)
        sent = endpoint.httpx_client.post.await_args
        self.assertIsNone(sent.kwargs["headers"])
        self.assertNotIn("cache_salt", sent.kwargs["data"])
        self.assertNotIn(AFFINITY_HEADER, endpoint.httpx_client.headers)
