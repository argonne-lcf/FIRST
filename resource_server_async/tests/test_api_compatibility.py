from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from django.http import StreamingHttpResponse
from django.test import TestCase

from resource_server_async.errors import ResponseNotFound
from resource_server_async.models import StoredResponse
from resource_server_async.services import (
    _prepare_responses_continuation,
    _store_responses_result,
    _store_streamed_responses_result,
    delete_stored_response,
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


