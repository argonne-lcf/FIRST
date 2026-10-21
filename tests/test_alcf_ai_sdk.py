"""alcf-ai SDK: error mapping, SSE framing, inference calls, and warm-up."""

from pathlib import Path
from typing import Any

import httpx
import pytest

from alcf_ai import InferenceClient, StartupStatus
from alcf_ai._http import iter_sse, raise_for_status
from alcf_ai.inference import (
    response_reasoning,
    response_text,
    response_usage,
    stream_delta,
)
from first_common.errors import (
    FirstError,
    NotFound,
    ServiceUnavailable,
    TooManyRequests,
)

from .fixtures.alcf_ai import FakeGateway, cold_start_response, error_body, sse

# --- raise_for_status -----------------------------------------------------------------


def _response(status: int, content: bytes = b"", **headers: str) -> httpx.Response:
    resp = httpx.Response(status, content=content, headers=headers)
    resp.request = httpx.Request("GET", "http://gateway.test/x")
    return resp


def test_raise_for_status_maps_codes_to_typed_errors() -> None:
    with pytest.raises(ServiceUnavailable) as exc:
        raise_for_status(cold_start_response(eta=30))
    assert exc.value.status_code == 503
    assert exc.value.info == {"startup_eta_sec": 30, "typical_startup_sec": 120}
    assert exc.value.retry_after_sec == 5

    with pytest.raises(TooManyRequests):
        raise_for_status(
            _response(429, error_body("too_many_requests", "Requests per minute above"))
        )
    with pytest.raises(NotFound):
        raise_for_status(_response(404, error_body("not_found", "nope")))


def test_raise_for_status_unknown_code_and_validation_errors() -> None:
    with pytest.raises(FirstError) as exc:
        raise_for_status(_response(418, error_body("teapot", "short and stout")))
    assert type(exc.value) is FirstError
    assert exc.value.status_code == 418

    detail = b'{"detail": [{"loc": ["body", "messages"], "msg": "field required"}]}'
    with pytest.raises(FirstError, match="messages: field required"):
        raise_for_status(_response(422, detail))

    raise_for_status(_response(200, b"{}"))  # no error


# --- SSE ---------------------------------------------------------------------------------


def test_iter_sse_frames_events_and_stops_at_done() -> None:
    body = (
        b": keepalive\n"
        b'event: message_start\ndata: {"a": 1}\n\n'
        b'data: {"b":\ndata: 2}\r\n\r\n'
        b"data: [DONE]\n\n"
        b'data: {"never": true}\n\n'
    )
    events = list(iter_sse(_response(200, body)))
    assert [(e.event, e.json()) for e in events] == [
        ("message_start", {"a": 1}),
        (None, {"b": 2}),
    ]


# --- Text extraction across dialects ----------------------------------------------------


def test_response_text_and_reasoning_per_dialect() -> None:
    chat = {
        "choices": [{"message": {"content": "hi", "reasoning_content": "think"}}],
        "usage": {"prompt_tokens": 1, "completion_tokens": 2},
    }
    assert response_text("chat/completions", chat) == "hi"
    assert response_reasoning("chat/completions", chat) == "think"
    assert response_usage("chat/completions", chat) == {
        "prompt_tokens": 1,
        "completion_tokens": 2,
    }

    responses = {
        "output": [
            {"type": "reasoning", "summary": [{"type": "summary_text", "text": "r"}]},
            {"type": "message", "content": [{"type": "output_text", "text": "hel"}]},
            {"type": "message", "content": [{"type": "output_text", "text": "lo"}]},
        ]
    }
    assert response_text("responses", responses) == "hello"
    assert response_reasoning("responses", responses) == "r"

    messages = {
        "content": [
            {"type": "thinking", "thinking": "hmm"},
            {"type": "text", "text": "yo"},
        ],
        "usage": {"input_tokens": 3, "output_tokens": 4},
    }
    assert response_text("messages", messages) == "yo"
    assert response_reasoning("messages", messages) == "hmm"
    assert response_usage("messages", messages) == {
        "input_tokens": 3,
        "output_tokens": 4,
    }


def test_stream_delta_per_dialect() -> None:
    def event(data: dict[str, object], name: str | None = None):  # type: ignore[no-untyped-def]
        import json

        from alcf_ai import SSEEvent

        return SSEEvent(data=json.dumps(data), event=name)

    assert stream_delta(
        "chat/completions", event({"choices": [{"delta": {"content": "a"}}]})
    ) == ("text", "a")
    assert stream_delta(
        "chat/completions", event({"choices": [{"delta": {"reasoning_content": "b"}}]})
    ) == ("reasoning", "b")
    assert stream_delta("chat/completions", event({"choices": [], "usage": {}})) is None

    assert stream_delta(
        "responses", event({"type": "response.output_text.delta", "delta": "c"})
    ) == ("text", "c")
    assert stream_delta(
        "responses",
        event({"type": "response.reasoning_summary_text.delta", "delta": "d"}),
    ) == ("reasoning", "d")
    assert stream_delta("responses", event({"type": "response.created"})) is None

    assert stream_delta(
        "messages",
        event(
            {
                "type": "content_block_delta",
                "delta": {"type": "text_delta", "text": "e"},
            }
        ),
    ) == ("text", "e")
    assert stream_delta(
        "messages",
        event(
            {
                "type": "content_block_delta",
                "delta": {"type": "thinking_delta", "thinking": "f"},
            }
        ),
    ) == ("reasoning", "f")
    assert stream_delta("messages", event({"type": "message_start"})) is None

    # Terminal Responses event embeds usage under "response".
    assert response_usage(
        "responses", {"type": "response.completed", "response": {"usage": {"x": 1}}}
    ) == {"x": 1}


# --- Inference API -----------------------------------------------------------------------


def test_chat_completions_unary_posts_to_federated_route(
    sdk_client: InferenceClient, gateway: FakeGateway
) -> None:
    gateway.routes["POST /federated/v1/chat/completions"] = httpx.Response(
        200, json={"choices": [{"message": {"content": "hi"}}]}
    )
    body = sdk_client.inference.chat_completions(
        "openai/gpt-oss-120b", [{"role": "user", "content": "yo"}], temperature=0.1
    )
    assert body["choices"][0]["message"]["content"] == "hi"
    assert gateway.requests[-1].extensions["timeout"] == {
        "connect": 10.0,
        "read": 900.0,
        "write": 10.0,
        "pool": 10.0,
    }
    assert gateway.last_json == {
        "model": "openai/gpt-oss-120b",
        "messages": [{"role": "user", "content": "yo"}],
        "temperature": 0.1,
    }


def test_deployment_scoped_routes_use_slug(
    sdk_client: InferenceClient, gateway: FakeGateway
) -> None:
    gateway.routes["POST /deployments/sophia~openai~gpt-oss-120b/v1/messages"] = (
        httpx.Response(200, json={"content": [{"type": "text", "text": "pinned"}]})
    )
    api = sdk_client.deployment("sophia/openai/gpt-oss-120b")
    body = api.messages("openai/gpt-oss-120b", [{"role": "user", "content": "x"}])
    assert response_text("messages", body) == "pinned"
    assert gateway.last_json["max_tokens"] == 1024  # Anthropic requires it


def test_streaming_yields_events_and_sets_stream_flag(
    sdk_client: InferenceClient, gateway: FakeGateway
) -> None:
    gateway.routes["POST /federated/v1/responses"] = httpx.Response(
        200,
        content=sse(
            "response.created",
            {"type": "response.created"},
            "response.output_text.delta",
            {"type": "response.output_text.delta", "delta": "hel"},
            "response.output_text.delta",
            {"type": "response.output_text.delta", "delta": "lo"},
            done=True,
        ),
        headers={"content-type": "text/event-stream"},
    )
    events = list(sdk_client.inference.responses("m", "hi", stream=True))
    assert gateway.requests[-1].extensions["timeout"] == {
        "connect": 10.0,
        "read": 900.0,
        "write": 10.0,
        "pool": 10.0,
    }
    assert gateway.last_json["stream"] is True
    text = "".join(
        d[1] for e in events if (d := stream_delta("responses", e)) is not None
    )
    assert text == "hello"


def test_streaming_error_raises_typed_error(
    sdk_client: InferenceClient, gateway: FakeGateway
) -> None:
    gateway.routes["POST /federated/v1/chat/completions"] = cold_start_response(eta=42)
    with pytest.raises(ServiceUnavailable) as exc:
        list(sdk_client.inference.chat_completions("m", [], stream=True))
    status = StartupStatus.from_error(exc.value)
    assert status is not None and status.startup_eta_sec == 42


def test_task_posts_generic_payload(
    sdk_client: InferenceClient, gateway: FakeGateway
) -> None:
    gateway.routes["POST /federated/v1/tasks"] = httpx.Response(
        200, json={"result": {"scores": [0.9]}}
    )
    result = sdk_client.inference.task("facebook/sam3", "process", data_uri="x")
    assert result == {"result": {"scores": [0.9]}}
    assert gateway.last_json == {
        "model": "facebook/sam3",
        "endpoint": "process",
        "data_uri": "x",
    }


def test_sam3_wrapper_maps_to_task_and_decodes_labelmap(
    sdk_client: InferenceClient, gateway: FakeGateway
) -> None:
    import base64
    import gzip

    labelmap = base64.b64encode(gzip.compress(b"NUMPY-BYTES")).decode()
    gateway.routes["POST /federated/v1/tasks"] = httpx.Response(
        200,
        json={
            "result": {
                "input_image": "x.jpg",
                "prompt": {"text": "cat"},
                "boxes": [[1, 2, 3, 4]],
                "scores": [0.5],
                "labelmap_npy": labelmap,
            }
        },
    )
    result = sdk_client.sam3.submit_image(
        "https://x/y.jpg", "cat", weights_dir_override=Path("/w"), threshold=0.3
    )
    assert result.num_objects == 1 and result.labelmap_bytes() == b"NUMPY-BYTES"
    assert gateway.last_json == {
        "model": "sam3",
        "endpoint": "promptable-segmentation",
        "inference_type": "single-image",
        "data_uri": "https://x/y.jpg",
        "single_image_prompt": "cat",
        "weights_dir_override": "/w",
        "threshold": 0.3,
    }

    gateway.routes["POST /federated/v1/tasks"] = httpx.Response(
        200, json={"result_path": "/staging/out.results.tar"}
    )
    batch = sdk_client.sam3.submit_batch("/staging/in.tar", model="my/sam3")
    assert batch.result_path == "/staging/out.results.tar"
    assert gateway.last_json["model"] == "my/sam3"
    assert gateway.last_json["inference_type"] == "batch"


def test_dinov3_wrapper_maps_to_task(
    sdk_client: InferenceClient, gateway: FakeGateway
) -> None:
    gateway.routes["POST /federated/v1/tasks"] = httpx.Response(
        200, json={"mask_dir": "/staging/out/semantic_masks", "num_images": 3}
    )
    result = sdk_client.dinov3.submit("/staging/in", save_overlay=True, batch_size=8)
    assert result.mask_dir == "/staging/out/semantic_masks" and result.num_images == 3
    assert gateway.last_json == {
        "model": "dinov3",
        "endpoint": "segmentation",
        "input_dir": "/staging/in",
        "save_overlay": True,
        "batch_size": 8,
    }


def test_staging_api(
    sdk_client: InferenceClient, gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch
) -> None:
    from alcf_ai import staging as staging_module
    from alcf_ai.transfer import TransferResult

    gateway.routes["PUT /data/v1/staging"] = httpx.Response(
        200,
        json={
            "collection_id": "coll",
            "path": "/user-staging/u1/",
            "acl_rule_id": "r",
            "principal": "u1",
        },
    )
    calls: list[tuple[str, Any]] = []

    def fake_transfer(**kwargs: Any) -> TransferResult:
        calls.append(("globus", kwargs))
        return TransferResult(
            1.0, kwargs["source_path"], "coll", kwargs["destination_path"]
        )

    def fake_put(local: Path, remote: Path) -> TransferResult:
        calls.append(("https", (local, remote)))
        return TransferResult(1.0, str(local), "coll", remote.as_posix())

    monkeypatch.setattr(staging_module, "run_globus_transfer", fake_transfer)
    monkeypatch.setattr(staging_module, "https_put_to_collection", fake_put)

    result = sdk_client.staging.stage_in(
        Path("/data/imgs"), Path("imgs"), from_collection_id="src", recursive=True
    )
    assert calls[-1][1]["destination_path"] == "/user-staging/u1/imgs"
    assert calls[-1][1]["recursive"] is True
    assert sdk_client.staging.remote_path(result).endswith("/user-staging/u1/imgs")

    with pytest.raises(ValueError, match="requires a from_collection_id"):
        sdk_client.staging.stage_in(Path("/data/imgs"), Path("imgs"), recursive=True)
    with pytest.raises(ValueError, match="relative"):
        sdk_client.staging.stage_in(Path("/data/f"), Path("/abs"))

    sdk_client.staging.stage_out("dst", Path("out"), Path("/local/out"), recursive=True)
    assert calls[-1][1]["source_path"] == "/user-staging/u1/out"
    assert calls[-1][1]["source_collection_id"] == "coll"

    # The staging area is fetched once and cached.
    assert sum(r.url.path == "/data/v1/staging" for r in gateway.requests) == 1


def test_models_listing_and_catalog_lookup(
    sdk_client: InferenceClient, gateway: FakeGateway
) -> None:
    gateway.routes["GET /federated/v1/models"] = httpx.Response(
        200,
        json={
            "data": [
                {"id": "m", "display_name": "M", "max_tokens": 10, "capabilities": {}}
            ]
        },
    )
    (info,) = sdk_client.inference.models()
    assert (info.id, info.max_tokens) == ("m", 10)

    assert sdk_client.catalog.get_model("gpt-oss-120b").name == "openai/gpt-oss-120b"
    with pytest.raises(NotFound):
        sdk_client.catalog.get_model("nope")

    user = sdk_client.whoami()
    assert user.username == "user@anl.gov"


# --- Warm-up ---------------------------------------------------------------------------


def test_warmup_polls_until_ready_and_reports_eta(
    sdk_client: InferenceClient, gateway: FakeGateway
) -> None:
    attempts: list[int] = []

    def chat(request: httpx.Request) -> httpx.Response:
        attempts.append(1)
        if len(attempts) == 1:
            return cold_start_response(eta=None)  # not yet placed
        if len(attempts) == 2:
            return cold_start_response(eta=30)
        if len(attempts) == 3:
            return httpx.Response(
                429,
                content=error_body("too_many_requests", "Requests per minute above 5."),
                headers={"Retry-After": "7"},
            )
        return httpx.Response(200, json={"choices": []})

    gateway.routes["POST /federated/v1/chat/completions"] = chat
    seen: list[StartupStatus] = []
    delays: list[float] = []

    sdk_client.inference.warmup(
        "gpt-oss-120b", on_status=seen.append, sleep=delays.append
    )

    assert len(attempts) == 4
    # The probe used the model's first LLM endpoint with a 1-token request.
    probe = gateway.last_json
    assert probe["model"] == "gpt-oss-120b" and probe["max_tokens"] == 1
    assert [s.startup_eta_sec for s in seen] == [None, 30, None]
    assert [s.cold_start for s in seen] == [True, True, False]
    assert seen[0].typical_startup_sec == 120
    assert delays == [5, 5, 7]


def test_warmup_treats_saturated_503_as_ready(
    sdk_client: InferenceClient, gateway: FakeGateway
) -> None:
    gateway.routes["POST /federated/v1/chat/completions"] = httpx.Response(
        503, content=error_body("service_unavailable", "Backend saturated.")
    )
    sdk_client.inference.warmup("openai/gpt-oss-120b", sleep=lambda _: None)


def test_warmup_times_out(sdk_client: InferenceClient, gateway: FakeGateway) -> None:
    gateway.routes["POST /federated/v1/chat/completions"] = cold_start_response(eta=300)
    with pytest.raises(TimeoutError):
        sdk_client.inference.warmup(
            "openai/gpt-oss-120b", timeout=0, sleep=lambda _: None
        )


def test_warmup_requires_payload_for_task_models(sdk_client: InferenceClient) -> None:
    with pytest.raises(ValueError, match="No default probe"):
        sdk_client.inference.warmup("facebook/sam3")
