"""alcf-ai CLI: rendering of catalog tables, chat across dialects, tasks."""

import json
from pathlib import Path
from typing import Any

import httpx
import pytest
from typer.testing import CliRunner

import alcf_ai.cli as cli_module
from alcf_ai import InferenceClient
from alcf_ai.cli import cli

from .fixtures.alcf_ai import FakeGateway, cold_start_response, sse

runner = CliRunner()


@pytest.fixture(autouse=True)
def cli_client(
    sdk_client: InferenceClient, monkeypatch: pytest.MonkeyPatch
) -> InferenceClient:
    """Make the CLI's root callback hand out the mock-backed client."""
    monkeypatch.setattr(cli_module, "InferenceClient", lambda *_: sdk_client)
    monkeypatch.setenv("COLUMNS", "250")
    monkeypatch.delenv("ALCF_AI_MODEL", raising=False)
    return sdk_client


def run(*args: str, input: str | None = None) -> str:
    result = runner.invoke(cli, list(args), input=input, catch_exceptions=False)
    assert result.exit_code == 0, result.output
    return result.output


def test_models_ls_table_and_json() -> None:
    out = run("models", "ls")
    assert "openai/gpt-oss-120b" in out
    assert "gpt-oss-120b" in out  # alias shown under the name
    assert "chat, responses, messages" in out
    assert "65,536" in out
    assert "ready" in out and "offline" in out
    assert "1/1" in out

    data = json.loads(run("models", "ls", "--json"))
    assert [m["name"] for m in data] == [
        "openai/gpt-oss-120b",
        "google/gemma-4-31B-it",
        "facebook/sam3",
    ]

    out = run("models", "ls", "--endpoint", "/responses")
    assert "gpt-oss-120b" in out and "gemma" not in out


def test_models_get_shows_capabilities_and_deployments() -> None:
    out = run("models", "get", "gpt-oss-120b")
    assert "GPT-OSS 120B" in out
    assert "tool_calling" in out
    assert "sophia/openai/gpt-oss-120b" in out
    assert "healthy" in out
    assert "tokens/min" in out


def test_deployments_ls_and_get(gateway: FakeGateway) -> None:
    out = run("deployments", "ls")
    assert "sophia/openai/gpt-oss-120b" in out
    assert "metis/google/gemma-4-31B-it" in out
    assert "pilot" in out and "static" in out

    out = run("deployments", "ls", "--model", "google/gemma-4-31B-it")
    assert "sophia" not in out and "metis" in out

    gateway.routes["GET /catalog/v1/deployments/pilot/sophia/openai/gpt-oss-120b"] = (
        httpx.Response(
            200,
            json={
                **gateway_pilot_detail(),
            },
        )
    )
    out = run("deployments", "get", "sophia/openai/gpt-oss-120b")
    assert "Replicas" in out and "rep-1" in out and "ready" in out

    # Static deployments fall back to the listing when no pilot matches.
    out = run("deployments", "get", "metis/google/gemma-4-31B-it")
    assert "StaticDeployment" in out and "gemma-4-31B-it" in out


def gateway_pilot_detail() -> dict[str, Any]:
    from .fixtures.alcf_ai import pilot_deployment

    return {
        **pilot_deployment("sophia/openai/gpt-oss-120b", "openai/gpt-oss-120b"),
        "launch_template_name": "vllm",
        "launch_spec": {"served_model_name": "x", "gpus_per_node": 4, "num_nodes": 1},
        "replicas": [
            {
                "kind": "PilotReplica",
                "name": "rep-1",
                "uid": 100,
                "created_at": "2026-09-11T12:00:00Z",
                "pilot_deployment_name": "sophia/openai/gpt-oss-120b",
                "pilot_job_name": "job-1",
                "claimed_gpu_ids": [],
                "resources": [{"hostname": "n1", "gpu_ids": ["0", "1"]}],
                "model_url": "http://n1:8000",
                "state": "ready",
                "state_message": "Serving.",
                "runtime": {"inflight": 1},
            }
        ],
    }


def test_whoami() -> None:
    out = run("whoami")
    assert "user@anl.gov" in out and "Argonne" in out


def test_chat_streams_chat_completions_by_default(gateway: FakeGateway) -> None:
    gateway.routes["POST /federated/v1/chat/completions"] = httpx.Response(
        200,
        content=sse(
            {"choices": [{"delta": {"reasoning_content": "thinking..."}}]},
            {"choices": [{"delta": {"content": "Hello"}}]},
            {"choices": [{"delta": {"content": " world"}}]},
            {"choices": [], "usage": {"prompt_tokens": 5, "completion_tokens": 2}},
            done=True,
        ),
        headers={"content-type": "text/event-stream"},
    )
    out = run("chat", "-m", "gpt-oss-120b", "-s", "be brief", "say hi")
    assert "Hello world" in out
    assert "thinking..." not in out
    assert gateway.last_json["messages"] == [
        {"role": "system", "content": "be brief"},
        {"role": "user", "content": "say hi"},
    ]
    assert gateway.last_json["stream"] is True

    out = run("chat", "-m", "gpt-oss-120b", "--show-reasoning", "say hi")
    assert "thinking..." in out


def test_chat_auto_selects_model_and_dialect(gateway: FakeGateway) -> None:
    # gemma only supports chat/completions and sorts after facebook/sam3 (no chat).
    gateway.routes["POST /federated/v1/chat/completions"] = httpx.Response(
        200, json={"choices": [{"message": {"content": "**bold** answer"}}]}
    )
    out = run("chat", "--no-stream", "hello", input="")
    assert "bold" in out and "answer" in out
    assert gateway.last_json["model"] == "google/gemma-4-31B-it"


def test_chat_messages_dialect_and_json(gateway: FakeGateway) -> None:
    gateway.routes["POST /federated/v1/messages"] = httpx.Response(
        200, json={"content": [{"type": "text", "text": "yo"}], "usage": {}}
    )
    out = run(
        "chat",
        "-m",
        "openai/gpt-oss-120b",
        "--api",
        "messages",
        "-n",
        "5",
        "-s",
        "sys",
        "--json",
        "hi",
    )
    assert json.loads(out)["content"][0]["text"] == "yo"
    assert gateway.last_json == {
        "model": "openai/gpt-oss-120b",
        "messages": [{"role": "user", "content": "hi"}],
        "system": "sys",
        "max_tokens": 5,
    }


def test_chat_responses_dialect_with_stdin_and_deployment(gateway: FakeGateway) -> None:
    gateway.routes["POST /deployments/sophia~openai~gpt-oss-120b/v1/responses"] = (
        httpx.Response(
            200,
            content=sse(
                {"type": "response.output_text.delta", "delta": "ok"},
                {
                    "type": "response.completed",
                    "response": {"usage": {"input_tokens": 1}},
                },
            ),
            headers={"content-type": "text/event-stream"},
        )
    )
    out = run(
        "chat",
        "-m",
        "openai/gpt-oss-120b",
        "--api",
        "responses",
        "-d",
        "sophia/openai/gpt-oss-120b",
        "-t",
        "0.5",
        "fix this",
        input="piped text\n",
    )
    assert "ok" in out
    assert gateway.last_json["input"] == "piped text\n\nfix this"
    assert gateway.last_json["temperature"] == 0.5


def test_chat_waits_through_cold_start(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("alcf_ai.commands._shared.countdown", lambda *_: None)
    attempts: list[int] = []

    def chat(request: httpx.Request) -> httpx.Response:
        attempts.append(1)
        if len(attempts) < 3:
            return cold_start_response(eta=20)
        return httpx.Response(200, json={"choices": [{"message": {"content": "warm"}}]})

    gateway.routes["POST /federated/v1/chat/completions"] = chat
    out = run("chat", "-m", "gpt-oss-120b", "--api", "chat", "--no-stream", "hi")
    assert "warm" in out and len(attempts) == 3

    attempts.clear()
    result = runner.invoke(
        cli, ["chat", "-m", "gpt-oss-120b", "--api", "chat", "--no-wait", "hi"]
    )
    assert result.exit_code != 0 and len(attempts) == 1


def test_models_warmup(gateway: FakeGateway) -> None:
    attempts: list[int] = []

    def chat(request: httpx.Request) -> httpx.Response:
        attempts.append(1)
        if len(attempts) == 1:
            resp = cold_start_response(eta=1)
            resp.headers["Retry-After"] = "1"
            return resp
        return httpx.Response(200, json={"choices": []})

    gateway.routes["POST /federated/v1/chat/completions"] = chat
    out = run("models", "warmup", "gpt-oss-120b")
    assert "is ready" in out and len(attempts) == 2


def test_embed(gateway: FakeGateway) -> None:
    gateway.routes["POST /federated/v1/embeddings"] = httpx.Response(
        200, json={"data": [{"embedding": [0.1, 0.2]}]}
    )
    out = run("embed", "-m", "emb", "a", "b")
    assert json.loads(out)["data"][0]["embedding"] == [0.1, 0.2]
    assert gateway.last_json == {"model": "emb", "input": ["a", "b"]}


def test_tasks_ls_and_run(gateway: FakeGateway) -> None:
    out = run("tasks", "ls")
    assert "facebook/sam3" in out and "process" in out and "gemma" not in out

    gateway.routes["POST /federated/v1/tasks"] = httpx.Response(
        200, json={"result": {"boxes": []}}
    )
    out = run(
        "tasks",
        "run",
        "facebook/sam3",
        "process",
        "-p",
        "data_uri=https://x/y.jpg",
        "-p",
        "threshold=0.5",
        "--json",
        '{"inference_type": "single-image"}',
    )
    assert json.loads(out) == {"result": {"boxes": []}}
    assert gateway.last_json == {
        "model": "facebook/sam3",
        "endpoint": "process",
        "inference_type": "single-image",
        "data_uri": "https://x/y.jpg",
        "threshold": 0.5,
    }


def test_error_panel_on_gateway_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """main() turns FirstErrors into a clean panel and a non-zero exit."""
    import sys

    monkeypatch.setattr(sys, "argv", ["alcf-ai", "models", "get", "nope"])
    with pytest.raises(SystemExit) as exc:
        cli_module.main()
    assert exc.value.code == 1


def test_sam3_submit_image_cli(gateway: FakeGateway, tmp_path: Path) -> None:
    import base64
    import gzip

    gateway.routes["POST /federated/v1/tasks"] = httpx.Response(
        200,
        json={
            "input_image": "x.jpg",
            "prompt": {"text": "Baguette"},
            "boxes": [[10, 20, 30, 40]],
            "scores": [0.91],
            "labelmap_npy": base64.b64encode(gzip.compress(b"LABELS")).decode(),
        },
    )
    labels = tmp_path / "labels.npy"
    result_json = tmp_path / "result.json"
    out = run(
        "sam3",
        "submit-image",
        "https://x/y.jpg",
        "Baguette",
        "--save-labelmap",
        str(labels),
        "-o",
        str(result_json),
    )
    assert "1 object(s)" in out and "0.910" in out
    assert labels.read_bytes() == b"LABELS"
    assert json.loads(result_json.read_text())["scores"] == [0.91]
    assert gateway.last_json["endpoint"] == "promptable-segmentation"
    assert gateway.last_json["data_uri"] == "https://x/y.jpg"


def test_dinov3_submit_cli_stages_in_and_out(
    gateway: FakeGateway, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
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
    gateway.routes["POST /federated/v1/tasks"] = httpx.Response(
        200, json={"mask_dir": "/user-staging/u1/imgs-results/semantic_masks"}
    )
    transfers: list[dict[str, Any]] = []

    def fake_transfer(**kwargs: Any) -> TransferResult:
        transfers.append(kwargs)
        return TransferResult(
            1.0, kwargs["source_path"], "coll", kwargs["destination_path"]
        )

    monkeypatch.setattr(staging_module, "run_globus_transfer", fake_transfer)
    input_dir = tmp_path / "imgs"
    input_dir.mkdir()

    out = run(
        "dinov3",
        "submit",
        str(input_dir),
        "--from-collection-id",
        "src",
        "--save-overlay",
        "--batch-size",
        "4",
    )
    assert json.loads(out)["mask_dir"].endswith("semantic_masks")
    assert gateway.last_json == {
        "model": "dinov3",
        "endpoint": "segmentation",
        "input_dir": "/eagle/IRIBeta/inference_data_staging/user-staging/u1/imgs",
        "save_overlay": True,
        "batch_size": 4,
    }
    assert [t["recursive"] for t in transfers] == [True, True]
    assert transfers[1]["source_path"] == "/user-staging/u1/imgs-results"
    assert transfers[1]["destination_path"] == str(tmp_path / "imgs-results")
