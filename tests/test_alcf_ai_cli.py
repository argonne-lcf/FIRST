"""alcf-ai CLI: rendering of catalog tables, chat across dialects, tasks."""

import json
from pathlib import Path
from typing import Any, Callable

import click
import httpx
import pytest
from typer.testing import CliRunner

import alcf_ai.cli as cli_module
from alcf_ai import InferenceClient
from alcf_ai.cli import cli

from .fixtures.alcf_ai import FakeGateway, cold_start_response, pilot_deployment, sse

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
    assert "Your limits" in out and "5,000 tokens/min" in out


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
                **pilot_deployment("sophia/openai/gpt-oss-120b", "openai/gpt-oss-120b"),
                "replica_counts": {"ready": 2, "unhealthy": 1},
            },
        )
    )
    out = run("deployments", "get", "sophia/openai/gpt-oss-120b")
    assert "2 ready, 1 unhealthy" in out

    # Static deployments fall back to the listing when no pilot matches.
    out = run("deployments", "get", "metis/google/gemma-4-31B-it")
    assert "StaticDeployment" in out and "gemma-4-31B-it" in out


def gateway_pilot_detail() -> dict[str, Any]:
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


def test_catalog_models_are_public_views(sdk_client: InferenceClient) -> None:
    from first_common.schema.resources.read import ModelPublic

    models = sdk_client.catalog.models()
    assert models and all(type(m) is ModelPublic for m in models)
    # Operational detail the fake serves never reaches the SDK's models.
    dumped = json.dumps([m.model_dump(mode="json") for m in models])
    assert "api_url" not in dumped and "upstream_model_name" not in dumped


def test_clusters_ls(gateway: FakeGateway) -> None:
    gateway.routes["GET /catalog/v1/clusters"] = httpx.Response(
        200,
        json=[
            {
                "kind": "Cluster",
                "name": "sophia",
                "health": "healthy",
                "maintenance_notice": "Down Tuesday",
            }
        ],
    )
    out = run("clusters", "ls")
    assert "sophia" in out and "Down Tuesday" in out


def test_admin_only_commands_are_not_in_alcf_ai() -> None:
    for args in (["clusters", "get", "sophia"], ["deployments", "logs", "rep"]):
        assert runner.invoke(cli, args).exit_code == 2


@pytest.fixture
def admin_run(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch
) -> Callable[..., str]:
    """Run `first-admin` against the fake as the admin API (full resources)."""
    from first_gateway.admin import cli as admin_cli
    from first_gateway.admin.client import AdminClient

    def fake_client(*_: Any) -> AdminClient:
        client = AdminClient.__new__(AdminClient)
        httpx.Client.__init__(
            client, base_url="http://admin.test", transport=httpx.MockTransport(gateway)
        )
        return client

    monkeypatch.setattr(admin_cli, "AdminClient", fake_client)

    def run_admin(*args: str) -> str:
        result = runner.invoke(admin_cli.cli, list(args), catch_exceptions=False)
        assert result.exit_code == 0, result.output
        return result.output

    return run_admin


def test_admin_catalog_commands_show_full_resources(
    gateway: FakeGateway, admin_run: Callable[..., str]
) -> None:
    out = admin_run("models", "get", "gpt-oss-120b")
    assert "Default limits" in out and "tool_calling" in out

    out = admin_run("models", "get", "google/gemma-4-31B-it")
    assert "Upstream model" in out and "gemma-4-31B-it" in out

    assert admin_run("deployments", "ls", "-q", "metis/*").splitlines() == [
        "metis/google/gemma-4-31B-it"
    ]

    gateway.routes["GET /catalog/v1/deployments/pilot/sophia/openai/gpt-oss-120b"] = (
        httpx.Response(200, json=gateway_pilot_detail())
    )
    out = admin_run("deployments", "get", "sophia/openai/gpt-oss-120b")
    assert "Replicas" in out and "rep-1" in out and "job-1" in out

    out = admin_run("deployments", "get", "metis/google/gemma-4-31B-it")
    assert "StaticDeployment" in out and "In-flight requests" in out

    gateway.routes["GET /catalog/v1/pilot-replicas/a~b/logs"] = httpx.Response(
        200, json="log tail"
    )
    assert "log tail" in admin_run("deployments", "logs", "a/b")

    gateway.routes["GET /catalog/v1/clusters"] = httpx.Response(
        200,
        json=[
            {
                "kind": "Cluster",
                "name": "sophia",
                "uid": 1,
                "created_at": "2026-09-11T12:00:00Z",
                "health": "healthy",
                "health_check": {"url": "https://status.example"},
                "pilot_system": None,
            }
        ],
    )
    assert "static only" in admin_run("clusters", "ls")


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
        "file:///staging/u1/y.jpg",
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
    assert gateway.last_json["data_uri"] == "file:///staging/u1/y.jpg"


def test_sam3_submit_image_cli_rejects_remote_uri(gateway: FakeGateway) -> None:
    # The server only reads the caller's staging area; fail before calling it.
    result = runner.invoke(cli, ["sam3", "submit-image", "https://x/y.jpg", "cat"])
    assert result.exit_code != 0
    assert "remote URIs are not fetched" in result.output
    assert not any(r.url.path.endswith("/tasks") for r in gateway.requests)


def test_sam3_cli_has_no_weights_override() -> None:
    result = runner.invoke(
        cli, ["sam3", "submit-batch", "x.tar", "--weights-dir-override", "/w"]
    )
    assert result.exit_code != 0
    assert "No such option" in result.output


def test_dinov3_cli_bounds_batch_size(tmp_path: Path) -> None:
    result = runner.invoke(
        cli,
        [
            "dinov3",
            "submit",
            str(tmp_path),
            "--from-collection-id",
            "c",
            "--batch-size",
            "65",
        ],
    )
    assert result.exit_code == 2  # click usage error
    # Typer forces a styled terminal under GITHUB_ACTIONS/FORCE_COLOR, so the
    # error panel contains ANSI codes that split up the option name.
    assert "--batch-size" in click.unstyle(result.output)


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


def test_singular_aliases() -> None:
    assert "openai/gpt-oss-120b" in run("model", "ls")
    assert "sophia/openai/gpt-oss-120b" in run("deployment", "ls")


def test_deployments_ls_glob_and_names_only() -> None:
    assert run("deployments", "ls", "-q").splitlines() == [
        "sophia/facebook/sam3",  # pilots first, then statics
        "sophia/openai/gpt-oss-120b",
        "metis/google/gemma-4-31B-it",
    ]
    assert run("deployments", "ls", "-q", "sophia/openai/*").splitlines() == [
        "sophia/openai/gpt-oss-120b"
    ]
    assert run("deployments", "ls", "-q", "metis/*", "*/sam3").splitlines() == [
        "sophia/facebook/sam3",
        "metis/google/gemma-4-31B-it",
    ]


def test_names_not_truncated_in_narrow_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("COLUMNS", "60")
    assert "metis/google/gemma-4-31B-it" in run("deployments", "ls")
    assert "google/gemma-4-31B-it" in run("models", "ls")


def test_chat_unquoted_prompt_and_deployment_only(gateway: FakeGateway) -> None:
    gateway.routes[
        "POST /deployments/sophia~openai~gpt-oss-120b/v1/chat/completions"
    ] = httpx.Response(
        200,
        json={"choices": [{"message": {"content": "hi"}}]},
    )
    out = run(
        "chat",
        "-d",
        "sophia/openai/gpt-oss-120b",
        "--no-stream",
        "what",
        "is",
        "up",
    )
    assert "hi" in out
    assert gateway.last_json["model"] == "openai/gpt-oss-120b"
    assert gateway.last_json["messages"] == [{"role": "user", "content": "what is up"}]


def test_embed_deployment_only(gateway: FakeGateway) -> None:
    gateway.routes["POST /deployments/metis~google~gemma-4-31B-it/v1/embeddings"] = (
        httpx.Response(200, json={"data": []})
    )
    run("embed", "-d", "metis/google/gemma-4-31B-it", "a")
    assert gateway.last_json == {"model": "google/gemma-4-31B-it", "input": "a"}


def test_admin_deployments_wait(
    gateway: FakeGateway,
    admin_run: Callable[..., str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from first_gateway.admin.cli import cli as admin_cli

    monkeypatch.setattr("first_gateway.admin.catalog_cli.time.sleep", lambda _: None)
    detail = gateway_pilot_detail()  # one ready replica
    gateway.routes["GET /catalog/v1/deployments/pilot/sophia/openai/gpt-oss-120b"] = (
        httpx.Response(200, json=detail)
    )
    out = admin_run("deployments", "wait", "sophia/openai/gpt-oss-120b")
    assert "rep-1" in out

    result = runner.invoke(
        admin_cli,
        ["deployments", "wait", "sophia/openai/gpt-oss-120b", "--ready", "2"]
        + ["--timeout", "0"],
    )
    assert result.exit_code == 1
    assert "Timed out with 1 of 2" in result.output


def test_admin_set_desired_replicas_many_and_stdin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from first_common.errors import NotFound
    from first_gateway.admin import cli as admin_cli

    calls: list[tuple[str, int]] = []

    class FakeAdmin:
        base_url = "http://admin"

        def __init__(self, *_: Any) -> None: ...

        def set_desired_pilot_deployment_replicas(self, name: str, n: int) -> None:
            if name == "missing":
                raise NotFound("nope")
            calls.append((name, n))

    monkeypatch.setattr(admin_cli, "AdminClient", FakeAdmin)
    result = runner.invoke(
        admin_cli.cli,
        ["set-desired-replicas", "a", "-", "0"],
        input="b\n\nc\n",
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    assert calls == [("a", 0), ("b", 0), ("c", 0)]

    result = runner.invoke(admin_cli.cli, ["set-desired-replicas", "missing", "d", "2"])
    assert result.exit_code == 1
    assert "missing" in result.output and ("d", 2) in calls
