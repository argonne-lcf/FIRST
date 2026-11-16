"""alcf-ai agent configuration: opencode, pi, codex."""

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import tomlkit
import typer
from typer.testing import CliRunner

import alcf_ai.cli as cli_module
from alcf_ai import InferenceClient
from alcf_ai.agent import (
    _AUTO_APPROVE_OVERWRITES,
    AGENT_ENDPOINTS,
    AgentModel,
    ConfigLoadError,
    ModelCapabilities,
    build_codex_catalog,
    configure_claude,
    configure_codex,
    configure_opencode,
    configure_pi,
    opencode_v2_model_entry,
    pi_model_entry,
    select_models,
    token_helper_command,
)
from alcf_ai.cli import cli
from first_common.schema.resources.read import ModelPublic

from .fixtures.alcf_ai import CATALOG_MODELS

BASE_URL = "http://gateway.test/"


@pytest.fixture(autouse=True)
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("HOME", str(tmp_path))
    return tmp_path


@pytest.fixture(autouse=True)
def approve_direct_overwrites() -> Iterator[None]:
    """Direct configurator tests exercise backups, not interactive prompts."""
    approval = _AUTO_APPROVE_OVERWRITES.set(True)
    try:
        yield
    finally:
        _AUTO_APPROVE_OVERWRITES.reset(approval)


@pytest.fixture(autouse=True)
def opencode_v1(monkeypatch: pytest.MonkeyPatch) -> None:
    # Don't depend on whichever opencode is installed on the test host.
    monkeypatch.setattr("alcf_ai.agent.opencode_version", lambda: "1.18.0")


@pytest.fixture
def models() -> list[AgentModel]:
    return select_models([ModelPublic.model_validate(m) for m in CATALOG_MODELS], [])


def test_select_models_keeps_llms_and_filters_by_name_or_alias() -> None:
    summaries = [ModelPublic.model_validate(m) for m in CATALOG_MODELS]
    assert [m.id for m in select_models(summaries, [])] == [
        "google/gemma-4-31B-it",
        "openai/gpt-oss-120b",
    ]
    assert [m.id for m in select_models(summaries, ["gpt-oss-120b"])] == [
        "openai/gpt-oss-120b"
    ]
    with pytest.raises(ValueError, match="nope"):
        select_models(summaries, ["nope"])


def test_select_models_orders_by_names_and_filters_endpoints() -> None:
    summaries = [ModelPublic.model_validate(m) for m in CATALOG_MODELS]
    assert [
        m.id
        for m in select_models(summaries, ["gpt-oss-120b", "google/gemma-4-31B-it"])
    ] == ["openai/gpt-oss-120b", "google/gemma-4-31B-it"]
    assert [m.id for m in select_models(summaries, [], AGENT_ENDPOINTS["claude"])] == [
        "openai/gpt-oss-120b"
    ]


def test_configure_opencode_merges_into_existing_config(
    home: Path, models: list[AgentModel]
) -> None:
    path = home / ".config" / "opencode" / "opencode.jsonc"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"theme": "dark", "provider": {"other": {}}}))

    assert configure_opencode(BASE_URL, "tok", models) == path
    config = json.loads(path.read_text())
    assert config["theme"] == "dark" and "other" in config["provider"]

    provider = config["provider"]["alcf-inference-service-federated"]
    assert provider["options"] == {
        "baseURL": "http://gateway.test/federated/v1",
        "apiKey": "tok",
        "headers": {"X-ALCF-Session-ID": "{env:ALCF_SESSION_ID}"},
    }
    gpt = provider["models"]["openai/gpt-oss-120b"]
    assert gpt["name"] == "GPT-OSS 120B"
    assert gpt["limit"] == {"context": 65536, "output": 0}
    assert gpt["tool_call"] is True and gpt["reasoning"] is True
    assert gpt["variants"] == {
        "low": {"reasoningEffort": "low"},
        "medium": {"reasoningEffort": "medium"},
        "high": {"reasoningEffort": "high"},
    }
    assert gpt["interleaved"] == {"field": "reasoning"}
    gemma = provider["models"]["google/gemma-4-31B-it"]
    assert gemma["limit"]["context"] == 131072 and "tool_call" not in gemma


HELPER = ["/opt/bin/alcf-tokens", "get-token", "inference"]


def test_configure_opencode_token_helper_installs_plugin(
    home: Path, models: list[AgentModel]
) -> None:
    path = home / ".config" / "opencode" / "opencode.jsonc"
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "provider": {"alcf-inference-service": {}},
                "plugin": ["other-plugin", ["alcf-ai-opencode-plugin", {"old": 1}]],
            }
        )
    )

    configure_opencode(BASE_URL, "tok", models, token_helper=HELPER)
    config = json.loads(path.read_text())
    # The plugin only injects tokens for IDs with this prefix; the bare key an
    # earlier release wrote is dropped.
    assert list(config["provider"]) == ["alcf-inference-service-federated"]
    assert all(p.startswith("alcf-inference-service-") for p in config["provider"])
    options = config["provider"]["alcf-inference-service-federated"]["options"]
    assert options["apiKey"] == "alcf-ai-auto-refresh"
    assert config["plugin"] == [
        "other-plugin",
        ["alcf-ai-opencode-plugin", {"tokenHelper": HELPER}],
    ]

    # Without a helper, the token is embedded and the plugin entry removed.
    configure_opencode(BASE_URL, "tok", models)
    config = json.loads(path.read_text())
    assert (
        config["provider"]["alcf-inference-service-federated"]["options"]["apiKey"]
        == "tok"
    )
    assert config["plugin"] == ["other-plugin"]


def test_configure_pi_token_helper(home: Path, models: list[AgentModel]) -> None:
    helper = ["/path with space/alcf-ai", "auth", "get-access-token"]
    path = configure_pi(BASE_URL, "tok", models, token_helper=helper)
    provider = json.loads(path.read_text())["providers"]["alcf-inference-service"]
    assert provider["apiKey"] == "!'/path with space/alcf-ai' auth get-access-token"


@pytest.mark.parametrize(
    "version, uses_command",
    [("0.118.0", True), ("0.120.0-alpha.1", True), ("0.117.9", False)],
)
def test_configure_codex_token_helper(
    home: Path, models: list[AgentModel], version: str, uses_command: bool
) -> None:
    path = configure_codex(
        BASE_URL,
        "tok",
        models,
        token_helper=HELPER,
        version=version,
        profile=None,
        fetch_catalog=_fake_catalog,
    )
    provider = tomlkit.parse(path.read_text())["model_providers"][
        "alcf-inference-service"
    ]
    if uses_command:
        assert provider["auth"] == {"command": HELPER[0], "args": HELPER[1:]}
        assert "experimental_bearer_token" not in provider
    else:
        assert provider["experimental_bearer_token"] == "tok"
        assert "auth" not in provider


def test_configure_pi(home: Path, models: list[AgentModel]) -> None:
    path = configure_pi(BASE_URL, "tok", models)
    provider = json.loads(path.read_text())["providers"]["alcf-inference-service"]
    assert provider["baseUrl"] == "http://gateway.test/federated/v1"
    assert provider["headers"] == {
        "X-ALCF-Session-ID": "!echo ${ALCF_SESSION_ID:-default}"
    }
    by_id = {m["id"]: m for m in provider["models"]}
    assert by_id["openai/gpt-oss-120b"]["api"] == "openai-responses"
    assert by_id["openai/gpt-oss-120b"]["contextWindow"] == 65536
    assert by_id["openai/gpt-oss-120b"]["thinkingLevelMap"]["high"] == "high"
    assert by_id["openai/gpt-oss-120b"]["thinkingLevelMap"]["max"] is None
    assert by_id["google/gemma-4-31B-it"]["api"] == "openai-completions"


def _fake_catalog(version: str) -> tuple[dict[str, Any], str]:
    return {"models": [{"slug": "gpt-5.4", "supports_parallel_tool_calls": True}]}, (
        f"prompt for {version}"
    )


def test_configure_codex_splits_providers_by_wire_api(
    home: Path, models: list[AgentModel]
) -> None:
    path = configure_codex(
        BASE_URL,
        "tok",
        models,
        version="0.90.0",
        fetch_catalog=_fake_catalog,
        profile=None,
    )
    config = tomlkit.parse(path.read_text())
    providers = config["model_providers"]
    assert providers["alcf-inference-service"]["wire_api"] == "responses"
    assert providers["alcf-inference-service-chat"]["wire_api"] == "chat"
    for provider in providers.values():
        assert provider["env_http_headers"] == {"X-ALCF-Session-ID": "ALCF_SESSION_ID"}
    assert config["model"] == "openai/gpt-oss-120b"
    assert config["model_provider"] == "alcf-inference-service"

    catalog = json.loads(
        (home / ".codex" / "catalogs" / "openai-gpt-oss-120b.json").read_text()
    )
    entry = catalog["models"][0]
    assert entry["slug"] == "openai/gpt-oss-120b"
    assert entry["context_window"] == 65536
    assert entry["base_instructions"] == "prompt for 0.90.0"
    assert entry["supports_parallel_tool_calls"] is False
    # vLLM rejects the Lark-grammar custom tool that "freeform" produces.
    assert entry["apply_patch_tool_type"] is None

    profile = tomlkit.parse(
        (home / ".codex" / "alcf-openai-gpt-oss-120b.config.toml").read_text()
    )
    assert profile["model_provider"] == "alcf-inference-service"
    assert (home / ".codex" / "alcf-google-gemma-4-31B-it.config.toml").exists()


def _model_with_inputs(*modalities: str) -> AgentModel:
    return AgentModel(
        id="m",
        name="M",
        context_window=None,
        endpoints=("responses",),
        capabilities=ModelCapabilities(input_modalities=list(modalities)),
    )


def test_input_modalities_filtered_to_agent_schema() -> None:
    upstream, prompt = _fake_catalog("0.90.0")

    multimodal = _model_with_inputs("text", "image", "audio", "video", "image")
    assert pi_model_entry(multimodal)["input"] == ["text", "image"]
    codex = build_codex_catalog(upstream, prompt, multimodal, "0.90.0")
    assert codex["models"][0]["input_modalities"] == ["text", "image", "audio"]

    video_only = _model_with_inputs("video")
    assert "input" not in pi_model_entry(video_only)
    codex = build_codex_catalog(upstream, prompt, video_only, "0.90.0")
    assert codex["models"][0]["input_modalities"] == ["text"]


def test_configure_codex_new_versions_drop_chat_only_models(
    home: Path, models: list[AgentModel]
) -> None:
    path = home / ".codex" / "config.toml"
    path.parent.mkdir()
    path.write_text('model = "my-model"\nmodel_provider = "mine"\n')

    configure_codex(
        BASE_URL,
        "tok",
        models,
        version="0.100.0",
        fetch_catalog=_fake_catalog,
        profile=None,
    )
    config = tomlkit.parse(path.read_text())
    assert config["model"] == "my-model"  # user's choice preserved
    assert "alcf-inference-service-chat" not in config["model_providers"]
    assert not (home / ".codex" / "alcf-google-gemma-4-31B-it.config.toml").exists()


def test_configure_claude(home: Path) -> None:
    summaries = [ModelPublic.model_validate(m) for m in CATALOG_MODELS]
    models = select_models(summaries, [], AGENT_ENDPOINTS["claude"])
    path = home / ".claude" / "settings.json"
    path.parent.mkdir()
    path.write_text(json.dumps({"theme": "dark", "env": {"FOO": "1"}}))

    assert (
        configure_claude(BASE_URL, "tok", models, token_helper=HELPER, profile=None)
        == path
    )
    config = json.loads(path.read_text())
    assert config["theme"] == "dark"
    assert config["apiKeyHelper"] == "/opt/bin/alcf-tokens get-token inference"
    assert config["env"] == {
        "FOO": "1",
        "ANTHROPIC_BASE_URL": "http://gateway.test/federated",
        "ANTHROPIC_MODEL": "openai/gpt-oss-120b",
        "ANTHROPIC_DEFAULT_OPUS_MODEL": "openai/gpt-oss-120b",
        "ANTHROPIC_DEFAULT_SONNET_MODEL": "openai/gpt-oss-120b",
        "ANTHROPIC_DEFAULT_HAIKU_MODEL": "openai/gpt-oss-120b",
        "CLAUDE_CODE_MAX_CONTEXT_TOKENS": "65536",
    }

    # Without a helper, the token is embedded and the stale helper removed.
    configure_claude(BASE_URL, "tok", models, profile=None)
    config = json.loads(path.read_text())
    assert "apiKeyHelper" not in config
    assert config["env"]["ANTHROPIC_AUTH_TOKEN"] == "tok"


def test_agent_configure_claude_rejects_models_without_messages(
    home: Path, sdk_client: InferenceClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cli_module, "InferenceClient", lambda *_: sdk_client)
    monkeypatch.setattr(
        "alcf_ai.commands.agent.get_access_token", lambda _service: "tok"
    )

    result = CliRunner().invoke(
        cli, ["agent", "configure", "claude", "-m", "google/gemma-4-31B-it"]
    )
    assert result.exit_code != 0
    assert "No models serving messages" in result.output
    assert not (home / ".claude" / "settings.json").exists()


def _executable(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/sh\n")
    path.chmod(0o755)
    return path


def test_token_helper_skips_uv_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache = tmp_path / "uv-cache"
    cached = _executable(cache / "archive-v0" / "abc" / "bin" / "alcf-tokens")
    durable = _executable(tmp_path / "bin" / "alcf-ai")
    monkeypatch.setenv("UV_CACHE_DIR", str(cache))
    monkeypatch.setenv("PATH", f"{cached.parent}:{durable.parent}")

    assert token_helper_command() == [str(durable), "auth", "get-access-token"]

    monkeypatch.setenv("PATH", str(cached.parent))
    assert token_helper_command() is None


def test_agent_configure_cli(
    home: Path, sdk_client: InferenceClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cli_module, "InferenceClient", lambda *_: sdk_client)
    monkeypatch.setattr(
        "alcf_ai.commands.agent.get_access_token", lambda _service: "tok"
    )
    monkeypatch.setattr("alcf_ai.commands.agent.token_helper_command", lambda: HELPER)
    monkeypatch.setenv("COLUMNS", "200")

    result = CliRunner().invoke(
        cli,
        ["agent", "configure", "opencode", "-m", "gpt-oss-120b"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    assert "openai/gpt-oss-120b" in result.output and "gemma" not in result.output
    assert "refresh access tokens automatically" in result.output
    config = json.loads((home / ".config/opencode/opencode.jsonc").read_text())
    assert list(config["provider"]["alcf-inference-service-federated"]["models"]) == [
        "openai/gpt-oss-120b"
    ]
    assert config["plugin"] == [["alcf-ai-opencode-plugin", {"tokenHelper": HELPER}]]


def test_configure_opencode_v2_schema(home: Path, models: list[AgentModel]) -> None:
    path = home / ".config" / "opencode" / "opencode.jsonc"
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "provider": {"alcf-inference-service": {}, "other": {}},
                "plugin": ["alcf-ai-opencode-plugin@0.1.0"],
                "plugins": [{"package": "alcf-ai-opencode-plugin", "options": {}}],
            }
        )
    )
    cached = home / ".cache" / "opencode" / "packages" / "alcf-ai-opencode-plugin@0.1"
    cached.mkdir(parents=True)

    configure_opencode(BASE_URL, "tok", models, token_helper=HELPER, version="2.0.1")
    config = json.loads(path.read_text())
    assert config["provider"] == {"other": {}}
    assert "plugin" not in config
    assert config["plugins"] == [
        {"package": "alcf-ai-opencode-plugin", "options": {"tokenHelper": HELPER}}
    ]
    provider = config["providers"]["alcf-inference-service-federated"]
    assert provider["package"] == "@opencode/ai/providers/openai-compatible"
    assert provider["settings"]["baseURL"] == "http://gateway.test/federated/v1"
    assert provider["settings"]["apiKey"] == "alcf-ai-auto-refresh"
    assert provider["headers"] == {"X-ALCF-Session-ID": "{env:ALCF_SESSION_ID}"}
    assert set(provider["models"]) == {"openai/gpt-oss-120b", "google/gemma-4-31B-it"}
    assert not cached.exists()


def test_opencode_v2_model_entry() -> None:
    model = AgentModel(
        id="m",
        name="M",
        context_window=1000,
        endpoints=("responses",),
        capabilities=ModelCapabilities.model_validate(
            {
                "input_modalities": ["text", "image"],
                "tool_calling": {"supported": True},
                "reasoning": {
                    "supported": True,
                    "effort_levels": ["low", "high"],
                    "default_effort": "high",
                    "separate_output": True,
                },
            }
        ),
    )
    assert opencode_v2_model_entry(model) == {
        "name": "M",
        "capabilities": {
            "tools": True,
            "input": ["text", "image"],
            "reasoning": {"supported": True, "effort": ["low", "high"]},
        },
        "settings": {"reasoningEffort": "high"},
        "compatibility": {"reasoningField": "reasoning"},
        "limit": {"context": 1000},
    }


def test_configure_backs_up_changed_config(
    home: Path, models: list[AgentModel]
) -> None:
    path = home / ".pi" / "agent" / "models.json"
    path.parent.mkdir(parents=True)
    path.write_text('{"providers": {}}')

    configure_pi(BASE_URL, "tok", models)
    backups = list(path.parent.glob("models.json.bak.*"))
    assert [b.read_text() for b in backups] == ['{"providers": {}}']

    # An unchanged rewrite leaves no further backup.
    configure_pi(BASE_URL, "tok", models)
    assert list(path.parent.glob("models.json.bak.*")) == backups


def test_configure_writes_through_symlinks(
    home: Path, models: list[AgentModel]
) -> None:
    real = home / "dotfiles" / "models.json"
    real.parent.mkdir()
    real.write_text("{}")
    link = home / ".pi" / "agent" / "models.json"
    link.parent.mkdir(parents=True)
    link.symlink_to(real)

    configure_pi(BASE_URL, "tok", models)
    assert link.is_symlink()
    assert "alcf-inference-service" in json.loads(real.read_text())["providers"]


@pytest.mark.parametrize("content", ["{not json", "[1, 2]"])
def test_agent_configure_refuses_unparseable_config(
    home: Path,
    sdk_client: InferenceClient,
    monkeypatch: pytest.MonkeyPatch,
    content: str,
) -> None:
    monkeypatch.setattr(cli_module, "InferenceClient", lambda *_: sdk_client)
    monkeypatch.setattr(
        "alcf_ai.commands.agent.get_access_token", lambda _service: "tok"
    )
    path = home / ".pi" / "agent" / "models.json"
    path.parent.mkdir(parents=True)
    path.write_text(content)

    result = CliRunner().invoke(cli, ["agent", "configure", "pi"])
    assert result.exit_code == 1
    assert "Refusing to modify" in result.output
    assert path.read_text() == content


def test_configure_codex_refuses_unparseable_config(
    home: Path, models: list[AgentModel]
) -> None:
    path = home / ".codex" / "config.toml"
    path.parent.mkdir()
    path.write_text("model = [")
    with pytest.raises(ConfigLoadError):
        configure_codex(
            BASE_URL,
            "tok",
            models,
            version="0.120.0",
            fetch_catalog=_fake_catalog,
            profile=None,
        )
    assert path.read_text() == "model = ["


def _plain_output(output: str) -> str:
    """Remove Rich panel wrapping before matching long CLI diagnostics."""
    return " ".join(output.replace("│", " ").split())


def _prepare_agent_cli(
    sdk_client: InferenceClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cli_module, "InferenceClient", lambda *_: sdk_client)
    monkeypatch.setattr(
        "alcf_ai.commands.agent.get_access_token", lambda _service: "tok"
    )
    monkeypatch.setattr("alcf_ai.commands.agent.token_helper_command", lambda: HELPER)


def _capture_exec(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    def fake_execvpe(file: str, args: list[str], env: dict[str, str]) -> None:
        calls.append({"file": file, "args": args, "env": env})

    monkeypatch.setattr("alcf_ai.commands.agent.os.execvpe", fake_execvpe)
    return calls


def test_configure_codex_default_profile_and_version_floor(
    home: Path, models: list[AgentModel]
) -> None:
    path = configure_codex(
        BASE_URL, "tok", models, version="0.134.0", fetch_catalog=_fake_catalog
    )

    assert path == home / ".codex" / "alcf.config.toml"
    assert not (home / ".codex" / "config.toml").exists()
    config = tomlkit.parse(path.read_text())
    assert config["model"] == "openai/gpt-oss-120b"
    assert config["model_provider"] == "alcf-inference-service"
    assert config["model_catalog_json"].endswith("openai-gpt-oss-120b.json")

    model_profile = tomlkit.parse(
        (home / ".codex" / "alcf-openai-gpt-oss-120b.config.toml").read_text()
    )
    provider = model_profile["model_providers"]["alcf-inference-service"]
    assert provider["base_url"] == "http://gateway.test/federated/v1"

    with pytest.raises(typer.BadParameter, match="external profiles"):
        configure_codex(
            BASE_URL,
            "tok",
            models,
            version="0.133.0",
            fetch_catalog=_fake_catalog,
        )


def test_agent_configure_claude_profile_defaults_and_opt_out(
    home: Path, sdk_client: InferenceClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _prepare_agent_cli(sdk_client, monkeypatch)
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["agent", "configure", "claude", "-m", "gpt-oss-120b"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    assert (home / ".claude" / "profiles" / "alcf.json").is_file()
    assert not (home / ".claude" / "settings.json").exists()
    assert "alcf-ai agent run claude --profile alcf" in result.output

    result = runner.invoke(
        cli,
        ["agent", "configure", "claude", "--no-profile", "-m", "gpt-oss-120b"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    assert (home / ".claude" / "settings.json").is_file()

    result = runner.invoke(
        cli,
        ["agent", "configure", "claude", "--profile", "other", "--no-profile"],
    )
    assert result.exit_code == 2
    assert "--no-profile cannot be combined with --profile" in _plain_output(
        result.output
    )


def test_agent_configure_prompts_before_overwrite_and_yes_bypasses(
    home: Path, sdk_client: InferenceClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _prepare_agent_cli(sdk_client, monkeypatch)
    path = home / ".config" / "opencode" / "opencode.jsonc"
    path.parent.mkdir(parents=True)
    original = json.dumps({"theme": "dark"})
    path.write_text(original)
    runner = CliRunner()

    result = runner.invoke(
        cli, ["agent", "configure", "opencode", "-m", "gpt-oss-120b"]
    )
    assert result.exit_code == 1
    assert "Overwrite" in result.output
    assert path.read_text() == original
    assert not list(path.parent.glob("opencode.jsonc.bak.*"))

    result = runner.invoke(
        cli,
        ["agent", "configure", "opencode", "-m", "gpt-oss-120b", "--yes"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    assert path.read_text() != original
    backups = list(path.parent.glob("opencode.jsonc.bak.*"))
    assert len(backups) == 1
    assert backups[0].read_text() == original


def test_agent_run_opencode_uses_isolated_federated_config(
    home: Path, sdk_client: InferenceClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _prepare_agent_cli(sdk_client, monkeypatch)
    calls = _capture_exec(monkeypatch)

    result = CliRunner().invoke(
        cli,
        ["agent", "run", "opencode", "-m", "gpt-oss-120b", "--", "--help"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    assert len(calls) == 1
    assert calls[0]["file"] == "opencode"
    assert calls[0]["args"] == ["opencode", "--help"]
    config = json.loads(calls[0]["env"]["OPENCODE_CONFIG_CONTENT"])
    assert config["model"] == ("alcf-inference-service-federated/openai/gpt-oss-120b")
    provider = config["provider"]["alcf-inference-service-federated"]
    assert provider["options"]["baseURL"] == "http://gateway.test/federated/v1"
    assert calls[0]["env"]["ALCF_SESSION_ID"] == "default"
    assert not (home / ".config" / "opencode" / "opencode.jsonc").exists()


def test_agent_run_codex_one_shot_and_responses_requirement(
    home: Path, sdk_client: InferenceClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _prepare_agent_cli(sdk_client, monkeypatch)
    monkeypatch.setattr("alcf_ai.commands.agent.codex_version", lambda: "0.134.0")
    calls = _capture_exec(monkeypatch)
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["agent", "run", "codex", "-m", "gpt-oss-120b", "--", "--help"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    assert len(calls) == 1
    assert calls[0]["file"] == "codex"
    assert calls[0]["args"][1:3] == ["--model", "openai/gpt-oss-120b"]
    assert calls[0]["args"][-1] == "--help"
    joined_args = "\n".join(calls[0]["args"])
    assert 'model_provider="alcf-inference-service"' in joined_args
    assert (
        "model_providers.alcf-inference-service.base_url="
        '"http://gateway.test/federated/v1"'
    ) in joined_args
    assert calls[0]["env"]["ALCF_API_KEY"] == "tok"
    assert not (home / ".codex").exists()

    result = runner.invoke(
        cli, ["agent", "run", "codex", "-m", "google/gemma-4-31B-it"]
    )
    assert result.exit_code == 2
    assert "does not support the Responses API" in _plain_output(result.output)
    assert len(calls) == 1


def test_agent_run_codex_profile_checks_version_and_forwards_no_model(
    home: Path, sdk_client: InferenceClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _prepare_agent_cli(sdk_client, monkeypatch)
    calls = _capture_exec(monkeypatch)
    profile = home / ".codex" / "alcf.config.toml"
    profile.parent.mkdir()
    profile.write_text("")
    runner = CliRunner()

    monkeypatch.setattr("alcf_ai.commands.agent.codex_version", lambda: "0.133.0")
    result = runner.invoke(cli, ["agent", "run", "codex", "--profile", "alcf"])
    assert result.exit_code == 2
    assert "does not support external profiles" in _plain_output(result.output)
    assert not calls

    monkeypatch.setattr("alcf_ai.commands.agent.codex_version", lambda: "0.134.0")
    result = runner.invoke(
        cli, ["agent", "run", "codex", "--profile", "alcf"], catch_exceptions=False
    )
    assert result.exit_code == 0, result.output
    assert calls[0]["args"] == ["codex", "--profile", "alcf"]


def test_agent_run_claude_profile_and_model_conflict(
    home: Path, sdk_client: InferenceClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _prepare_agent_cli(sdk_client, monkeypatch)
    calls = _capture_exec(monkeypatch)
    profile = home / ".claude" / "profiles" / "research.json"
    profile.parent.mkdir(parents=True)
    profile.write_text("{}")
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["agent", "run", "claude", "--profile", "research", "--", "--verbose"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    assert calls[0]["args"] == [
        "claude",
        "--settings",
        str(profile),
        "--verbose",
    ]

    result = runner.invoke(
        cli,
        [
            "agent",
            "run",
            "claude",
            "--profile",
            "research",
            "-m",
            "gpt-oss-120b",
        ],
    )
    assert result.exit_code == 2
    assert "--profile cannot be combined with --default-model" in _plain_output(
        result.output
    )
    assert len(calls) == 1


def test_agent_run_aider_uses_federated_environment(
    sdk_client: InferenceClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _prepare_agent_cli(sdk_client, monkeypatch)
    calls = _capture_exec(monkeypatch)

    result = CliRunner().invoke(
        cli,
        ["agent", "run", "aider", "-m", "gpt-oss-120b", "--", "--yes-always"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    assert calls[0]["args"] == ["aider", "--yes-always"]
    assert calls[0]["env"]["OPENAI_API_BASE"] == "http://gateway.test/federated/v1"
    assert calls[0]["env"]["OPENAI_API_KEY"] == "tok"
    assert calls[0]["env"]["AIDER_MODEL"] == "openai/openai/gpt-oss-120b"
