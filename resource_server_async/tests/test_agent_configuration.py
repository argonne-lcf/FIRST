"""Agent configuration regression tests: no network, credentials, or real home."""

import copy
import importlib
import json
import os
import tempfile
import tomllib
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from click.testing import Result
from httpx import URL
from typer.testing import CliRunner

from alcf_ai import agent

MODEL = "mistral-large-4-nvfp4"
PROVIDER = "alcf-inference-service-minerva-api"
BASE_URL = URL("https://inference-api.alcf.anl.gov/resource_server/")
HELPER = ["/test/bin/alcf-tokens", "get-token", "inference"]
TOKEN = "test-token-not-a-real-credential"
MISTRAL = {
    "id": MODEL,
    "display_name": "Mistral Large 4 NVFP4",
    "framework": "api",
    "capabilities": {
        "schema_version": 1,
        "api_protocols": ["chat_completions", "responses", "messages"],
        "context_window_tokens": 1048576,
        "input_modalities": ["text", "image"],
        "streaming": True,
        "tool_calling": {"supported": True},
        "reasoning": {
            "supported": True,
            "effort_levels": ["low", "medium", "high"],
            "default_effort": "high",
            "separate_output": True,
        },
    },
}
MUSE = {
    "id": "muse-spark-bf16",
    "display_name": "Muse Spark Open BF16",
    "framework": "api",
    "capabilities": {"context_window_tokens": 1048576},
}

# Catalog fetches are mocked. Unknown upstream fields must survive adaptation.
UPSTREAM = {
    "models": [
        {
            "slug": "gpt-5.4",
            "upstream_schema_field": {"preserve": True},
            "supports_parallel_tool_calls": True,
        }
    ]
}


class AgentConfigurationTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = self.enterContext(
            tempfile.TemporaryDirectory(prefix="agent-config-test-")
        )
        self.home = Path(tmp)
        self.enterContext(patch.object(Path, "home", return_value=self.home))
        self.enterContext(
            patch.dict(
                os.environ,
                {
                    "HOME": tmp,
                    "XDG_CONFIG_HOME": str(self.home / ".config"),
                    "XDG_CACHE_HOME": str(self.home / ".cache"),
                    "CODEX_HOME": str(self.home / ".codex"),
                },
            )
        )
        self.helper = self.enterContext(
            patch.object(agent, "_token_helper_command", return_value=HELPER)
        )
        self.enterContext(patch.object(agent, "_codex_version", return_value="0.149.1"))
        self.catalog_fetch = self.enterContext(
            patch.object(
                agent,
                "_fetch_catalog_and_prompt",
                return_value=(
                    json.dumps(UPSTREAM),
                    "Version-matched test instructions",
                ),
            )
        )
        self.enterContext(patch.object(agent, "get_access_token", return_value=TOKEN))
        # An accidental remote call must fail instead of reaching FIRST or upstream.
        self.enterContext(
            patch("httpx.Client.send", side_effect=AssertionError("Unexpected network"))
        )
        self.infos = {"minerva": [copy.deepcopy(MISTRAL)]}

    def write(self, relative: str, content: str) -> Path:
        path = self.home / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        return path

    def read_json(self, relative: str) -> dict:
        return json.loads((self.home / relative).read_text())

    def read_toml(self, relative: str) -> dict:
        return tomllib.loads((self.home / relative).read_text())

    def invoke_configure(
        self, name: str, *options: str, input: str | None = None
    ) -> Result:
        cli_module = importlib.import_module("alcf_ai.cli")
        client = Mock(base_url=BASE_URL)
        listings = {
            "sophia": [{"id": "openai/gpt-oss-120b", "framework": "vllm"}],
            "minerva": [
                copy.deepcopy(MISTRAL),
                {"id": "nemotron-3-ultra", "framework": "api"},
                {"id": "unlisted-model", "framework": "api"},
            ],
            "tara": [
                {"id": "nemotron-3-ultra", "framework": "api"},
                {"id": "nemotron-35-super-vl", "framework": "api"},
            ],
        }
        client.list_models.side_effect = listings.__getitem__
        with patch.dict(cli_module._cli_state, {"client": client}):
            return CliRunner().invoke(
                agent.cli, ["configure", name, *options], input=input
            )

    def configure(self, name: str, *options: str) -> Result:
        result = self.invoke_configure(name, "--yes", *options)
        self.assertEqual(result.exit_code, 0, result.output or str(result.exception))
        return result

    def run_agent(self, name: str, *options: str) -> Result:
        cli_module = importlib.import_module("alcf_ai.cli")
        client = Mock(base_url=BASE_URL)
        client.list_models.return_value = [
            copy.deepcopy(MISTRAL),
            copy.deepcopy(MUSE),
        ]
        with patch.dict(cli_module._cli_state, {"client": client}):
            return CliRunner().invoke(agent.cli, ["run", name, *options])

    def run_claude(self, *options: str) -> Result:
        return self.run_agent("claude", *options)

    def test_allowlist_changes_only_minerva_nemotron(self) -> None:
        self.assertEqual(
            agent.ALLOWLIST,
            {
                "sophia": ["openai/gpt-oss-120b"],
                "minerva": [MODEL, "inkling-bf16", "muse-spark-bf16", "beam-1.0-bf16"],
                "tara": [
                    "openai/gpt-oss-20b",
                    "nemotron-35-super-vl",
                    "inkling-bf16",
                    "nemotron-3-ultra",
                ],
            },
        )

    def test_cli_filters_retired_minerva_but_preserves_tara_nemotron(self) -> None:
        with patch.object(agent, "edit_pi") as edit:
            self.configure("pi")
        infos = edit.call_args.args[2]
        self.assertEqual([m["id"] for m in infos["minerva"]], [MODEL])
        self.assertEqual(
            [m["id"] for m in infos["tara"]],
            ["nemotron-3-ultra", "nemotron-35-super-vl"],
        )
        self.assertEqual(infos["sophia"][0]["id"], "openai/gpt-oss-120b")

    def test_include_experimental_still_opts_in_to_unlisted_models(self) -> None:
        with patch.object(agent, "edit_pi") as edit:
            self.configure("pi", "--include-experimental")
        self.assertIn(
            "unlisted-model", [m["id"] for m in edit.call_args.args[2]["minerva"]]
        )

    def test_opencode_v1_uses_published_capabilities_and_refresh_plugin(self) -> None:
        relative = ".config/opencode/opencode.jsonc"
        self.write(
            relative,
            json.dumps(
                {
                    "theme": "existing",
                    "provider": {"other": {"keep": True}},
                    "plugin": ["other-plugin"],
                }
            ),
        )
        with (
            patch.object(agent.shutil, "which", return_value="/test/bin/opencode"),
            patch.object(agent.subprocess, "check_output", return_value="1.4.0"),
        ):
            self.configure("opencode")
        config = self.read_json(relative)
        self.assertEqual(config["theme"], "existing")
        self.assertEqual(config["provider"]["other"], {"keep": True})
        provider = config["provider"][PROVIDER]
        self.assertEqual(provider["options"]["baseURL"], f"{BASE_URL}minerva/api/v1")
        self.assertEqual(provider["options"]["apiKey"], "alcf-ai-auto-refresh")
        self.assertEqual(
            provider["options"]["headers"]["X-ALCF-Session-ID"], "{env:ALCF_SESSION_ID}"
        )
        model = provider["models"][MODEL]
        self.assertEqual(model["limit"]["context"], 1048576)
        self.assertEqual(model["modalities"]["input"], ["text", "image"])
        self.assertTrue(model["tool_call"])
        self.assertTrue(model["reasoning"])
        self.assertEqual(model["options"]["reasoningEffort"], "high")
        self.assertEqual(
            model["variants"],
            {level: {"reasoningEffort": level} for level in ("low", "medium", "high")},
        )
        self.assertEqual(model["interleaved"], {"field": "reasoning"})
        self.assertEqual(
            config["plugin"],
            ["other-plugin", [agent.OPENCODE_PLUGIN_PACKAGE, {"tokenHelper": HELPER}]],
        )
        self.assertNotIn(TOKEN, json.dumps(config))

    def test_opencode_v2_uses_published_capabilities_and_preserves_settings(
        self,
    ) -> None:
        relative = ".config/opencode/opencode.jsonc"
        self.write(
            relative,
            json.dumps(
                {
                    "theme": "existing",
                    "providers": {"other": {}},
                    "plugins": [{"package": "other-plugin"}],
                }
            ),
        )
        with (
            patch.object(agent.shutil, "which", return_value="/test/bin/opencode"),
            patch.object(agent.subprocess, "check_output", return_value="2.1.0"),
        ):
            self.configure("opencode")
        config = self.read_json(relative)
        self.assertEqual(config["theme"], "existing")
        self.assertIn("other", config["providers"])
        provider = config["providers"][PROVIDER]
        self.assertEqual(provider["settings"]["baseURL"], f"{BASE_URL}minerva/api/v1")
        self.assertEqual(provider["settings"]["apiKey"], "alcf-ai-auto-refresh")
        model = provider["models"][MODEL]
        self.assertEqual(model["limit"]["context"], 1048576)
        self.assertEqual(
            model["capabilities"],
            {
                "tools": True,
                "input": ["text", "image"],
                "reasoning": {"supported": True, "effort": ["low", "medium", "high"]},
            },
        )
        self.assertEqual(model["settings"]["reasoningEffort"], "high")
        self.assertEqual(model["compatibility"]["reasoningField"], "reasoning")
        self.assertEqual(
            config["plugins"],
            [
                {"package": "other-plugin"},
                {
                    "package": agent.OPENCODE_PLUGIN_PACKAGE,
                    "options": {"tokenHelper": HELPER},
                },
            ],
        )
        self.assertNotIn(TOKEN, json.dumps(config))

    def test_pi_uses_responses_context_image_and_supported_reasoning_levels(
        self,
    ) -> None:
        relative = ".pi/agent/models.json"
        self.write(relative, json.dumps({"providers": {"other": {"keep": True}}}))
        self.configure("pi")
        config = self.read_json(relative)
        self.assertEqual(config["providers"]["other"], {"keep": True})
        provider = config["providers"][PROVIDER]
        self.assertEqual(provider["baseUrl"], f"{BASE_URL}minerva/api/v1")
        self.assertEqual(
            provider["apiKey"], "!/test/bin/alcf-tokens get-token inference"
        )
        self.assertEqual(
            provider["headers"]["X-ALCF-Session-ID"],
            "!echo ${ALCF_SESSION_ID:-default}",
        )
        model = provider["models"][0]
        self.assertEqual(model["id"], MODEL)
        self.assertEqual(model["api"], "openai-responses")
        self.assertEqual(model["contextWindow"], 1048576)
        self.assertEqual(model["input"], ["text", "image"])
        self.assertTrue(model["reasoning"])
        self.assertEqual(
            model["thinkingLevelMap"],
            {
                "minimal": None,
                "low": "low",
                "medium": "medium",
                "high": "high",
                "xhigh": None,
                "max": None,
            },
        )
        self.assertTrue(model["compat"]["supportsReasoningEffort"])
        self.assertNotIn(TOKEN, json.dumps(config))

    def test_codex_generates_native_responses_provider_profile_and_catalog(
        self,
    ) -> None:
        self.configure("codex", "--default-model", MODEL)
        config = self.read_toml(".codex/alcf.config.toml")
        self.assertEqual(config["model"], MODEL)
        self.assertEqual(config["model_provider"], PROVIDER)
        provider = config["model_providers"][PROVIDER]
        self.assertEqual(provider["base_url"], f"{BASE_URL}minerva/api/v1")
        self.assertEqual(provider["wire_api"], "responses")
        self.assertEqual(provider["auth"], {"command": HELPER[0], "args": HELPER[1:]})
        self.assertEqual(
            provider["env_http_headers"], {"X-ALCF-Session-ID": "ALCF_SESSION_ID"}
        )
        profile = self.read_toml(f".codex/alcf-minerva-{MODEL}.config.toml")
        self.assertEqual(profile["model"], MODEL)
        self.assertEqual(profile["model_provider"], PROVIDER)
        self.assertEqual(
            profile["model_providers"][PROVIDER]["base_url"],
            f"{BASE_URL}minerva/api/v1",
        )
        catalog_path = self.home / ".codex/catalogs" / f"{MODEL}.json"
        self.assertEqual(profile["model_catalog_json"], str(catalog_path))
        self.assertEqual(config["model_catalog_json"], str(catalog_path))
        entry = json.loads(catalog_path.read_text())["models"][0]
        self.assertEqual(entry["slug"], MODEL)
        self.assertEqual(entry["context_window"], 1048576)
        self.assertEqual(entry["max_context_window"], 1048576)
        self.assertEqual(entry["input_modalities"], ["text", "image"])
        self.assertEqual(
            entry["base_instructions"], "Version-matched test instructions"
        )
        self.assertEqual(entry["upstream_schema_field"], {"preserve": True})
        # Do not introduce OpenAI-specific summary/search/websocket assumptions.
        self.assertFalse(entry["supports_reasoning_summary_parameter"])
        self.assertFalse(entry["supports_search_tool"])
        self.assertFalse(entry["prefer_websockets"])
        self.assertIsNone(entry["apply_patch_tool_type"])
        self.catalog_fetch.assert_called_once_with("0.149.1")
        self.assertNotIn(TOKEN, (self.home / ".codex/alcf.config.toml").read_text())
        self.assertFalse((self.home / ".codex/config.toml").exists())

    def test_codex_no_profile_supports_legacy_base_config(self) -> None:
        self.write(".codex/config.toml", 'approval_policy = "on-request"\n')
        with patch.object(agent, "_codex_version", return_value="0.133.9"):
            self.configure("codex", "--no-profile", "--default-model", MODEL)

        config = self.read_toml(".codex/config.toml")
        self.assertEqual(config["approval_policy"], "on-request")
        self.assertEqual(config["model"], MODEL)
        self.assertEqual(config["model_provider"], PROVIDER)
        self.assertIn(PROVIDER, config["model_providers"])
        self.assertFalse((self.home / ".codex/alcf.config.toml").exists())

    def test_no_profile_rejects_agents_without_profiles(self) -> None:
        result = CliRunner().invoke(agent.cli, ["configure", "pi", "--no-profile"])

        self.assertEqual(result.exit_code, 2)
        self.assertIn("only supported for Claude", result.output)

    def test_codex_default_remains_muse(self) -> None:
        self.configure("codex")
        config = self.read_toml(".codex/alcf.config.toml")
        self.assertEqual(config["model"], "muse-spark-bf16")
        self.assertEqual(config["model_provider"], PROVIDER)

    def test_codex_preserves_existing_selection_settings_and_other_providers(
        self,
    ) -> None:
        settings = self.write(".codex/config.toml", "invalid existing config")
        self.write(
            ".codex/research.config.toml",
            'model = "existing-model"\n'
            'model_provider = "other"\napproval_policy = "on-request"\n'
            '[model_providers.other]\nname = "Existing"\n',
        )
        self.configure("codex", "--default-model", MODEL, "--profile", "research")
        config = self.read_toml(".codex/research.config.toml")
        self.assertEqual(config["model"], "existing-model")
        self.assertEqual(config["model_provider"], "other")
        self.assertEqual(config["approval_policy"], "on-request")
        self.assertEqual(config["model_providers"]["other"], {"name": "Existing"})
        self.assertEqual(settings.read_text(), "invalid existing config")

    def test_claude_uses_messages_and_refresh_helper_preserving_other_settings(
        self,
    ) -> None:
        relative = ".claude/profiles/research.json"
        self.write(
            relative,
            json.dumps(
                {
                    "permissions": {"allow": ["Read"]},
                    "env": {"KEEP_ME": "yes", "ANTHROPIC_AUTH_TOKEN": "old"},
                }
            ),
        )
        self.configure("claude", "--default-model", MODEL, "--profile", "research")
        config = self.read_json(relative)
        self.assertEqual(config["permissions"], {"allow": ["Read"]})
        self.assertEqual(config["env"]["KEEP_ME"], "yes")
        self.assertEqual(config["env"]["ANTHROPIC_BASE_URL"], f"{BASE_URL}minerva/api")
        self.assertEqual(config["env"]["CLAUDE_CODE_MAX_CONTEXT_TOKENS"], "1048576")
        for key in (
            "ANTHROPIC_MODEL",
            "ANTHROPIC_DEFAULT_OPUS_MODEL",
            "ANTHROPIC_DEFAULT_SONNET_MODEL",
            "ANTHROPIC_DEFAULT_HAIKU_MODEL",
        ):
            self.assertEqual(config["env"][key], MODEL)
        self.assertEqual(
            config["apiKeyHelper"], "/test/bin/alcf-tokens get-token inference"
        )
        self.assertNotIn("ANTHROPIC_AUTH_TOKEN", config["env"])
        self.assertNotIn(TOKEN, json.dumps(config))

    def test_claude_no_profile_updates_base_settings(self) -> None:
        self.write(
            ".claude/settings.json",
            json.dumps({"permissions": {"allow": ["Read"]}}),
        )
        self.configure("claude", "--no-profile", "--default-model", MODEL)

        config = self.read_json(".claude/settings.json")
        self.assertEqual(config["permissions"], {"allow": ["Read"]})
        self.assertEqual(config["env"]["ANTHROPIC_MODEL"], MODEL)
        self.assertEqual(config["env"]["CLAUDE_CODE_MAX_CONTEXT_TOKENS"], "1048576")
        self.assertFalse((self.home / ".claude/profiles/alcf.json").exists())

    def test_no_profile_rejects_explicit_profile_name(self) -> None:
        with patch.object(agent, "get_access_token") as get_token:
            result = CliRunner().invoke(
                agent.cli,
                ["configure", "claude", "--profile", "research", "--no-profile"],
            )

        self.assertEqual(result.exit_code, 2)
        self.assertIn("cannot be combined", result.output)
        get_token.assert_not_called()

    def test_claude_default_remains_muse(self) -> None:
        settings = self.write(".claude/settings.json", "invalid existing config")
        self.configure("claude")
        profile = self.read_json(".claude/profiles/alcf.json")
        self.assertEqual(profile["env"]["ANTHROPIC_MODEL"], "muse-spark-bf16")
        self.assertEqual(settings.read_text(), "invalid existing config")

    def test_static_token_fallback_is_unchanged(self) -> None:
        self.helper.return_value = None
        self.configure("pi")
        self.assertEqual(
            self.read_json(".pi/agent/models.json")["providers"][PROVIDER]["apiKey"],
            TOKEN,
        )
        self.configure("codex", "--default-model", MODEL)
        provider = self.read_toml(".codex/alcf.config.toml")["model_providers"][
            PROVIDER
        ]
        self.assertNotIn("auth", provider)
        self.assertEqual(provider["experimental_bearer_token"], TOKEN)
        self.configure("claude", "--default-model", MODEL)
        config = self.read_json(".claude/profiles/alcf.json")
        self.assertNotIn("apiKeyHelper", config)
        self.assertEqual(config["env"]["ANTHROPIC_AUTH_TOKEN"], TOKEN)

    def test_run_claude_execs_with_environment_and_forwards_arguments(self) -> None:
        settings = self.write(".claude/settings.json", "invalid existing config")
        with (
            patch.dict(
                os.environ,
                {"KEEP_ME": "yes", "ANTHROPIC_MODEL": "parent-model"},
            ),
            patch.object(agent.os, "execvpe") as execvpe,
        ):
            result = self.run_claude(
                "--default-model",
                MODEL,
                "--default-cluster",
                "tara",
                "--",
                "--print",
                "hello",
            )
            self.assertEqual(os.environ["ANTHROPIC_MODEL"], "parent-model")

        self.assertEqual(result.exit_code, 0, result.output or str(result.exception))
        executable, argv, env = execvpe.call_args.args
        self.assertEqual(executable, "claude")
        self.assertEqual(argv, ["claude", "--print", "hello"])
        self.assertEqual(env["KEEP_ME"], "yes")
        self.assertEqual(env["ANTHROPIC_BASE_URL"], f"{BASE_URL}tara/api")
        self.assertEqual(env["ANTHROPIC_AUTH_TOKEN"], TOKEN)
        self.assertEqual(env["CLAUDE_CODE_MAX_CONTEXT_TOKENS"], "1048576")
        for key in (
            "ANTHROPIC_MODEL",
            "ANTHROPIC_DEFAULT_OPUS_MODEL",
            "ANTHROPIC_DEFAULT_SONNET_MODEL",
            "ANTHROPIC_DEFAULT_HAIKU_MODEL",
        ):
            self.assertEqual(env[key], MODEL)
        self.assertEqual(settings.read_text(), "invalid existing config")
        self.helper.assert_not_called()

    def test_run_claude_uses_muse_by_default(self) -> None:
        with patch.object(agent.os, "execvpe") as execvpe:
            result = self.run_claude()

        self.assertEqual(result.exit_code, 0, result.output or str(result.exception))
        env = execvpe.call_args.args[2]
        self.assertEqual(env["ANTHROPIC_BASE_URL"], f"{BASE_URL}minerva/api")
        self.assertEqual(env["ANTHROPIC_MODEL"], "muse-spark-bf16")
        self.assertEqual(env["CLAUDE_CODE_MAX_CONTEXT_TOKENS"], "1048576")

    def test_run_claude_reports_missing_executable(self) -> None:
        settings = self.write(".claude/settings.json", "invalid existing config")
        with patch.object(
            agent.os,
            "execvpe",
            side_effect=FileNotFoundError(2, "No such file or directory"),
        ):
            result = self.run_claude()

        self.assertEqual(result.exit_code, 1)
        self.assertIn("Could not execute claude", result.output)
        self.assertEqual(settings.read_text(), "invalid existing config")

    def test_run_claude_loads_profile_with_settings_overlay(self) -> None:
        settings = self.write(".claude/settings.json", "invalid existing config")
        self.configure("claude", "--default-model", MODEL)
        profile_path = self.home / ".claude/profiles/alcf.json"

        with (
            patch.object(agent, "get_access_token") as get_token,
            patch.object(agent.os, "execvpe") as execvpe,
        ):
            result = self.run_claude("--profile", "alcf", "--", "--version")

        self.assertEqual(result.exit_code, 0, result.output or str(result.exception))
        executable, argv, env = execvpe.call_args.args
        self.assertEqual(executable, "claude")
        self.assertEqual(argv, ["claude", "--settings", str(profile_path), "--version"])
        self.assertNotIn("ANTHROPIC_AUTH_TOKEN", env)
        self.assertEqual(settings.read_text(), "invalid existing config")
        get_token.assert_not_called()

    def test_run_claude_rejects_missing_profile(self) -> None:
        with (
            patch.object(agent, "get_access_token") as get_token,
            patch.object(agent.os, "execvpe") as execvpe,
        ):
            result = self.run_claude("--profile", "missing")

        self.assertEqual(result.exit_code, 2)
        self.assertIn("does not exist", result.output)
        get_token.assert_not_called()
        execvpe.assert_not_called()

    def test_run_claude_profile_rejects_model_override(self) -> None:
        self.write(".claude/profiles/alcf.json", "{}")
        with (
            patch.object(agent, "get_access_token") as get_token,
            patch.object(agent.os, "execvpe") as execvpe,
        ):
            result = self.run_claude("--profile", "alcf", "--default-model", MODEL)

        self.assertEqual(result.exit_code, 2)
        self.assertIn("cannot be combined", result.output)
        get_token.assert_not_called()
        execvpe.assert_not_called()

    def test_run_opencode_uses_inline_config_and_forwards_arguments(self) -> None:
        settings = self.write(
            ".config/opencode/opencode.jsonc", "invalid existing config"
        )
        self.helper.return_value = None
        with (
            patch.object(agent.shutil, "which", return_value="/test/bin/opencode"),
            patch.object(agent.subprocess, "check_output", return_value="2.1.0"),
            patch.object(agent.os, "execvpe") as execvpe,
        ):
            result = self.run_agent(
                "opencode", "--default-model", MODEL, "--", "run", "hello"
            )

        self.assertEqual(result.exit_code, 0, result.output or str(result.exception))
        executable, argv, env = execvpe.call_args.args
        self.assertEqual(executable, "opencode")
        self.assertEqual(argv, ["opencode", "run", "hello"])
        config = json.loads(env["OPENCODE_CONFIG_CONTENT"])
        self.assertEqual(config["model"], f"{PROVIDER}/{MODEL}")
        provider = config["providers"][PROVIDER]
        self.assertEqual(provider["settings"]["baseURL"], f"{BASE_URL}minerva/api/v1")
        self.assertEqual(provider["settings"]["apiKey"], TOKEN)
        self.assertIn(MODEL, provider["models"])
        self.assertEqual(settings.read_text(), "invalid existing config")

    def test_run_opencode_rejects_unavailable_model_before_exec(self) -> None:
        with patch.object(agent.os, "execvpe") as execvpe:
            result = self.run_agent("opencode", "--default-model", "missing-model")

        self.assertEqual(result.exit_code, 2)
        self.assertIn("not available", result.output)
        execvpe.assert_not_called()

    def test_run_codex_uses_cli_overrides_and_environment_credentials(self) -> None:
        settings = self.write(".codex/config.toml", "invalid existing config")
        with patch.object(agent.os, "execvpe") as execvpe:
            result = self.run_agent(
                "codex", "--default-model", MODEL, "--", "exec", "hello"
            )

        self.assertEqual(result.exit_code, 0, result.output or str(result.exception))
        executable, argv, env = execvpe.call_args.args
        self.assertEqual(executable, "codex")
        self.assertEqual(argv[:3], ["codex", "--model", MODEL])
        self.assertEqual(argv[-2:], ["exec", "hello"])
        self.assertEqual(env["ALCF_API_KEY"], TOKEN)
        self.assertEqual(env["ALCF_SESSION_ID"], "default")
        self.assertNotIn(TOKEN, " ".join(argv))

        overrides = {}
        for index, value in enumerate(argv):
            if value == "--config":
                key, override = argv[index + 1].split("=", 1)
                overrides[key] = override
        self.assertEqual(overrides["model_provider"], json.dumps(PROVIDER))
        self.assertEqual(
            overrides[f"model_providers.{PROVIDER}.base_url"],
            json.dumps(f"{BASE_URL}minerva/api/v1"),
        )
        self.assertEqual(
            overrides[f"model_providers.{PROVIDER}.wire_api"],
            json.dumps("responses"),
        )
        self.assertEqual(
            overrides[f"model_providers.{PROVIDER}.env_key"],
            json.dumps("ALCF_API_KEY"),
        )
        self.assertEqual(overrides["model_context_window"], "1048576")
        self.assertEqual(settings.read_text(), "invalid existing config")
        self.catalog_fetch.assert_not_called()

    def test_run_codex_reports_version_detection_failure(self) -> None:
        with (
            patch.object(
                agent,
                "_codex_version",
                side_effect=RuntimeError("codex CLI is not installed"),
            ),
            patch.object(agent.os, "execvpe") as execvpe,
        ):
            result = self.run_agent("codex", "--default-model", MODEL)

        self.assertEqual(result.exit_code, 1)
        self.assertIn("Could not configure codex", result.output)
        execvpe.assert_not_called()

    def test_run_codex_loads_native_profile(self) -> None:
        settings = self.write(".codex/config.toml", "invalid existing config")
        self.configure("codex", "--default-model", MODEL)

        with (
            patch.object(agent, "get_access_token") as get_token,
            patch.object(agent.os, "execvpe") as execvpe,
        ):
            result = self.run_agent("codex", "--profile", "alcf", "--", "--version")

        self.assertEqual(result.exit_code, 0, result.output or str(result.exception))
        executable, argv, env = execvpe.call_args.args
        self.assertEqual(executable, "codex")
        self.assertEqual(argv, ["codex", "--profile", "alcf", "--version"])
        self.assertNotIn("ALCF_API_KEY", env)
        self.assertEqual(settings.read_text(), "invalid existing config")
        get_token.assert_not_called()

    def test_run_codex_rejects_missing_profile(self) -> None:
        with (
            patch.object(agent, "get_access_token") as get_token,
            patch.object(agent.os, "execvpe") as execvpe,
        ):
            result = self.run_agent("codex", "--profile", "missing")

        self.assertEqual(result.exit_code, 2)
        self.assertIn("does not exist", result.output)
        get_token.assert_not_called()
        execvpe.assert_not_called()

    def test_run_codex_rejects_profiles_before_0_134(self) -> None:
        self.write(".codex/alcf.config.toml", "")
        with (
            patch.object(agent, "_codex_version", return_value="0.133.9"),
            patch.object(agent, "get_access_token") as get_token,
            patch.object(agent.os, "execvpe") as execvpe,
        ):
            result = self.run_agent("codex", "--profile", "alcf")

        self.assertEqual(result.exit_code, 2)
        self.assertIn("0.134.0", result.output)
        get_token.assert_not_called()
        execvpe.assert_not_called()

    def test_configure_codex_does_not_fall_back_to_base_config(self) -> None:
        settings = self.write(".codex/config.toml", "invalid existing config")
        with patch.object(agent, "_codex_version", return_value="0.133.9"):
            with self.assertRaises(agent.typer.BadParameter):
                agent.edit_codex(BASE_URL, TOKEN, self.infos, MODEL, "minerva")

        self.assertEqual(settings.read_text(), "invalid existing config")
        self.assertFalse((self.home / ".codex/alcf.config.toml").exists())
        self.catalog_fetch.assert_not_called()

    def test_run_aider_uses_openai_environment_and_forwards_arguments(self) -> None:
        settings = self.write(".aider.conf.yml", "invalid existing config")
        with (
            patch.dict(os.environ, {"OPENAI_API_BASE": "parent-url"}),
            patch.object(agent.os, "execvpe") as execvpe,
        ):
            result = self.run_agent(
                "aider", "--default-model", MODEL, "--", "--message", "hello"
            )
            self.assertEqual(os.environ["OPENAI_API_BASE"], "parent-url")

        self.assertEqual(result.exit_code, 0, result.output or str(result.exception))
        executable, argv, env = execvpe.call_args.args
        self.assertEqual(executable, "aider")
        self.assertEqual(argv, ["aider", "--message", "hello"])
        self.assertEqual(env["OPENAI_API_BASE"], f"{BASE_URL}minerva/api/v1")
        self.assertEqual(env["OPENAI_API_KEY"], TOKEN)
        self.assertEqual(env["AIDER_MODEL"], f"openai/{MODEL}")
        self.assertEqual(settings.read_text(), "invalid existing config")
        self.helper.assert_not_called()

    def test_overwrite_rejection_preserves_existing_file(self) -> None:
        profile = self.write(".claude/profiles/alcf.json", "{}")
        result = self.invoke_configure("claude", "--default-model", MODEL, input="n\n")

        self.assertEqual(result.exit_code, 1)
        self.assertIn("[y/N]", result.output)
        self.assertEqual(profile.read_text(), "{}")
        self.assertFalse(list(self.home.rglob("*.bak.*")))

    def test_yes_approves_overwrites_without_prompt(self) -> None:
        profile = self.write(".claude/profiles/alcf.json", "{}")
        result = self.invoke_configure("claude", "--yes", "--default-model", MODEL)

        self.assertEqual(result.exit_code, 0, result.output or str(result.exception))
        self.assertNotIn("[y/N]", result.output)
        self.assertEqual(
            self.read_json(".claude/profiles/alcf.json")["env"]["ANTHROPIC_MODEL"],
            MODEL,
        )
        backups = list(profile.parent.glob(f"{profile.name}.bak.*"))
        self.assertEqual(len(backups), 1)
        self.assertEqual(backups[0].read_text(), "{}")

    def test_new_configuration_file_does_not_prompt(self) -> None:
        result = self.invoke_configure("claude")

        self.assertEqual(result.exit_code, 0, result.output or str(result.exception))
        self.assertNotIn("[y/N]", result.output)
        self.assertTrue((self.home / ".claude/profiles/alcf.json").is_file())

    def test_generated_files_are_idempotent(self) -> None:
        with (
            patch.object(agent.shutil, "which", return_value="/test/bin/opencode"),
            patch.object(agent.subprocess, "check_output", return_value="2.1.0"),
        ):
            for name in ("opencode", "pi", "codex", "claude"):
                self.configure(name, "--default-model", MODEL)
            first = {
                p.relative_to(self.home): p.read_bytes()
                for p in self.home.rglob("*")
                if p.is_file()
            }
            for name in ("opencode", "pi", "codex", "claude"):
                self.configure(name, "--default-model", MODEL)
        self.assertEqual(
            first,
            {
                p.relative_to(self.home): p.read_bytes()
                for p in self.home.rglob("*")
                if p.is_file()
            },
        )
        self.assertFalse(list(self.home.rglob("*.bak.*")))

    def test_invalid_existing_config_is_not_overwritten(self) -> None:
        for name, relative in (
            ("pi", ".pi/agent/models.json"),
            ("codex", ".codex/alcf.config.toml"),
            ("claude", ".claude/profiles/alcf.json"),
            ("opencode", ".config/opencode/opencode.jsonc"),
        ):
            with self.subTest(agent=name):
                path = self.write(relative, "invalid existing config")
                with self.assertRaises(agent.ConfigLoadError):
                    if name == "pi":
                        agent.edit_pi(BASE_URL, TOKEN, self.infos)
                    elif name == "codex":
                        agent.edit_codex(BASE_URL, TOKEN, self.infos, MODEL, "minerva")
                    elif name == "claude":
                        agent.edit_claude(BASE_URL, TOKEN, MODEL, "minerva")
                    else:
                        agent.edit_opencode(BASE_URL, TOKEN, self.infos)
                self.assertEqual(path.read_text(), "invalid existing config")


if __name__ == "__main__":
    unittest.main()
