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

    def configure(self, name: str, *options: str):
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
            result = CliRunner().invoke(agent.cli, [name, *options])
        self.assertEqual(result.exit_code, 0, result.output or str(result.exception))
        return result

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
        self.assertEqual(model["interleaved"], {"field": "reasoning_content"})
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
        self.assertEqual(model["compatibility"]["reasoningField"], "reasoning_content")
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
        config = self.read_toml(".codex/config.toml")
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
        catalog_path = self.home / ".codex/catalogs" / f"{MODEL}.json"
        self.assertEqual(profile["model_catalog_json"], str(catalog_path))
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
        self.catalog_fetch.assert_called_once_with("0.149.1")
        self.assertNotIn(TOKEN, (self.home / ".codex/config.toml").read_text())

    def test_codex_default_remains_muse(self) -> None:
        self.configure("codex")
        config = self.read_toml(".codex/config.toml")
        self.assertEqual(config["model"], "muse-spark-bf16")
        self.assertEqual(config["model_provider"], PROVIDER)

    def test_codex_preserves_existing_selection_settings_and_other_providers(
        self,
    ) -> None:
        self.write(
            ".codex/config.toml",
            'model = "existing-model"\n'
            'model_provider = "other"\napproval_policy = "on-request"\n'
            '[model_providers.other]\nname = "Existing"\n',
        )
        self.configure("codex", "--default-model", MODEL)
        config = self.read_toml(".codex/config.toml")
        self.assertEqual(config["model"], "existing-model")
        self.assertEqual(config["model_provider"], "other")
        self.assertEqual(config["approval_policy"], "on-request")
        self.assertEqual(config["model_providers"]["other"], {"name": "Existing"})

    def test_claude_uses_messages_and_refresh_helper_preserving_other_settings(
        self,
    ) -> None:
        relative = ".claude/settings.json"
        self.write(
            relative,
            json.dumps(
                {
                    "permissions": {"allow": ["Read"]},
                    "env": {"KEEP_ME": "yes", "ANTHROPIC_AUTH_TOKEN": "old"},
                }
            ),
        )
        self.configure("claude", "--default-model", MODEL)
        config = self.read_json(relative)
        self.assertEqual(config["permissions"], {"allow": ["Read"]})
        self.assertEqual(config["env"]["KEEP_ME"], "yes")
        self.assertEqual(config["env"]["ANTHROPIC_BASE_URL"], f"{BASE_URL}minerva/api")
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

    def test_claude_default_remains_muse(self) -> None:
        self.configure("claude")
        self.assertEqual(
            self.read_json(".claude/settings.json")["env"]["ANTHROPIC_MODEL"],
            "muse-spark-bf16",
        )

    def test_static_token_fallback_is_unchanged(self) -> None:
        self.helper.return_value = None
        self.configure("pi")
        self.assertEqual(
            self.read_json(".pi/agent/models.json")["providers"][PROVIDER]["apiKey"],
            TOKEN,
        )
        self.configure("codex", "--default-model", MODEL)
        provider = self.read_toml(".codex/config.toml")["model_providers"][PROVIDER]
        self.assertNotIn("auth", provider)
        self.assertEqual(provider["experimental_bearer_token"], TOKEN)
        self.configure("claude", "--default-model", MODEL)
        config = self.read_json(".claude/settings.json")
        self.assertNotIn("apiKeyHelper", config)
        self.assertEqual(config["env"]["ANTHROPIC_AUTH_TOKEN"], TOKEN)

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
            ("codex", ".codex/config.toml"),
            ("claude", ".claude/settings.json"),
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
