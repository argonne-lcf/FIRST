import copy
import json
import logging
import shutil
import subprocess
from pathlib import Path
from typing import Annotated, Any, Literal

import httpx
import tomlkit
import typer
from httpx import URL

from alcf_ai.auth import get_inference_authorizer

cli = typer.Typer(no_args_is_help=True)

ALLOWLIST: dict[str, list[str]] = {
    "sophia": ["openai/gpt-oss-120b"],
    # Metis has a SN-specific sanitization issue w.r.t. tool call outputs, disable for now.
    # "metis": ["gpt-oss-120b"],
    "minerva": ["nemotron-3-ultra", "inkling-bf16", "gpt-oss-120b"],
}

DEFAULT_CAPABILITIES: dict[str, Any] = {
    "api_protocols": ["chat_completions"],
    "context_window_tokens": 65536,
    "input_modalities": ["text"],
}


def _merge_capabilities(model: dict[str, Any]) -> dict[str, Any]:
    """
    Merge the model's reported capabilities with DEFAULT_CAPABILITIES.
    """
    caps = dict(DEFAULT_CAPABILITIES)
    caps.update(model.get("capabilities") or {})

    # Every capability that list_models(<cluster>) can return for API-backed
    # models. vLLM-backed deployments (e.g. sophia) report the same capability
    # fields at the top level (max_model_len, max_num_seqs,
    # enable_auto_tool_choice, tool_call_parser, reasoning_parser) instead of a
    # ``capabilities`` sub-object.
    if "max_model_len" in model:
        caps["context_window_tokens"] = model["max_model_len"]
    if "max_num_seqs" in model:
        caps["max_num_seqs"] = model["max_num_seqs"]
    if model.get("enable_auto_tool_choice") or model.get("tool_call_parser"):
        caps.setdefault("tool_calling", {})["supported"] = True
    if model.get("reasoning_parser"):
        caps.setdefault("reasoning", {})["supported"] = True

    return caps


def _codex_version() -> str:
    """Return the installed Codex CLI version (e.g. "0.149.1")."""
    codex = shutil.which("codex")
    if codex is None:
        raise RuntimeError("codex CLI is not installed")
    output = subprocess.check_output([codex, "--version"], text=True).strip()
    return output.rsplit(maxsplit=1)[-1].lstrip("v")


def _fetch_catalog_and_prompt(version: str) -> tuple[str, str]:
    """Fetch the upstream catalog/prompt matching the Codex version."""
    base = f"https://raw.githubusercontent.com/openai/codex/rust-v{version}/codex-rs/models-manager"
    with httpx.Client(timeout=httpx.Timeout(60)) as client:
        resp = client.get(f"{base}/models.json")
        resp.raise_for_status()
        catalog = resp.text
        resp = client.get(f"{base}/prompt.md")
        resp.raise_for_status()
        prompt = resp.text
    return catalog, prompt


def _build_codex_catalog(
    upstream: dict[str, Any],
    prompt_text: str,
    *,
    slug: str,
    display_name: str,
    description: str,
    context_window: int,
    version: str,
) -> dict[str, Any]:
    """Clone a version-matched upstream entry and adapt it to a served model."""
    template = next(
        (m for m in upstream["models"] if m.get("slug") == "gpt-5.4"),
        upstream["models"][0],
    )
    entry = copy.deepcopy(template)
    entry.update(
        {
            "slug": slug,
            "display_name": display_name,
            "description": description,
            "visibility": "list",
            "supported_in_api": True,
            "priority": 0,
            "minimal_client_version": version,
            "availability_nux": None,
            "upgrade": None,
            "context_window": context_window,
            "max_context_window": context_window,
            "auto_compact_token_limit": None,
            "effective_context_window_percent": 90,
            "comp_hash": None,
            "default_reasoning_level": None,
            "supported_reasoning_levels": [],
            "supports_reasoning_summary_parameter": False,
            "supports_reasoning_summaries": False,
            "default_reasoning_summary": "none",
            "reasoning_summary_format": "none",
            "support_verbosity": False,
            "default_verbosity": None,
            "shell_type": "shell_command",
            "apply_patch_tool_type": "freeform",
            "supports_search_tool": False,
            "web_search_tool_type": "text",
            "experimental_supported_tools": [],
            "input_modalities": ["text"],
            "supports_image_detail_original": False,
            "prefer_websockets": False,
            "use_responses_lite": False,
            "tool_mode": None,
            "multi_agent_version": None,
            "auto_review_model_override": None,
            "model_specialty": None,
            "service_tiers": [],
            "additional_speed_tiers": [],
            "default_service_tier": None,
            "include_skills_usage_instructions": True,
            "include_plugin_usage_instructions": False,
            "include_apps_usage_instructions": False,
        }
    )
    messages = entry.setdefault("model_messages", {})
    messages["instructions_template"] = prompt_text
    messages["instructions_variables"] = None
    entry["base_instructions"] = prompt_text
    if "supports_parallel_tool_calls" in entry:
        entry["supports_parallel_tool_calls"] = False
    if "node_repl_disabled" in entry:
        entry["node_repl_disabled"] = True
    if "node_repl_auto_review_required" in entry:
        entry["node_repl_auto_review_required"] = False
    return {"models": [entry]}


def generate_codex_model_configs(
    model_infos: dict[str, list[dict[str, Any]]], version: str
) -> None:
    """Build Codex model catalogs and per-model config files for exposed models."""
    _CODEX_HOME = Path.home() / ".codex"
    _CODEX_CATALOG_DIR = _CODEX_HOME / "catalogs"

    try:
        upstream, prompt_text = _fetch_catalog_and_prompt(version)
        template = json.loads(upstream)
    except (
        OSError,
        RuntimeError,
        subprocess.SubprocessError,
        httpx.HTTPError,
        ValueError,
        json.JSONDecodeError,
    ) as err:
        logging.warning(f"Skipping codex model catalog generation: {err}")
        return

    for cluster_name, models in model_infos.items():
        for framework in [f for m in models if (f := m.get("framework"))]:
            provider_name = f"alcf-inference-service-{cluster_name}-{framework}"
            for model in models:
                if model.get("framework") != framework:
                    continue

                caps = _merge_capabilities(model)
                if (
                    (slug := model.get("id"))
                    and (ctx := caps.get("context_window_tokens"))
                    and (name := model.get("display_name"))
                ):
                    catalog_path = _CODEX_CATALOG_DIR / f"{slug.replace('/', '-')}.json"
                    catalog = _build_codex_catalog(
                        template,
                        prompt_text,
                        slug=slug,
                        display_name=name,
                        description=f"{slug} served through ALCF {cluster_name.title()}",
                        context_window=ctx,
                        version=version,
                    )
                    catalog_path.write_text(json.dumps(catalog, indent=2))
                    logging.info(f"Created {catalog_path} for Codex {version}")

                    profile_path = (
                        _CODEX_HOME
                        / f"alcf-{cluster_name}-{slug.replace('/', '-')}.config.toml"
                    )
                    profile = tomlkit.TOMLDocument()
                    profile["model_provider"] = provider_name
                    profile["model"] = slug
                    profile["model_catalog_json"] = str(catalog_path)
                    profile_path.write_text(tomlkit.dumps(profile))
                    logging.info(f"Created {profile_path}")


def edit_opencode(
    base_url: URL,
    api_key: str,
    model_infos: dict[str, list[dict[str, Any]]],
) -> None:
    path = Path.home() / ".config" / "opencode" / "opencode.jsonc"
    try:
        with path.open() as f:
            config = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        config = {}

    providers = config.get("provider", {})
    for cluster_name, models in model_infos.items():
        for framework in [f for m in models if (f := m.get("framework"))]:
            entries = {}
            for model in models:
                if model.get("framework") != framework:
                    continue

                caps = _merge_capabilities(model)
                entry = {}

                if name := model.get("display_name"):
                    entry["name"] = name

                entry["timeout"] = False

                if ctx := caps.get("context_window_tokens"):
                    entry["limit"] = {
                        "context": ctx,
                        "output": 0,
                    }

                if inputs := caps.get("input_modalities"):
                    entry["modalities"] = {"input": inputs}

                if caps.get("tool_calling", {}).get("supported", False):
                    entry["tool_call"] = True

                if reasoning := caps.get("reasoning"):
                    if reasoning.get("supported", False):
                        entry["reasoning"] = True

                    if (effort := reasoning.get("default_effort")) and (
                        lvls := reasoning.get("effort_levels")
                    ):
                        entry.setdefault("options", {})["reasoningEffort"] = effort
                        entry["variants"] = {
                            lvl: {"reasoningEffort": lvl} for lvl in lvls
                        }

                    if reasoning.get("separate_output", False):
                        entry["interleaved"] = {
                            "field": "reasoning_content",
                        }

                if m_id := model.get("id"):
                    entries[m_id] = entry

            providers[f"alcf-inference-service-{cluster_name}-{framework}"] = {
                "name": f"ALCF Inference Service ({cluster_name.title()}, {'vLLM' if framework == 'vllm' else 'Direct API'})",
                "npm": "@ai-sdk/openai-compatible",
                "options": {
                    "baseURL": f"{base_url}{cluster_name}/{framework}/v1",
                    "apiKey": f"{api_key}",
                },
                "models": entries,
            }

    config["provider"] = providers

    path.parent.mkdir(exist_ok=True, parents=True)
    with path.open("w") as f:
        json.dump(config, f, indent=2)

    logging.info(f"Updated configuration at {path}")


def edit_pi(
    base_url: URL,
    api_key: str,
    model_infos: dict[str, list[dict[str, Any]]],
) -> None:
    path = Path.home() / ".pi" / "agent" / "models.json"
    try:
        with path.open() as f:
            config = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        config = {}

    providers = config.get("providers", {})
    for cluster_name, models in model_infos.items():
        for framework in [f for m in models if (f := m.get("framework"))]:
            entries = []
            for model in models:
                if model.get("framework") != framework:
                    continue

                caps = _merge_capabilities(model)
                entry = {}

                if m_id := model.get("id"):
                    entry["id"] = m_id
                else:
                    continue

                if name := model.get("display_name"):
                    entry["name"] = name

                if protocols := model.get("api_protocols"):
                    entry["api"] = (
                        "openai-responses"
                        if "responses" in protocols
                        else "openai-completions"
                    )

                if ctx := model.get("context_window_tokens"):
                    entry["contextWindow"] = ctx

                if inputs := caps.get("input_modalities"):
                    entry["input"] = inputs

                if reasoning := caps.get("reasoning"):
                    if reasoning.get("supported", False):
                        entry["reasoning"] = True

                    if lvls := reasoning.get("effort_levels"):
                        entry["thinkingLevelMap"] = {
                            lvl: lvl if lvl in lvls else None
                            for lvl in (
                                "minimal",
                                "low",
                                "medium",
                                "high",
                                "xhigh",
                                "max",
                            )
                        }
                        entry.setdefault("compat", {})["supportsReasoningEffort"] = True

                entries.append(entry)

            providers[f"alcf-inference-service-{cluster_name}-{framework}"] = {
                "baseUrl": f"{base_url}{cluster_name}/{framework}/v1",
                "api": "openai-completions",
                "apiKey": f"{api_key}",
                "compat": {
                    "supportsDeveloperRole": False,
                },
                "models": entries,
            }

    config["providers"] = providers

    path.parent.mkdir(exist_ok=True, parents=True)
    with path.open("w") as f:
        json.dump(config, f, indent=2)

    logging.info(f"Updated configuration at {path}")


def edit_codex(
    base_url: URL,
    api_key: str,
    model_infos: dict[str, list[dict[str, Any]]],
) -> None:
    path = Path.home() / ".codex" / "config.toml"
    try:
        with path.open() as f:
            config = tomlkit.load(f)
    except (FileNotFoundError, tomlkit.exceptions.ParseError):
        config = tomlkit.TOMLDocument()

    try:
        version = _codex_version()
    except (
        OSError,
        RuntimeError,
        subprocess.SubprocessError,
        httpx.HTTPError,
        ValueError,
    ) as err:
        logging.warning(f"Skipping codex model catalog generation: {err}")
        return

    providers = config.get("model_providers", {})
    for cluster_name, models in model_infos.items():
        for framework in [f for m in models if (f := m.get("framework"))]:
            wire_api = "chat"
            if tuple(map(int, version.split("."))) > (0, 94, 0) or "responses" in [
                p for m in models if (pl := m.get("api_protocols")) for p in pl
            ]:
                wire_api = "responses"

            providers[f"alcf-inference-service-{cluster_name}-{framework}"] = {
                "name": f"ALCF Inference Service ({cluster_name.title()}, {'vLLM' if framework == 'vllm' else 'Direct API'})",
                "base_url": f"{base_url}{cluster_name}/{framework}/v1",
                "experimental_bearer_token": f"{api_key}",
                "wire_api": wire_api,
            }

    config["model_providers"] = providers

    # Pick a default model only when the user hasn't already configured one;
    # never override an existing model/model_provider selection.
    config.setdefault("model", "inkling-bf16")
    config.setdefault("model_provider", "alcf-inference-service-minerva-api")

    path.parent.mkdir(exist_ok=True, parents=True)
    with path.open("w") as f:
        tomlkit.dump(config, f)

    generate_codex_model_configs(model_infos, version)

    logging.info(f"Updated configuration at {path}")


@cli.command()
def configure(
    agent: Annotated[Literal["opencode", "codex", "pi"], typer.Argument()],
) -> None:
    """
    Generates a configuration template for the given agent.
    """
    from .cli import _cli_state

    client = _cli_state["client"]
    auth = get_inference_authorizer()
    auth.ensure_valid_token()  # type: ignore[attr-defined]
    api_key = auth.access_token  # type: ignore[attr-defined]

    model_infos: dict[str, list[dict[str, Any]]] = {
        cluster_name: models
        for cluster_name, whitelist in ALLOWLIST.items()
        if (
            models := [
                m for m in client.list_models(cluster_name) if m["id"] in whitelist
            ]
        )
    }

    match agent:
        case "opencode":
            edit_opencode(client.base_url, api_key, model_infos)
        case "codex":
            edit_codex(client.base_url, api_key, model_infos)
        case "pi":
            edit_pi(client.base_url, api_key, model_infos)
