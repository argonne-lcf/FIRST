import json
import logging
from pathlib import Path
from typing import Annotated, Any, Literal

import tomlkit
import typer
from httpx import URL

from alcf_ai.auth import get_inference_authorizer

cli = typer.Typer(no_args_is_help=True)

ALLOWLIST = {
    "sophia": {"openai/gpt-oss-120b": {"limit": {"context": 65536, "output": 0}}},
    # Metis has a SN-specific sanitization issue w.r.t. tool call outputs, disable for now.
    # "metis": {"gpt-oss-120b": {"limits": {"context": 65536, "output": 0}}},
    "minerva": {
        "nemotron-3-ultra": {"limit": {"context": 262144, "output": 0}},
        "inkling-bf16": {"limit": {"context": 262144, "output": 0}},
    },
}


def edit_opencode(service_url: URL, api_key: str, endpoints: dict[str, Any]) -> None:
    path = Path.home() / ".config" / "opencode" / "opencode.jsonc"
    try:
        with path.open() as f:
            config = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        config = {}

    providers = config.get("provider", {})
    for cluster_name, cluster in endpoints["clusters"].items():
        for framework_name, framework in cluster["frameworks"].items():
            if framework_name not in ("vllm", "api"):
                continue

            models = {
                model: {
                    "name": model,
                    "timeout": False,
                    "limit": ALLOWLIST[cluster_name][model]["limit"],
                }
                for model in framework["models"]
                if model in ALLOWLIST.get(cluster_name, {})
            }

            if not models:
                continue

            providers[f"alcf-inference-service-{cluster_name}-{framework_name}"] = {
                "name": f"ALCF Inference Service ({cluster_name.title()}, {'vLLM' if framework_name == 'vllm' else 'Direct API'})",
                "npm": "@ai-sdk/openai-compatible",
                "options": {
                    "baseURL": f"{service_url}{cluster_name}/{framework_name}/v1",
                    "apiKey": f"{api_key}",
                },
                "models": models,
            }

    config["provider"] = providers
    path.parent.mkdir(exist_ok=True, parents=True)
    with path.open("w") as f:
        json.dump(config, f, indent=2)

    logging.info(f"Updated configuration at {path}")


def edit_codex(service_url: URL, api_key: str, endpoints: dict[str, Any]) -> None:
    path = Path.home() / ".codex" / "config.toml"
    try:
        with path.open() as f:
            config = tomlkit.load(f)
    except (FileNotFoundError, tomlkit.exceptions.ParseError):
        config = tomlkit.TOMLDocument()

    providers = config.get("model_providers", {})
    for cluster_name, cluster in endpoints["clusters"].items():
        for api in cluster["frameworks"].keys():
            if api not in ("vllm", "api"):
                continue

            providers[f"alcf-inference-service-{cluster_name}-{api}"] = {
                "name": f"ALCF Inference Service ({cluster_name.title()}, {'vLLM' if api == 'vllm' else 'Direct API'})",
                "base_url": f"{service_url}{cluster_name}/{api}/v1",
                "experimental_bearer_token": f"{api_key}",
                "wire_api": "chat",
            }

    config["model_providers"] = providers

    # hardcode default to minerva nemotron
    config["model"] = "nemotron-3-ultra"
    config["model_provider"] = "alcf-inference-service-minerva-api"

    path.parent.mkdir(exist_ok=True, parents=True)
    with path.open("w") as f:
        tomlkit.dump(config, f)

    logging.info(f"Updated configuration at {path}")


@cli.command()
def configure(agent: Annotated[Literal["opencode", "codex"], typer.Argument()]) -> None:
    """
    Generates a configuration template for a given agent.
    """
    from .cli import _cli_state

    client = _cli_state["client"]
    endpoints = client.list_endpoints()

    auth = get_inference_authorizer()
    auth.ensure_valid_token()  # type: ignore[attr-defined]
    api_key = auth.access_token  # type: ignore[attr-defined]

    match agent:
        case "opencode":
            edit_opencode(client.base_url, api_key, endpoints)
        case "codex":
            edit_codex(client.base_url, api_key, endpoints)
