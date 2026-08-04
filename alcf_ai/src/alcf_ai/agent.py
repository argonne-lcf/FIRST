import json
import logging
import typer
from pathlib import Path
from typing import Annotated, Literal

cli = typer.Typer(no_args_is_help=True)

ALLOWLIST = {
    "sophia": {"openai/gpt-oss-120b": {"limit": {"context": 65536, "output": 0}}},
    # Metis has a SN-specific sanitization issue w.r.t. tool call outputs, disable for now.
    # "metis": {"gpt-oss-120b": {"limits": {"context": 65536, "output": 0}}},
    "minerva": {"nemotron-3-ultra": {"limit": {"context": 96000, "output": 0}}},
}


def edit_opencode(service_url: str, endpoints) -> None:
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

            providers[f"inference-service-{cluster_name}-{framework_name}"] = {
                "name": f"ALCF Inference Service ({cluster_name.title()}, {'vLLM' if framework_name == 'vllm' else 'Direct API'})",
                "npm": "@ai-sdk/openai-compatible",
                "options": {
                    "baseURL": f"{service_url}{cluster_name}/{framework_name}/v1",
                    "apiKey": "{env:ALCF_AI_TOKEN}",
                },
                "models": models,
            }

    config["provider"] = providers
    path.parent.mkdir(exist_ok=True, parents=True)
    with path.open("w") as f:
        json.dump(config, f, indent=2)

    logging.info(f"Updated configuration at {path}")


@cli.command()
def configure(agent: Annotated[Literal["opencode"], typer.Argument()]) -> None:
    """
    Generates a configuration template for a given agent.
    """
    from .cli import _cli_state

    client = _cli_state["client"]
    endpoints = client.list_endpoints()

    match agent:
        case "opencode":
            edit_opencode(client.base_url, endpoints)
