"""
`alcf-ai tasks`: discover and invoke non-LLM models through the generic
``tasks`` route.
"""

import json
import sys
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.table import Table

from ..client import InferenceClient
from ._shared import (
    JsonFlag,
    console,
    get_client,
    model_state,
    print_json,
    ready_deployments,
    styled_state,
    task_endpoints,
    with_startup_wait,
)

cli = typer.Typer(no_args_is_help=True)


def parse_params(params: list[str]) -> dict[str, Any]:
    """Parse ``key=value`` pairs; values are JSON when they parse, else strings."""
    parsed: dict[str, Any] = {}
    for item in params:
        key, sep, raw = item.partition("=")
        if not sep or not key:
            raise typer.BadParameter(f"Expected key=value, got {item!r}")
        try:
            parsed[key] = json.loads(raw)
        except json.JSONDecodeError:
            parsed[key] = raw
    return parsed


def load_body(body: str | None) -> dict[str, Any]:
    """A JSON object given inline, as ``@file``, or ``-`` for stdin."""
    if body is None:
        return {}
    if body == "-":
        text = sys.stdin.read()
    elif body.startswith("@"):
        text = Path(body[1:]).read_text()
    else:
        text = body
    data = json.loads(text)
    if not isinstance(data, dict):
        raise typer.BadParameter("The request body must be a JSON object.")
    return data


@cli.command("ls")
def tasks_ls(ctx: typer.Context, as_json: bool = JsonFlag) -> None:
    """List Models exposing non-LLM task endpoints."""
    models = [m for m in get_client(ctx).catalog.models() if task_endpoints(m)]
    if as_json:
        print_json(models)
        return

    table = Table(title="Task models")
    table.add_column("Model", style="bold")
    table.add_column("Task endpoints")
    table.add_column("State")
    table.add_column("Deployments", justify="right")
    for m in sorted(models, key=lambda m: m.name):
        ready, total = ready_deployments(m)
        table.add_row(
            m.name,
            ", ".join(task_endpoints(m)),
            styled_state(model_state(m)),
            f"{ready}/{total}",
        )
    console.print(table)


@cli.command("run")
def tasks_run(
    ctx: typer.Context,
    model: Annotated[str, typer.Argument(help="Model name or alias")],
    endpoint: Annotated[str, typer.Argument(help="Backend endpoint, e.g. 'process'")],
    body: Annotated[
        str | None,
        typer.Option(
            "--json", "-j", help="JSON object body: inline, @file, or - for stdin."
        ),
    ] = None,
    params: Annotated[
        list[str] | None,
        typer.Option("--param", "-p", help="Body field as key=value (JSON values ok)."),
    ] = None,
    deployment: Annotated[
        str | None,
        typer.Option("--deployment", "-d", help="Pin the request to one deployment."),
    ] = None,
    timeout: Annotated[
        float,
        typer.Option("--timeout", help="Max seconds to wait for a cold start."),
    ] = 1800,
    wait: Annotated[
        bool,
        typer.Option(
            "--wait/--no-wait", help="Wait through a cold start (shows the ETA)."
        ),
    ] = True,
) -> None:
    """
    Invoke a task endpoint on a non-LLM model and print the JSON result.

    The body fields are passed through verbatim to the backend's /v1/ENDPOINT.
    """
    client: InferenceClient = get_client(ctx)
    inference = client.deployment(deployment) if deployment else client.inference
    fields = {**load_body(body), **parse_params(params or [])}

    def run() -> dict[str, Any]:
        return inference.task(model, endpoint, **fields)

    print_json(with_startup_wait(run, model=model, wait=wait, timeout=timeout))
