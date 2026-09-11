from typing import Annotated

import typer
from alcf_tokens.auth import get_access_token
from rich.table import Table

from ..agent import (
    AGENT_ENDPOINTS,
    CONFIGURATORS,
    Agent,
    AgentModel,
    ConfigLoadError,
    select_models,
    token_helper_command,
)
from ..client import INFERENCE_SERVICE
from ._shared import console, err_console, fmt_int, get_client

cli = typer.Typer(no_args_is_help=True)


def _api(agent: Agent, m: AgentModel) -> str:
    if agent == "claude":
        return "messages"
    return "responses" if m.supports_responses else "chat"


@cli.command()
def configure(
    ctx: typer.Context,
    agent: Annotated[Agent, typer.Argument(help="Which coding agent to configure.")],
    models: Annotated[
        list[str] | None,
        typer.Option(
            "--model",
            "-m",
            help=(
                "Only include this model (repeatable); the first is the default. "
                "Default: every LLM you can use."
            ),
        ),
    ] = None,
) -> None:
    """
    Add the gateway as a model provider in a coding agent's configuration.

    Model entries carry the context window and capabilities the catalog
    publishes.  The agent runs `alcf-tokens get-token inference` (or
    `alcf-ai auth get-access-token`) to refresh its access token; if neither
    can be found, the current token is embedded and you must re-run this
    command after it expires.

    Requests carry an X-ALCF-Session-ID header from $ALCF_SESSION_ID so
    backends can reuse prompt caches.  For claude, export
    ANTHROPIC_CUSTOM_HEADERS="X-ALCF-Session-ID: <id>" instead.
    """
    client = get_client(ctx)
    endpoints = AGENT_ENDPOINTS[agent]
    selected = select_models(client.catalog.models(), models or [], endpoints)
    if not selected:
        raise typer.BadParameter(
            f"No models serving {' or '.join(sorted(endpoints))} to configure."
        )

    token_helper = token_helper_command()
    try:
        path = CONFIGURATORS[agent](
            str(client.base_url),
            get_access_token(INFERENCE_SERVICE),
            selected,
            token_helper=token_helper,
        )
    except ConfigLoadError as err:
        err_console.print(f"[red]{err}[/]")
        raise typer.Exit(1) from err

    table = Table(title=f"Configured {agent}", title_justify="left")
    table.add_column("Model", style="bold")
    table.add_column("Context", justify="right")
    table.add_column("API")
    table.add_column("Tools")
    table.add_column("Reasoning")
    for m in selected:
        table.add_row(
            m.id,
            fmt_int(m.context_window),
            _api(agent, m),
            "yes" if m.capabilities.tool_calling.supported else "—",
            "yes" if m.capabilities.reasoning.supported else "—",
        )
    console.print(table)
    console.print(f"Updated [bold]{path}[/]")
    if token_helper is None:
        console.print(
            "[yellow]Could not find a token helper (alcf-tokens or alcf-ai); "
            "embedded the current access token instead. Re-run this command "
            "when it expires.[/]"
        )
    else:
        console.print(
            "Configured the agent to refresh access tokens automatically; "
            "run `alcf-tokens login` if the refresh token expires."
        )
