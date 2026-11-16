import os
import subprocess
from typing import Annotated, Literal

import typer
from alcf_tokens.auth import get_access_token
from click.core import ParameterSource
from rich.table import Table

from ..agent import (
    _AUTO_APPROVE_OVERWRITES,
    AGENT_ENDPOINTS,
    CONFIGURATORS,
    SESSION_ENV,
    Agent,
    AgentModel,
    ConfigLoadError,
    _codex_profile_path,
    aider_environment,
    claude_environment,
    claude_profile_path,
    codex_launch,
    codex_version,
    configure_claude,
    configure_codex,
    opencode_environment,
    require_codex_profile_version,
    select_models,
    token_helper_command,
)
from ..client import INFERENCE_SERVICE
from ._shared import console, default_model_name, err_console, fmt_int, get_client

cli = typer.Typer(no_args_is_help=True)


def _api(agent: Agent, m: AgentModel) -> str:
    if agent == "claude":
        return "messages"
    return "responses" if m.supports_responses else "chat"


LaunchAgent = Literal["claude", "opencode", "codex", "aider"]


def _is_default_parameter(ctx: typer.Context, name: str) -> bool:
    """Compare across Typer's vendored and Click's external ParameterSource enums."""
    source = ctx.get_parameter_source(name)
    return source is None or source.name == ParameterSource.DEFAULT.name


def _launch_model(ctx: typer.Context, agent: LaunchAgent, model: str) -> AgentModel:
    client = get_client(ctx)
    endpoints = AGENT_ENDPOINTS[agent]
    try:
        selected = select_models(client.catalog.models(), [model], endpoints)
    except ValueError as err:
        raise typer.BadParameter(str(err), param_hint="--default-model") from err
    if not selected:
        raise typer.BadParameter(
            f"{model!r} does not serve {' or '.join(sorted(endpoints))}",
            param_hint="--default-model",
        )
    return selected[0]


@cli.command(
    context_settings={
        "allow_extra_args": True,
        "ignore_unknown_options": True,
    }
)
def run(
    ctx: typer.Context,
    agent: Annotated[LaunchAgent, typer.Argument(help="Which coding agent to run.")],
    model: Annotated[
        str | None,
        typer.Option(
            "--default-model",
            "-m",
            help="Model for the agent (default: $ALCF_AI_MODEL or mistral-large-4)",
        ),
    ] = None,
    profile: Annotated[
        str | None,
        typer.Option(
            "--profile",
            help="Load a Claude or Codex settings profile instead of one-off overrides",
        ),
    ] = None,
) -> None:
    """Run an agent with ALCF configuration, without editing config files.

    Additional arguments after ``--`` are forwarded to the agent.
    """
    client = get_client(ctx)
    if profile is not None:
        if agent not in ("claude", "codex"):
            raise typer.BadParameter(
                "Profiles are only supported for Claude and Codex",
                param_hint="--profile",
            )
        if not _is_default_parameter(ctx, "model"):
            raise typer.BadParameter(
                "--profile cannot be combined with --default-model; "
                "configure the profile to change its model",
                param_hint="--profile",
            )

    model_name = model or default_model_name()
    env = os.environ.copy()
    env.setdefault(SESSION_ENV, "default")
    launch_args: list[str] = []
    match agent:
        case "claude":
            if profile is not None:
                profile_path = claude_profile_path(profile)
                if not profile_path.is_file():
                    raise typer.BadParameter(
                        f"Claude profile {profile!r} does not exist at {profile_path}; "
                        f"run `alcf-ai agent configure claude --profile {profile}` first",
                        param_hint="--profile",
                    )
                launch_args = ["--settings", str(profile_path)]
            else:
                selected = _launch_model(ctx, agent, model_name)
                env.update(
                    claude_environment(
                        str(client.base_url),
                        get_access_token(INFERENCE_SERVICE),
                        selected,
                    )
                )
        case "opencode":
            selected = _launch_model(ctx, agent, model_name)
            env.update(
                opencode_environment(
                    str(client.base_url),
                    get_access_token(INFERENCE_SERVICE),
                    selected,
                    token_helper=token_helper_command(),
                )
            )
        case "codex":
            try:
                if profile is not None:
                    profile_path = _codex_profile_path(profile)
                    if not profile_path.is_file():
                        raise typer.BadParameter(
                            f"Codex profile {profile!r} does not exist at "
                            f"{profile_path}; run `alcf-ai agent configure codex "
                            f"--profile {profile}` first",
                            param_hint="--profile",
                        )
                    require_codex_profile_version(codex_version())
                    launch_args = ["--profile", profile]
                else:
                    selected = _launch_model(ctx, agent, model_name)
                    codex_env, launch_args = codex_launch(
                        str(client.base_url),
                        get_access_token(INFERENCE_SERVICE),
                        selected,
                        version=codex_version(),
                    )
                    env.update(codex_env)
            except (
                OSError,
                RuntimeError,
                subprocess.SubprocessError,
                ValueError,
            ) as err:
                typer.echo(f"Could not configure codex: {err}", err=True)
                raise typer.Exit(1) from err
        case "aider":
            selected = _launch_model(ctx, agent, model_name)
            env.update(
                aider_environment(
                    str(client.base_url),
                    get_access_token(INFERENCE_SERVICE),
                    selected,
                )
            )

    try:
        os.execvpe(agent, [agent, *launch_args, *ctx.args], env)
    except OSError as err:
        typer.echo(f"Could not execute {agent}: {err}", err=True)
        raise typer.Exit(1) from err


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
    profile: Annotated[
        str,
        typer.Option(
            "--profile",
            help="Settings profile name (Claude and Codex)",
        ),
    ] = "alcf",
    no_profile: Annotated[
        bool,
        typer.Option(
            "--no-profile",
            help="Update the base config instead of creating a Claude/Codex profile",
        ),
    ] = False,
    yes: Annotated[
        bool,
        typer.Option(
            "--yes",
            "-y",
            help="Approve all configuration file overwrites without prompting",
        ),
    ] = False,
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
    if no_profile:
        if agent not in ("claude", "codex"):
            raise typer.BadParameter(
                "--no-profile is only supported for Claude and Codex",
                param_hint="--no-profile",
            )
        if not _is_default_parameter(ctx, "profile"):
            raise typer.BadParameter(
                "--no-profile cannot be combined with --profile",
                param_hint="--no-profile",
            )
    profile_name = None if no_profile else profile

    client = get_client(ctx)
    endpoints = AGENT_ENDPOINTS[agent]
    catalog = client.catalog.models()
    selected = select_models(catalog, models or [], endpoints)
    if models is None:
        preferred = default_model_name()
        preferred_ids = {
            m.name for m in catalog if m.name == preferred or preferred in m.aliases
        }
        selected.sort(key=lambda m: m.id not in preferred_ids)
    if not selected:
        raise typer.BadParameter(
            f"No models serving {' or '.join(sorted(endpoints))} to configure."
        )

    token_helper = token_helper_command()
    api_key = get_access_token(INFERENCE_SERVICE)
    approval = _AUTO_APPROVE_OVERWRITES.set(yes)
    try:
        if agent == "codex":
            path = configure_codex(
                str(client.base_url),
                api_key,
                selected,
                token_helper=token_helper,
                profile=profile_name,
            )
        elif agent == "claude":
            path = configure_claude(
                str(client.base_url),
                api_key,
                selected,
                token_helper=token_helper,
                profile=profile_name,
            )
        else:
            path = CONFIGURATORS[agent](
                str(client.base_url),
                api_key,
                selected,
                token_helper=token_helper,
            )
    except ConfigLoadError as err:
        err_console.print(f"[red]{err}[/]")
        raise typer.Exit(1) from err
    finally:
        _AUTO_APPROVE_OVERWRITES.reset(approval)
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
    if profile_name is not None and agent in ("claude", "codex"):
        console.print(
            f"Load it with [bold]alcf-ai agent run {agent} --profile {profile_name}[/]"
        )
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
