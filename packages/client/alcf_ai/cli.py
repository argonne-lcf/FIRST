import logging
import sys
from typing import Any

import httpx
import typer
from alcf_tokens.auth import AuthError
from alcf_tokens.cli import cli as auth_cli
from alcf_tokens.cli import get_token, login
from alcf_tokens.globus_transfer_utils import COLLECTION_ALIASES
from rich.logging import RichHandler
from rich.panel import Panel
from typer import Typer

from first_common.errors import FirstError

from .client import InferenceClient
from .commands._shared import CliContext, err_console
from .commands.agent import cli as agent_cli
from .commands.catalog import (
    access_groups_cli,
    clusters_cli,
    deployments_cli,
    models_cli,
)
from .commands.chat import chat, embed
from .commands.dinov3 import cli as dinov3_cli
from .commands.sam3 import cli as sam3_cli
from .commands.tasks import cli as tasks_cli
from .commands.whoami import whoami

logger = logging.getLogger(__name__)

cli = Typer(no_args_is_help=True)

cli.add_typer(auth_cli, name="auth", help="Login and get access tokens")
cli.add_typer(models_cli, name="models", help="Discover and warm up Models")
cli.add_typer(deployments_cli, name="deployments", help="Inspect model deployments")
cli.add_typer(clusters_cli, name="clusters", help="Inspect cluster state")
cli.add_typer(access_groups_cli, name="access-groups", help="Inspect AccessGroups")
cli.add_typer(tasks_cli, name="tasks", help="Run non-LLM task models")
cli.add_typer(sam3_cli, name="sam3", help="SAM3 promptable image segmentation")
cli.add_typer(
    dinov3_cli, name="dinov3", help="DINOv3 semantic segmentation of image folders"
)
cli.add_typer(
    agent_cli,
    name="agent",
    help="Configure coding agents (opencode, codex, pi, claude)",
)
cli.command(name="whoami")(whoami)
cli.command(name="chat")(chat)
cli.command(name="embed")(embed)


@cli.callback()
def _root(
    ctx: typer.Context,
    base_url: str | None = None,
    log_level: str = "INFO",
) -> None:
    """
    ALCF Inference Gateway CLI
    """
    logging.basicConfig(
        level=log_level,
        format="%(name)s:%(lineno)d %(message)s",
        handlers=[RichHandler(console=err_console)],
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    client = InferenceClient(base_url)
    ctx.obj = CliContext(client=client)
    logger.debug(f"Using client: {client}")


@cli.command()
def version() -> None:
    from importlib.metadata import version

    print(version("alcf-ai"))


@auth_cli.command("login")
def login_override(
    authorize_transfer: list[str] = typer.Option(
        default=[],
        help=(
            "A Globus collection UUID (or alias) to authorize transfer against. "
            "Repeat the flag to authorize multiple collections. "
            "Append :data_access to a UUID to request the data_access dependency if needed, "
            "and/or :https to authorize direct HTTPS uploads and downloads on the collection. "
            f"Known aliases: {', '.join(sorted(COLLECTION_ALIASES))}."
        ),
    ),
) -> None:
    """
    Alias for `alcf-tokens login inference`
    """
    return login(service="inference", authorize_transfer=authorize_transfer)


@auth_cli.command("get-access-token")
def get_access_token() -> None:
    """
    Alias for `alcf-ai auth get-token inference`
    """
    return get_token("inference")


def main() -> None:
    """
    Entry point used by the `alcf-ai` script.

    Catches expected error types so the user sees a clean, formatted message
    instead of a multi-frame Rich traceback. Use --log-level=DEBUG to see the
    full traceback when diagnosing client bugs.
    """
    try:
        cli()
    except FirstError as exc:
        _print_error(f"Error ({exc.status_code})", str(exc), info=exc.info or None)
        sys.exit(1)
    except httpx.HTTPError as exc:
        _print_error("HTTP Error", f"{type(exc).__name__}: {exc}")
        sys.exit(1)
    except (AuthError, TimeoutError) as exc:
        _print_error(type(exc).__name__, str(exc))
        sys.exit(1)


def _print_error(title: str, message: str, info: dict[str, Any] | None = None) -> None:
    body = message.strip() or "(no message)"
    if info:
        body += "\n\n" + "\n".join(f"{k}: {v}" for k, v in info.items())
    err_console.print(Panel(body, title=title, border_style="red"))
    if logger.isEnabledFor(logging.DEBUG):
        err_console.print_exception()


if __name__ == "__main__":
    main()
