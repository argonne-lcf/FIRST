import logging
import sys
from typing import Any

import httpx
import typer
from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from typer import Typer

from first_common.errors import FirstError

from .client import InferenceClient
from .subcommands._context import CliContext
from .subcommands.access_groups import cli as access_groups_cli
from .subcommands.admin import cli as admin_cli
from .subcommands.auth import cli as auth_cli
from .subcommands.chat import chat as chat_command
from .subcommands.clusters import cli as clusters_cli
from .subcommands.endpoints import cli as endpoints_cli
from .subcommands.models import cli as models_cli
from .subcommands.pilot_deployments import cli as pilot_deployments_cli
from .subcommands.sam3 import cli as sam3_cli
from .subcommands.static_deployments import cli as static_deployments_cli

logger = logging.getLogger(__name__)
console = Console(stderr=True)

cli = Typer(no_args_is_help=True)

cli.add_typer(auth_cli, name="auth", help="Login and get access tokens")
cli.add_typer(sam3_cli, name="sam3", help="Use the SAM3 image segmentation service")
cli.add_typer(admin_cli, name="admin", help="Manage Inference Gateway Resources")
cli.add_typer(endpoints_cli, name="endpoints", help="Inspect available API endpoints")
cli.add_typer(clusters_cli, name="clusters", help="Inspect cluster state")
cli.add_typer(
    access_groups_cli, name="access-groups", help="Inspect AccessGroup resources"
)
cli.add_typer(models_cli, name="models", help="Inspect Model resources")
cli.add_typer(
    pilot_deployments_cli,
    name="pilot-deployments",
    help="Inspect PilotDeployment resources",
)
cli.add_typer(
    static_deployments_cli,
    name="static-deployments",
    help="Inspect StaticDeployment resources",
)
cli.command(name="chat")(chat_command)


@cli.callback()
def _root(
    ctx: typer.Context,
    base_url: str | None = None,
    log_level: str = "INFO",
) -> None:
    """
    Inference Gateway CLI
    """
    logging.basicConfig(
        level=log_level,
        format="%(name)s:%(lineno)d %(message)s",
        handlers=[RichHandler(console=console)],
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    client = InferenceClient(base_url)
    ctx.obj = CliContext(client=client)
    logger.debug(f"Using client: {client}")


@cli.command()
def version() -> None:
    from importlib.metadata import version

    print(version("alcf-ai"))


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


def _print_error(title: str, message: str, info: dict[str, Any] | None = None) -> None:
    body = message.strip() or "(no message)"
    if info:
        body += "\n\n" + "\n".join(f"{k}: {v}" for k, v in info.items())
    console.print(Panel(body, title=title, border_style="red"))
    if logger.isEnabledFor(logging.DEBUG):
        console.print_exception()


if __name__ == "__main__":
    main()
