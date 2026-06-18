import logging

import typer
from rich.console import Console
from rich.logging import RichHandler
from typer import Typer

from .client import InferenceClient
from .subcommands._context import CliContext
from .subcommands.admin import cli as admin_cli
from .subcommands.auth import cli as auth_cli
from .subcommands.chat import chat as chat_command
from .subcommands.clusters import cli as clusters_cli
from .subcommands.endpoints import cli as endpoints_cli
from .subcommands.sam3 import cli as sam3_cli

logger = logging.getLogger(__name__)
console = Console(stderr=True)

cli = Typer(no_args_is_help=True)

cli.add_typer(auth_cli, name="auth", help="Login and get access tokens")
cli.add_typer(sam3_cli, name="sam3", help="Use the SAM3 image segmentation service")
cli.add_typer(admin_cli, name="admin", help="Manage Inference Gateway Resources")
cli.add_typer(endpoints_cli, name="endpoints", help="Inspect available API endpoints")
cli.add_typer(clusters_cli, name="clusters", help="Inspect cluster state")
cli.command(name="chat")(chat_command)


@cli.callback()
def main(
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


if __name__ == "__main__":
    cli()
