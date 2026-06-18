import typer
from rich import print

from ._context import get_client

cli = typer.Typer(no_args_is_help=True)


@cli.command("ls")
def ls(ctx: typer.Context) -> None:
    """
    List all endpoints available across clusters.
    """
    client = get_client(ctx)
    print(client.endpoints.list())
