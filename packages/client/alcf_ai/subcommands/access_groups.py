import typer
from rich.console import Console
from rich.table import Table

from ._context import get_client

cli = typer.Typer(no_args_is_help=True)


@cli.command("ls")
def ls(ctx: typer.Context) -> None:
    """List visible AccessGroups."""
    client = get_client(ctx)
    console = Console()

    table = Table(title="AccessGroups")
    table.add_column("Name", style="bold")
    table.add_column("UID", justify="right")
    table.add_column("Allowed groups")
    table.add_column("Allowed domains")

    for g in client.access_groups.list():
        table.add_row(
            g.name,
            str(g.uid),
            ", ".join(g.allowed_groups) or "-",
            ", ".join(g.allowed_domains) or "-",
        )

    console.print(table)
