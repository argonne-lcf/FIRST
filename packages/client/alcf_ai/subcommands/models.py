import typer
from rich.console import Console
from rich.pretty import Pretty
from rich.table import Table

from first_common.errors import FirstError

from ._context import get_client

cli = typer.Typer(no_args_is_help=True)


@cli.command("ls")
def ls(ctx: typer.Context) -> None:
    """List visible Models."""
    client = get_client(ctx)
    console = Console()

    table = Table(title="Models")
    table.add_column("Name", style="bold")
    table.add_column("UID", justify="right")
    table.add_column("Access group")
    table.add_column("Endpoints")
    table.add_column("Pilot deployments", justify="right")
    table.add_column("Static deployments", justify="right")

    for m in client.models.list():
        table.add_row(
            m.name,
            str(m.uid),
            m.access_group_name,
            ", ".join(m.supported_endpoints) or "-",
            str(len(m.pilot_deployments)),
            str(len(m.static_deployments)),
        )

    console.print(table)


@cli.command("get")
def get(ctx: typer.Context, name: str) -> None:
    """List visible Models."""
    client = get_client(ctx)
    console = Console()

    table = Table(title="Models")
    table.add_column("Name", style="bold")
    table.add_column("UID", justify="right")
    table.add_column("Access group")
    table.add_column("Endpoints")
    table.add_column("Pilot deployments", justify="right")
    table.add_column("Static deployments", justify="right")

    model = next((m for m in client.models.list() if m.name == name), None)
    if model is None:
        raise FirstError(f"Model {name=} not found")

    console.print(Pretty(model.model_dump(mode="json")))
