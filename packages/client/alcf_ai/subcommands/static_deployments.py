import typer
from rich.console import Console
from rich.table import Table

from ._context import get_client

cli = typer.Typer(no_args_is_help=True)


@cli.command("ls")
def ls(ctx: typer.Context) -> None:
    """List visible StaticDeployments."""
    client = get_client(ctx)
    console = Console()

    table = Table(title="StaticDeployments")
    table.add_column("Name", style="bold")
    table.add_column("Cluster")
    table.add_column("Model")
    table.add_column("Upstream")
    table.add_column("Health")
    table.add_column("Last health check")

    for d in client.static_deployments.list():
        table.add_row(
            d.name,
            d.cluster_name,
            d.model_name,
            d.upstream_model_name,
            d.health.value,
            d.last_health_check.isoformat() if d.last_health_check else "-",
        )

    console.print(table)
