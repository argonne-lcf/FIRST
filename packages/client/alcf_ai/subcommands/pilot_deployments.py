import typer
from rich.console import Console
from rich.pretty import Pretty
from rich.table import Table

from ._context import get_client

cli = typer.Typer(no_args_is_help=True)


@cli.command("ls")
def ls(ctx: typer.Context) -> None:
    """List visible PilotDeployments."""
    client = get_client(ctx)
    console = Console()

    table = Table(title="PilotDeployments")
    table.add_column("Name", style="bold")
    table.add_column("Cluster")
    table.add_column("Model")
    table.add_column("Health")
    table.add_column("Desired", justify="right")
    table.add_column("Launch fails", justify="right")
    table.add_column("Last health check")

    for d in client.pilot_deployments.list():
        table.add_row(
            d.name,
            d.cluster_name,
            d.model_name,
            d.health.value,
            str(d.desired_replicas),
            str(d.consecutive_launch_failures),
            d.last_health_check.isoformat() if d.last_health_check else "-",
        )

    console.print(table)


@cli.command("get")
def get(ctx: typer.Context, name: str) -> None:
    """Show a single PilotDeployment with its replicas."""
    client = get_client(ctx)
    deployment = client.pilot_deployments.get(name)
    Console().print(Pretty(deployment.model_dump(mode="json")))
