import typer
from rich import print
from rich.console import Console
from rich.pretty import Pretty
from rich.table import Table

from ._context import get_client

cli = typer.Typer(no_args_is_help=True)


@cli.command("ls")
def ls(ctx: typer.Context) -> None:
    """List all configured Clusters."""
    client = get_client(ctx)
    console = Console()

    table = Table(title="Clusters")
    table.add_column("Name", style="bold")
    table.add_column("UID", justify="right")
    table.add_column("Status")
    table.add_column("Last status check")

    for c in client.clusters.list():
        table.add_row(
            c.name,
            str(c.uid),
            c.status.value,
            c.last_status_check.isoformat() if c.last_status_check else "-",
        )

    console.print(table)


@cli.command("get")
def get(ctx: typer.Context, name: str) -> None:
    """Show a Cluster with its pilot jobs.  (Admin-only.)"""
    client = get_client(ctx)
    Console().print(Pretty(client.clusters.get(name).model_dump(mode="json")))


@cli.command()
def ls_jobs(ctx: typer.Context, cluster: str) -> None:
    """List ongoing jobs for a cluster."""
    client = get_client(ctx)
    jobs = client.clusters.get_handle(cluster).get_jobs()
    print(jobs)
