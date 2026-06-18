import typer
from rich import print

from ._context import get_client

cli = typer.Typer(no_args_is_help=True)


@cli.command()
def ls_jobs(ctx: typer.Context, cluster: str) -> None:
    """
    List ongoing jobs for a cluster.
    """
    client = get_client(ctx)
    jobs = client.clusters.get(cluster).get_jobs()
    print(jobs)
