import typer
from rich.table import Table

from ._shared import JsonFlag, console, get_client, print_json


def whoami(ctx: typer.Context, as_json: bool = JsonFlag) -> None:
    """Show the identity the gateway sees you as."""
    user = get_client(ctx).whoami()
    if as_json:
        print_json(user)
        return

    table = Table(show_header=False, box=None)
    table.add_column(style="bold")
    table.add_column()
    table.add_row("Name", user.name)
    table.add_row("Username", user.username)
    table.add_row("Identity", user.id)
    table.add_row("Identity provider", user.idp_name)
    table.add_row("Groups", "\n".join(user.user_group_uuids) or "—")
    console.print(table)
