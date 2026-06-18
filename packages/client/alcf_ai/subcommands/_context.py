from dataclasses import dataclass

import typer

from ..client import InferenceClient


@dataclass
class CliContext:
    client: InferenceClient


def get_client(ctx: typer.Context) -> InferenceClient:
    """Return the InferenceClient stashed on the Typer context by cli.main()."""
    obj: CliContext = ctx.obj
    return obj.client
