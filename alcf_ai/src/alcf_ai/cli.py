import logging
from typing import Any, TypedDict

import typer
from rich import print
from rich.console import Console
from rich.logging import RichHandler
from rich.markdown import Markdown
from typer import Typer

from .auth import cli as auth_cli
from .client import InferenceClient
from .d3_triton import cli as d3_triton_cli
from .sam3 import cli as sam3_cli

logger = logging.getLogger(__name__)
console = Console(stderr=True)


class CliState(TypedDict, total=False):
    client: InferenceClient


cli = Typer(no_args_is_help=True)
_cli_state: CliState = {}

cli.add_typer(auth_cli, name="auth", help="Login and get access tokens")
cli.add_typer(d3_triton_cli, name="d3-triton", help="Use the Triton HEP inference service")
cli.add_typer(sam3_cli, name="sam3", help="Use the SAM3 image segmentation service")


@cli.callback()
def main(
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
    _cli_state["client"] = InferenceClient(base_url)
    logger.debug(f"Using client: {_cli_state['client']}")


@cli.command()
def ls_endpoints() -> None:
    """
    List all endpoints available across clusters
    """
    client = _cli_state["client"]
    print(client.list_endpoints())


@cli.command()
def ls_jobs(cluster: str) -> None:
    """
    List ongoing jobs for a cluster
    """
    client = _cli_state["client"]

    jobs = client.clusters(cluster).get_jobs()
    print(jobs)


@cli.command()
def chat(
    prompt: str = typer.Argument(..., help="The prompt to send"),
    model: str = typer.Option(
        "meta-llama/Llama-4-Scout-17B-16E-Instruct", "--model", "-m"
    ),
    stream: bool = typer.Option(True, "--stream/--no-stream", "-s/-S"),
    temperature: float = typer.Option(0.7, "--temp", "-t"),
    max_tokens: int = typer.Option(1024, "--max-tokens", "-n"),
    cluster: str = typer.Option("sophia", "--cluster", "-c"),
) -> None:
    """Send a prompt to an LLM and print the response."""
    client = _cli_state["client"]
    oai = client.clusters(cluster).openai

    response: Any
    all_chunks = []
    if stream:
        collected = []
        with console.status("[dim]Thinking…[/dim]", spinner="dots"):
            response = oai.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=temperature,
                max_tokens=max_tokens,
                stream=True,
            )

        for chunk in response:
            all_chunks.append(chunk)
            if chunk.choices and chunk.choices[0].delta.content:
                token = chunk.choices[0].delta.content
                print(token, end="")
                collected.append(token)

        if not collected:
            print(all_chunks)

        print("")

    else:
        with console.status("[dim]Thinking…[/dim]", spinner="dots"):
            response = oai.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=temperature,
                max_tokens=max_tokens,
            )
        text = response.choices[0].message.content
        print(Markdown(text))


@cli.command()
def version() -> None:
    from importlib.metadata import version

    print(version("alcf-ai"))


if __name__ == "__main__":
    cli()
