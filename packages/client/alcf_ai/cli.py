import logging
import sys
from pathlib import Path
from typing import Any, TypedDict

import typer
from openai.types.chat.chat_completion_message_param import ChatCompletionMessageParam
from rich import print
from rich.console import Console
from rich.logging import RichHandler
from rich.markdown import Markdown
from typer import Typer

from .auth import cli as auth_cli
from .client import InferenceClient
from .sam3 import cli as sam3_cli

logger = logging.getLogger(__name__)
console = Console(stderr=True)


class CliState(TypedDict, total=False):
    client: InferenceClient


cli = Typer(no_args_is_help=True)
_cli_state: CliState = {}

cli.add_typer(auth_cli, name="auth", help="Login and get access tokens")
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
    prompt: str = typer.Argument("", help="The prompt to send"),
    model: str = typer.Option(
        "meta-llama/Llama-4-Scout-17B-16E-Instruct", "--model", "-m"
    ),
    system: str | None = typer.Option(
        None,
        "--system",
        "-s",
        help="System prompt (e.g. an instruction to apply to the input)",
    ),
    stream: bool = typer.Option(False, "--stream/--no-stream"),
    temperature: float | None = typer.Option(None, "--temp", "-t"),
    max_tokens: int | None = typer.Option(None, "--max-tokens", "-n"),
    cluster: str = typer.Option("sophia", "--cluster", "-c"),
    input_file: Path | None = typer.Option(
        None, "--input-file", "-i", help="Read additional user input from this file"
    ),
) -> None:
    """Send a prompt to an LLM and print the response.

    The user message is built by concatenating, in order: piped stdin (if any),
    the contents of --input-file (if any), and the positional PROMPT argument.
    """
    client = _cli_state["client"]
    oai = client.clusters(cluster).openai

    parts: list[str] = []
    if not sys.stdin.isatty():
        stdin_data = sys.stdin.read()
        if stdin_data.strip():
            parts.append(stdin_data)
    if input_file is not None:
        parts.append(input_file.read_text())
    if prompt.strip():
        parts.append(prompt)

    user_content = "\n\n".join(p.rstrip() for p in parts).strip()

    if not user_content and not system:
        print(
            "You must provide a prompt via the positional argument, "
            "--input-file, piped stdin, or --system."
        )
        raise typer.Abort()

    messages: list[ChatCompletionMessageParam] = []
    if system:
        messages.append({"role": "system", "content": system})
    if user_content:
        messages.append({"role": "user", "content": user_content})

    response: Any
    all_chunks = []
    if stream:
        collected = []
        with console.status("[dim]Thinking…[/dim]", spinner="dots"):
            response = oai.chat.completions.create(
                model=model,
                messages=messages,
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
            print("Failed to collect the chat completions streaming response.")
            print(all_chunks)
            raise typer.Abort()

        print("")

    else:
        with console.status("[dim]Thinking…[/dim]", spinner="dots"):
            response = oai.chat.completions.create(
                model=model,
                messages=messages,
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
