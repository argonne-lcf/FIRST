import logging
import sys
from pathlib import Path
from typing import Any

import typer
from openai.types.chat.chat_completion_message_param import ChatCompletionMessageParam
from rich import print
from rich.console import Console
from rich.markdown import Markdown

from ._context import get_client

logger = logging.getLogger(__name__)
console = Console(stderr=True)


def chat(
    ctx: typer.Context,
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
    client = get_client(ctx)
    oai = client.clusters.get_handle(cluster).openai

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
