"""
`alcf-ai chat` and `alcf-ai embed`: one-shot LLM requests from the terminal.
"""

import os
import sys
from enum import Enum
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.markdown import Markdown

from first_common.errors import NotFound
from first_common.schema.resources.read import ModelSummary

from ..client import InferenceClient
from ..inference import (
    InferenceAPI,
    response_reasoning,
    response_text,
    response_usage,
    stream_delta,
)
from ._shared import console, err_console, llm_endpoints, print_json, with_startup_wait

# Preference order when a Model supports several LLM dialects.
_CHAT_ENDPOINTS = ("chat/completions", "responses", "messages")


class Api(str, Enum):
    auto = "auto"
    chat = "chat"
    responses = "responses"
    messages = "messages"


_API_ENDPOINT = {
    Api.chat: "chat/completions",
    Api.responses: "responses",
    Api.messages: "messages",
}


def resolve_target(
    client: InferenceClient, model: str | None, api: Api
) -> tuple[str, str]:
    """
    Choose the (model, endpoint) for a chat request.

    An explicit model + API needs no lookup.  Otherwise the catalog is consulted:
    the model defaults to ``$ALCF_AI_MODEL`` or the first chat-capable Model,
    and the API to the first dialect the Model supports.
    """
    if model and api != Api.auto:
        return model, _API_ENDPOINT[api]

    models = client.catalog.models()
    model = model or os.environ.get("ALCF_AI_MODEL") or None
    chosen: ModelSummary | None
    if model:
        chosen = next(
            (m for m in models if m.name == model or model in m.aliases), None
        )
        if chosen is None:
            raise NotFound(f"Model {model!r} not found (or not visible to you).")
    else:
        chosen = next(
            (
                m
                for m in sorted(models, key=lambda m: m.name)
                if any(e in _CHAT_ENDPOINTS for e in m.supported_endpoints)
            ),
            None,
        )
        if chosen is None:
            raise NotFound("No chat-capable Model is visible to you.")
        err_console.print(
            f"[dim]Using model {chosen.name} (choose another with --model)[/]"
        )

    if api != Api.auto:
        return chosen.name, _API_ENDPOINT[api]
    endpoint = next(
        (e for e in _CHAT_ENDPOINTS if e in chosen.supported_endpoints), None
    )
    if endpoint is None:
        raise typer.BadParameter(
            f"{chosen.name} supports no chat API (endpoints: "
            f"{', '.join(llm_endpoints(chosen)) or 'none'})."
        )
    return chosen.name, endpoint


def build_payload(
    endpoint: str,
    model: str,
    user_content: str,
    *,
    system: str | None = None,
    temperature: float | None = None,
    max_tokens: int | None = None,
) -> dict[str, Any]:
    """Translate a simple (system, user) prompt into the endpoint's dialect."""
    payload: dict[str, Any] = {"model": model}
    if temperature is not None:
        payload["temperature"] = temperature

    if endpoint == "chat/completions":
        messages: list[dict[str, str]] = []
        if system:
            messages.append({"role": "system", "content": system})
        if user_content:
            messages.append({"role": "user", "content": user_content})
        payload["messages"] = messages
        if max_tokens is not None:
            payload["max_tokens"] = max_tokens
    elif endpoint == "responses":
        payload["input"] = user_content
        if system:
            payload["instructions"] = system
        if max_tokens is not None:
            payload["max_output_tokens"] = max_tokens
    elif endpoint == "messages":
        payload["messages"] = [{"role": "user", "content": user_content or "."}]
        if system:
            payload["system"] = system
        payload["max_tokens"] = max_tokens or 1024
    else:
        raise ValueError(f"Unsupported endpoint {endpoint!r}")
    return payload


def _gather_prompt(prompt: str, input_file: Path | None) -> str:
    parts: list[str] = []
    if not sys.stdin.isatty():
        stdin_data = sys.stdin.read()
        if stdin_data.strip():
            parts.append(stdin_data)
    if input_file is not None:
        parts.append(input_file.read_text())
    if prompt.strip():
        parts.append(prompt)
    return "\n\n".join(p.rstrip() for p in parts).strip()


def _print_usage(usage: dict[str, Any] | None) -> None:
    if not usage:
        return
    tokens_in = usage.get("prompt_tokens", usage.get("input_tokens"))
    tokens_out = usage.get("completion_tokens", usage.get("output_tokens"))
    err_console.print(f"[dim]tokens: {tokens_in} in · {tokens_out} out[/]")


def _run_streaming(
    api: InferenceAPI, endpoint: str, payload: dict[str, Any], show_reasoning: bool
) -> None:
    usage: dict[str, Any] | None = None
    in_reasoning = False
    for event in api.stream(endpoint, payload):
        usage = response_usage(endpoint, event.json()) or usage
        delta = stream_delta(endpoint, event)
        if delta is None:
            continue
        kind, text = delta
        if kind == "reasoning":
            if show_reasoning:
                in_reasoning = True
                console.print(text, end="", style="dim", markup=False, highlight=False)
            continue
        if in_reasoning:
            console.print()  # separate reasoning from the answer
            in_reasoning = False
        console.print(text, end="", markup=False, highlight=False, soft_wrap=True)
    console.print()
    _print_usage(usage)


def _run_unary(
    api: InferenceAPI,
    endpoint: str,
    payload: dict[str, Any],
    show_reasoning: bool,
    as_json: bool,
) -> None:
    body = api.post(endpoint, payload)
    if as_json:
        print_json(body)
        return
    if show_reasoning and (reasoning := response_reasoning(endpoint, body)):
        console.print(reasoning, style="dim", markup=False, highlight=False)
        console.print()
    console.print(Markdown(response_text(endpoint, body)))
    _print_usage(response_usage(endpoint, body))


def chat(
    ctx: typer.Context,
    prompt: Annotated[str, typer.Argument(help="The prompt to send")] = "",
    model: Annotated[
        str | None,
        typer.Option(
            "--model", "-m", help="Model name or alias (default: $ALCF_AI_MODEL)."
        ),
    ] = None,
    api: Annotated[
        Api, typer.Option("--api", help="Which LLM API dialect to use.")
    ] = Api.auto,
    system: Annotated[
        str | None,
        typer.Option("--system", "-s", help="System prompt (an instruction to apply)."),
    ] = None,
    stream: Annotated[bool, typer.Option("--stream/--no-stream")] = True,
    temperature: Annotated[float | None, typer.Option("--temp", "-t")] = None,
    max_tokens: Annotated[int | None, typer.Option("--max-tokens", "-n")] = None,
    deployment: Annotated[
        str | None,
        typer.Option("--deployment", "-d", help="Pin the request to one deployment."),
    ] = None,
    input_file: Annotated[
        Path | None,
        typer.Option(
            "--input-file", "-i", help="Read additional user input from this file."
        ),
    ] = None,
    show_reasoning: Annotated[
        bool, typer.Option("--show-reasoning", help="Print reasoning/thinking output.")
    ] = False,
    wait: Annotated[
        bool,
        typer.Option(
            "--wait/--no-wait", help="Wait through a cold start (shows the ETA)."
        ),
    ] = True,
    timeout: Annotated[
        float, typer.Option("--timeout", help="Max seconds to wait for a cold start.")
    ] = 1800,
    as_json: Annotated[
        bool,
        typer.Option("--json", help="Print the raw response (implies --no-stream)."),
    ] = False,
) -> None:
    """
    Send a prompt to an LLM and print the response.

    The user message is built by concatenating, in order: piped stdin (if any),
    the contents of --input-file (if any), and the positional PROMPT argument.
    Works with Chat Completions, Responses, and Anthropic Messages models.
    """
    client = ctx.obj.client
    user_content = _gather_prompt(prompt, input_file)
    if not user_content and not system:
        raise typer.BadParameter(
            "Provide a prompt via the positional argument, --input-file, "
            "piped stdin, or --system."
        )

    model, endpoint = resolve_target(client, model, api)
    payload = build_payload(
        endpoint,
        model,
        user_content,
        system=system,
        temperature=temperature,
        max_tokens=max_tokens,
    )
    inference = client.deployment(deployment) if deployment else client.inference

    def run() -> None:
        if stream and not as_json:
            _run_streaming(inference, endpoint, payload, show_reasoning)
        else:
            _run_unary(inference, endpoint, payload, show_reasoning, as_json)

    with_startup_wait(run, model=model, wait=wait, timeout=timeout)


def embed(
    ctx: typer.Context,
    text: Annotated[list[str], typer.Argument(help="One or more texts to embed")],
    model: Annotated[str, typer.Option("--model", "-m", help="Embedding model name.")],
    deployment: Annotated[
        str | None,
        typer.Option("--deployment", "-d", help="Pin the request to one deployment."),
    ] = None,
    wait: Annotated[
        bool,
        typer.Option(
            "--wait/--no-wait", help="Wait through a cold start (shows the ETA)."
        ),
    ] = True,
) -> None:
    """Embed text and print the OpenAI-format embeddings response as JSON."""
    client: InferenceClient = ctx.obj.client
    inference = client.deployment(deployment) if deployment else client.inference
    body = with_startup_wait(
        lambda: inference.embeddings(model, text if len(text) > 1 else text[0]),
        model=model,
        wait=wait,
        timeout=1800,
    )
    print_json(body)
