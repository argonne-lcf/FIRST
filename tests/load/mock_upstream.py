"""
Mock OpenAI-compatible upstream for load tests: a model that does no work.

It answers `/v1/chat/completions` (unary or SSE) after a configurable delay,
so the gateway in front of it is the only thing being measured.  Unlike
`tests/live/chaos_upstream.py` it keeps connections alive and runs several
worker processes, so it stays far faster than the gateway under test.

    uv run python -m tests.load mock-upstream --port 9000 --workers 4 \\
        --ttft-ms 20 --token-ms 5

The file is standalone (stdlib + uvicorn), so it can also be copied to another
host and run as `uvicorn mock_upstream:app --workers 4`, configured through the
environment:

    MOCK_TTFT_MS   delay before the first token / the unary response   (0)
    MOCK_TOKEN_MS  delay per output token                               (0)
    MOCK_TOKENS    output tokens when the request has no `max_tokens`   (16)

Routes:
    GET  /health, /v1/models     -> 200
    POST <anything>              -> a chat completion of `max_tokens` tokens;
                                    unary takes ttft + tokens * token_ms, a
                                    stream sends one chunk per token.
"""

import asyncio
import json
import os
import time
from typing import Any, Awaitable, Callable, MutableMapping

Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]

TTFT_SEC = float(os.environ.get("MOCK_TTFT_MS", "0")) / 1000
TOKEN_SEC = float(os.environ.get("MOCK_TOKEN_MS", "0")) / 1000
DEFAULT_TOKENS = int(os.environ.get("MOCK_TOKENS", "16"))

_JSON = [(b"content-type", b"application/json")]
_SSE = [(b"content-type", b"text/event-stream"), (b"cache-control", b"no-cache")]
_HEALTH = json.dumps({"status": "ok"}).encode()


def _sse(obj: Any) -> bytes:
    return b"data: " + json.dumps(obj, separators=(",", ":")).encode() + b"\n\n"


async def _read_body(receive: Receive) -> bytes:
    body: bytes = b""
    while True:
        message = await receive()
        body += message.get("body", b"")
        if not message.get("more_body"):
            return body


async def _respond(send: Send, status: int, body: bytes) -> None:
    await send({"type": "http.response.start", "status": status, "headers": _JSON})
    await send({"type": "http.response.body", "body": body})


async def _completion(send: Send, body: bytes) -> None:
    try:
        req = json.loads(body or b"{}")
    except ValueError:
        await _respond(send, 400, b'{"error":{"message":"invalid JSON"}}')
        return
    model = str(req.get("model", "mock"))
    tokens = int(req.get("max_tokens") or DEFAULT_TOKENS)
    usage = {
        "prompt_tokens": max(1, len(body) // 4),
        "completion_tokens": tokens,
        "total_tokens": max(1, len(body) // 4) + tokens,
    }
    created = int(time.time())

    if not req.get("stream"):
        if delay := TTFT_SEC + tokens * TOKEN_SEC:
            await asyncio.sleep(delay)
        payload = {
            "id": "mock",
            "object": "chat.completion",
            "created": created,
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "tok " * tokens},
                    "finish_reason": "length",
                }
            ],
            "usage": usage,
        }
        await _respond(send, 200, json.dumps(payload).encode())
        return

    def chunk(delta: dict[str, Any], finish: str | None = None) -> bytes:
        return _sse(
            {
                "id": "mock",
                "object": "chat.completion.chunk",
                "created": created,
                "model": model,
                "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
            }
        )

    await send({"type": "http.response.start", "status": 200, "headers": _SSE})
    # Like vLLM: the role chunk goes out at once, the first token after TTFT.
    first = chunk({"role": "assistant", "content": ""})
    await send({"type": "http.response.body", "body": first, "more_body": True})
    token = chunk({"content": "tok "})
    if TTFT_SEC:
        await asyncio.sleep(TTFT_SEC)
    for i in range(tokens):
        if i and TOKEN_SEC:
            await asyncio.sleep(TOKEN_SEC)
        await send({"type": "http.response.body", "body": token, "more_body": True})
    tail = (
        chunk({}, "length")
        + _sse(
            {
                "id": "mock",
                "object": "chat.completion.chunk",
                "created": created,
                "model": model,
                "choices": [],
                "usage": usage,
            }
        )
        + b"data: [DONE]\n\n"
    )
    await send({"type": "http.response.body", "body": tail})


async def app(scope: Scope, receive: Receive, send: Send) -> None:
    if scope["type"] == "lifespan":
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
                return
    if scope["type"] != "http":
        return
    if scope["method"] == "POST":
        await _completion(send, await _read_body(receive))
    elif scope["path"].endswith(("/health", "/models")):
        await _respond(send, 200, _HEALTH)
    else:
        await _respond(send, 404, b'{"error":{"message":"no such route"}}')
