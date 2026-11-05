"""
Fake OpenAI-compatible upstream for live tests (stdlib only).

Runs as a sidecar container on the compose network (see backend.ChaosUpstream)
and serves any number of virtual backends, one per path prefix:

    GET  /<backend>/health                -> 200, whatever the mode
    POST /<backend>/v1/chat/completions   -> depends on the backend's mode

The tests register each virtual backend as a StaticDeployment and switch its
behaviour through the control API:

    POST /_control/<backend>   {"mode": ..., <options>}   set mode (resets hits)
    GET  /_control                                          modes + hit counts

Modes:
    ok          normal completion (unary or SSE) with the configured usage
    status      reply `status` (e.g. 503) with a JSON error body
    disconnect  close the connection without sending a response
    slow        SSE: one chunk every `interval` s; stops after `chunks` chunks
                (0 = never) with usage and [DONE]
    break       SSE: 200, `chunks` chunks every `interval` s, then reset the
                connection (no terminating chunk, no usage, no [DONE])

Options: prompt_tokens / completion_tokens (usage to report; default: about
len(body)/4 and 5), status, interval, chunks.

Streams use chunked transfer encoding so that an aborted stream is visible to
the gateway as an incomplete body rather than a clean end of response.
"""

import asyncio
import json
import sys
import time
from typing import Any

MODES: dict[str, dict[str, Any]] = {}
HITS: dict[str, int] = {}


def _cfg(backend: str) -> dict[str, Any]:
    return MODES.get(backend, {"mode": "ok"})


async def _read_request(
    reader: asyncio.StreamReader,
) -> tuple[str, str, bytes] | None:
    head = await reader.readuntil(b"\r\n\r\n")
    lines = head.decode("latin-1").split("\r\n")
    method, path, _ = lines[0].split(" ", 2)
    length = 0
    for line in lines[1:]:
        if line.lower().startswith("content-length:"):
            length = int(line.split(":", 1)[1])
    body = await reader.readexactly(length) if length else b""
    return method, path, body


def _response(status: int, payload: Any) -> bytes:
    body = json.dumps(payload).encode()
    return (
        f"HTTP/1.1 {status} X\r\nContent-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n"
    ).encode() + body


def _chunk(data: bytes) -> bytes:
    return f"{len(data):x}\r\n".encode() + data + b"\r\n"


def _sse(obj: Any) -> bytes:
    return b"data: " + (obj if isinstance(obj, bytes) else json.dumps(obj).encode())


def _usage(cfg: dict[str, Any], body: bytes) -> dict[str, int]:
    prompt = int(cfg.get("prompt_tokens", max(1, len(body) // 4)))
    completion = int(cfg.get("completion_tokens", 5))
    return {
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": prompt + completion,
    }


def _delta(i: int, model: str) -> dict[str, Any]:
    return {
        "id": "chaos",
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": model,
        "choices": [{"index": 0, "delta": {"content": f"{i} "}}],
    }


async def _stream(
    writer: asyncio.StreamWriter, cfg: dict[str, Any], body: bytes, model: str
) -> None:
    mode = cfg["mode"]
    interval = float(cfg.get("interval", 0.0 if mode == "ok" else 0.5))
    chunks = int(cfg.get("chunks", 3 if mode in ("ok", "break") else 0))
    writer.write(
        b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
        b"Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
    )
    i = 0
    while chunks == 0 or i < chunks:
        writer.write(_chunk(_sse(_delta(i, model)) + b"\n\n"))
        await writer.drain()  # raises once the gateway hangs up
        i += 1
        if interval:
            await asyncio.sleep(interval)
    if mode == "break":
        writer.transport.abort()
        return
    final = {
        "id": "chaos",
        "object": "chat.completion.chunk",
        "model": model,
        "choices": [],
        "usage": _usage(cfg, body),
    }
    writer.write(_chunk(_sse(final) + b"\n\n"))
    writer.write(_chunk(_sse(b"[DONE]") + b"\n\n"))
    writer.write(b"0\r\n\r\n")
    await writer.drain()


async def _inference(writer: asyncio.StreamWriter, backend: str, body: bytes) -> None:
    HITS[backend] = HITS.get(backend, 0) + 1
    cfg = _cfg(backend)
    mode = cfg["mode"]
    req = json.loads(body or b"{}")
    model = str(req.get("model", "chaos"))
    if mode == "disconnect":
        writer.transport.abort()
        return
    if mode == "status":
        status = int(cfg.get("status", 503))
        writer.write(_response(status, {"error": {"message": f"chaos {status}"}}))
        return
    if req.get("stream") or mode in ("slow", "break"):
        await _stream(writer, cfg, body, model)
        return
    writer.write(
        _response(
            200,
            {
                "id": "chaos",
                "object": "chat.completion",
                "created": int(time.time()),
                "model": model,
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "OK"},
                        "finish_reason": "stop",
                    }
                ],
                "usage": _usage(cfg, body),
            },
        )
    )


async def _handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        req = await _read_request(reader)
        if req is None:
            return
        method, path, body = req
        parts = [p for p in path.split("?")[0].split("/") if p]
        if parts[:1] == ["_control"]:
            if method == "POST" and len(parts) == 2:
                MODES[parts[1]] = {"mode": "ok", **json.loads(body or b"{}")}
                HITS[parts[1]] = 0
            payload = {b: {**_cfg(b), "hits": HITS.get(b, 0)} for b in {*MODES, *HITS}}
            writer.write(_response(200, payload))
        elif len(parts) == 2 and parts[1] == "health":
            writer.write(_response(200, {"status": "ok"}))
        elif method == "POST" and len(parts) >= 2:
            await _inference(writer, parts[0], body)
        else:
            writer.write(_response(404, {"error": {"message": f"no route {path}"}}))
        if not writer.transport.is_closing():
            await writer.drain()
    except (ConnectionError, asyncio.IncompleteReadError):
        pass
    finally:
        if not writer.transport.is_closing():
            writer.close()


async def main(port: int) -> None:
    server = await asyncio.start_server(_handle, "0.0.0.0", port)
    print(f"chaos upstream listening on :{port}", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main(int(sys.argv[1]) if len(sys.argv) > 1 else 8000))
