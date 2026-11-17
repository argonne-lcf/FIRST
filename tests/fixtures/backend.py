"""A local HTTP server standing in for an inference backend.

``serve_backend(handler)`` starts an aiohttp server for ``handler`` on a free
loopback port and returns a ``BackendClient`` pointed at it; the
``stop_backends`` fixture shuts down whatever a test started.
"""

from typing import AsyncIterator, Awaitable, Callable

import aiohttp
import pytest
from aiohttp import web

from first_gateway.apiserver.backend_client_manager import (
    BACKEND_TIMEOUT,
    BackendClient,
)

Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]

_running: list[tuple[web.AppRunner, BackendClient]] = []


async def serve_backend(
    handler: Handler, *, timeout: aiohttp.ClientTimeout = BACKEND_TIMEOUT
) -> BackendClient:
    app = web.Application()
    app.router.add_route("*", "/{path:.*}", handler)
    runner = web.AppRunner(app, shutdown_timeout=0.1)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 0).start()
    port = runner.addresses[0][1]
    client = BackendClient(f"http://127.0.0.1:{port}", timeout=timeout)
    _running.append((runner, client))
    return client


def respond(
    status: int = 200, *, json: object = None, text: str | None = None
) -> Handler:
    """A handler that always returns the same response."""

    async def handler(request: web.Request) -> web.StreamResponse:
        if text is not None:
            return web.Response(status=status, text=text)
        return web.json_response(json, status=status)

    return handler


def stream(chunks: list[bytes], *, break_after: int | None = None) -> Handler:
    """A handler that streams ``chunks``; with ``break_after`` it drops the
    connection after that many chunks instead of ending the response."""

    async def handler(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        for i, chunk in enumerate(chunks):
            if i == break_after:
                assert request.transport is not None
                request.transport.abort()
                return response
            await response.write(chunk)
        await response.write_eof()
        return response

    return handler


@pytest.fixture
async def stop_backends() -> AsyncIterator[None]:
    yield
    while _running:
        runner, client = _running.pop()
        await client.aclose()
        await runner.cleanup()
