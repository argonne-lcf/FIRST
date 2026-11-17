"""CPU per request of a backend HTTP client as concurrency grows, without the gateway.

Isolates the backend HTTP client (H9-H11 in PROFILING.md). Start the mock with
a delay so requests overlap, then drive it with N concurrent requests through a
client configured like the gateway's backend pool:

    MOCK_TTFT_MS=1000 uv run uvicorn --app-dir tests/load mock_upstream:app \\
        --port 9911 --workers 4 --backlog 4096 --timeout-keep-alive 120 --no-access-log
    uv run python tests/load/pool_bench.py --users 64,256,1024
    uv run python tests/load/pool_bench.py --users 256,1024 --shards 16

`--client aiohttp` drives the gateway's own `BackendClient` (aiohttp, which
replaced httpx on the hot path); `--client httpx` (the default) an httpx client
configured like the gateway's former pool. `--stream` reads an SSE stream of
`--tokens` chunks instead (start the mock with MOCK_TTFT_MS=200 MOCK_TOKEN_MS=5
to match B1s): it measures the per-chunk read cost of the relay.

Reported per step: completed req/s, socket reads/s (streams), this process's
CPU, CPU ms per request. With a 1 s unary mock and no client limit, req/s should
equal `users`.
"""

import argparse
import asyncio
import itertools
import json
import resource
import time
from typing import Awaitable, Callable

import httpx

from first_gateway.apiserver.backend_client_manager import BackendClient

HEADERS = {"content-type": "application/json"}

# One request: returns the number of body reads it took.
Send = Callable[[], Awaitable[int]]
Close = Callable[[], Awaitable[None]]


def _cpu_sec() -> float:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_utime + usage.ru_stime


async def _step(sends: list[Send], users: int, warmup: float, secs: float) -> str:
    pick = itertools.cycle(sends)
    done = reads = 0
    end = 0.0

    async def user() -> None:
        nonlocal done, reads
        while time.time() < end:
            # Not `reads += await ...`: that reads `reads` before the await
            # and loses the other users' updates made meanwhile.
            n = await next(pick)()
            reads += n
            done += 1

    end = time.time() + warmup  # opens the connections
    await asyncio.gather(*(user() for _ in range(users)))
    done, reads, cpu0, t0 = 0, 0, _cpu_sec(), time.time()
    end = t0 + secs
    await asyncio.gather(*(user() for _ in range(users)))
    wall, cpu = time.time() - t0, _cpu_sec() - cpu0
    return (
        f"users={users:5d} req/s={done / wall:7.1f} reads/s={reads / wall:8.0f} "
        f"cpu={100 * cpu / wall:5.1f}% cpu_ms/req={1000 * cpu / max(done, 1):6.2f}"
    )


def _httpx(url: str, body: bytes, stream: bool, limit: int) -> tuple[Send, Close]:
    limits = httpx.Limits(
        max_connections=limit,
        max_keepalive_connections=limit,
        keepalive_expiry=30.0,  # as backend_client_manager.KEEPALIVE_EXPIRY
    )
    client = httpx.AsyncClient(
        transport=httpx.AsyncHTTPTransport(limits=limits, retries=2), timeout=300
    )

    async def send() -> int:
        if not stream:
            response = await client.post(url, content=body, headers=HEADERS)
            response.raise_for_status()
            return 1
        request = client.build_request("POST", url, content=body, headers=HEADERS)
        response = await client.send(request, stream=True)
        reads = 0
        try:
            response.raise_for_status()
            async for _ in response.aiter_raw():  # as the gateway's relay did
                reads += 1
        finally:
            await response.aclose()
        return reads

    return send, client.aclose


def _aiohttp(url: str, body: bytes, stream: bool, limit: int) -> tuple[Send, Close]:
    base, _, path = url.partition("/v1/")
    client = BackendClient(base, max_connections=limit)

    async def send() -> int:
        response = await client.post("/v1/" + path, body)
        async with response:
            response.raise_for_status()
            if not stream:
                await response.read()
                return 1
            reads = 0
            async for _ in response.content.iter_any():  # as the gateway's relay
                reads += 1
            return reads

    return send, client.aclose


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--url", default="http://127.0.0.1:9911/v1/chat/completions")
    parser.add_argument("--client", choices=["httpx", "aiohttp"], default="httpx")
    parser.add_argument("--stream", action="store_true", help="read an SSE stream")
    parser.add_argument("--tokens", type=int, default=160, help="max_tokens / chunks")
    parser.add_argument("--users", default="64,256,1024")
    parser.add_argument("--shards", type=int, default=1, help="clients, round-robin")
    parser.add_argument("--max-connections", type=int, default=4096, help="in total")
    parser.add_argument("--warmup", type=float, default=3.0)
    parser.add_argument("--duration", type=float, default=10.0)
    args = parser.parse_args()

    body = json.dumps(
        {
            "model": "m",
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": args.tokens,
            "stream": args.stream,
        }
    ).encode()
    per_client = max(1, args.max_connections // args.shards)
    make = _aiohttp if args.client == "aiohttp" else _httpx
    clients = [
        make(args.url, body, args.stream, per_client) for _ in range(args.shards)
    ]
    label = f"client={args.client} stream={args.stream} shards={args.shards}"
    try:
        for users in (int(u) for u in args.users.split(",")):
            line = await _step(
                [send for send, _ in clients], users, args.warmup, args.duration
            )
            print(f"{label} {line}", flush=True)
    finally:
        for _, close in clients:
            await close()


if __name__ == "__main__":
    asyncio.run(main())
