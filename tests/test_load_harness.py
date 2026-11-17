"""
The load-test harness (tests/load): statistics, and the generator driven over
real sockets through the gateway app to the mock upstream, so the
Server-Timing split it reports is the one the gateway actually sends.
"""

import asyncio
import contextlib
import threading
import time
from pathlib import Path
from typing import Any, AsyncIterator

import httpx
import pytest
import uvicorn

from first_gateway.apiserver.api import app
from first_gateway.apiserver.backend_client_manager import BackendClient

from .fixtures.auth import USER_TOKEN
from .load import mock_upstream
from .load.harness import Mode, StepConfig, run_step, run_users
from .load.stats import (
    comparison_table,
    parse_server_timing,
    steps_csv,
    steps_table,
    summarize,
)
from .test_structured_logs import _seed_streaming_config

# --------------------------------------------------------------------------
# Statistics
# --------------------------------------------------------------------------


def test_parse_server_timing() -> None:
    header = 'queue;dur=0.8, gateway;dur=41.3, upstream;dur=38.9;desc="attempts=2"'
    assert parse_server_timing(header) == {
        "queue": 0.8,
        "gateway": 41.3,
        "upstream": 38.9,
        "attempts": 2.0,
    }
    # Entries appended by another hop, and junk, do not break parsing.
    assert parse_server_timing('cfL4;desc="?proto=TCP", gateway;dur=5, x;dur=oops') == {
        "gateway": 5.0
    }
    assert parse_server_timing(None) == {}


def _record(t0: float, e2e: float, **fields: Any) -> dict[str, Any]:
    base = dict.fromkeys(("err", "ttfb", "ttft", "gap", "n", "out", "q", "att", "dep"))
    base.update(t0=t0, t1=t0 + e2e / 1000, e2e=e2e, st=200, ok=True, gw=None, up=None)
    return {**base, **fields}


def test_summarize_takes_differences_per_request() -> None:
    records = [
        _record(0.5, 100, gw=80, up=70),  # warm-up: ignored
        _record(1.0, 50, gw=40, up=30, q=2),
        _record(2.0, 60, gw=45, up=44),  # no queue stamp: folded into network
        _record(3.0, 10, st=429, ok=False),
        _record(10.9, 500, gw=45, up=44),  # finishes after the window: not counted
        _record(10.5, 9000, st=0, ok=False, err="ReadTimeout"),  # ... but an error is
    ]
    s = summarize(records, users=2, warmup=1.0, duration=10.0, stream=False)
    assert s["requests_ok"] == 2
    assert s["rps"] == pytest.approx(0.2)
    assert s["errors"] == {"http 429": 1, "ReadTimeout": 1}
    assert s["error_rate"] == pytest.approx(2 / 5)
    assert s["overhead"]["p50"] == 1 and s["overhead"]["max"] == 10
    assert sorted((s["network"]["p50"], s["network"]["max"])) == [8, 15]
    assert s["queue"]["max"] == 2


def test_tables_and_peak_scaling() -> None:
    def run(name: str, replicas: int, rps: list[float]) -> dict[str, Any]:
        steps = []
        for users, value in zip((1, 2, 4), rps):
            records = [
                _record(i / value, 10, gw=8, up=5) for i in range(int(value * 10))
            ]
            step = summarize(records, users=users, warmup=0, duration=10, stream=False)
            step["replicas"] = {"min": replicas, "max": replicas}
            step["client_cpu"] = 0.1
            steps.append(step)
        return {"meta": {"name": name, "mode": "unary", "url": "u"}, "steps": steps}

    runs = [run("one", 1, [10, 20, 20]), run("two", 2, [10, 20, 38])]
    table = comparison_table(runs)
    assert "| one | unary | 1 | 20.0 | 2 | 1.00× | 20.0 | 100% |" in table
    assert "| two | unary | 2 | 38.0 | 4 | 1.90× | 19.0 | 95% |" in table
    assert "| 4 | 2 | 38.0 | 0.0% |" in steps_table(runs[1]["steps"], stream=False)
    assert steps_csv(runs[0]["steps"]).count("\n") == 4


# --------------------------------------------------------------------------
# The generator, over real sockets
# --------------------------------------------------------------------------


@contextlib.asynccontextmanager
async def _serve(asgi_app: Any) -> AsyncIterator[str]:
    """Serve an ASGI app on a free loopback port in this event loop."""
    config = uvicorn.Config(
        asgi_app, host="127.0.0.1", port=0, lifespan="off", log_level="warning"
    )
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.01)
    port = server.servers[0].sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        await task


@pytest.mark.parametrize("mode", ["unary", "stream", "health"])
async def test_harness_through_gateway_to_mock_upstream(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch, mode: Mode
) -> None:
    monkeypatch.setattr(mock_upstream, "TTFT_SEC", 0.03)
    unlimited = {"tpm": 10**9, "burst_tokens": 10**9, "rpm": 10**7}
    unlimited |= {"burst_requests": 10**6, "max_user_concurrency": 1000}
    _seed_streaming_config(usage_limits={"default": unlimited})
    async with _serve(mock_upstream.app) as upstream, _serve(app) as gateway:
        app.state.backend_client_manager._clients["static_deployment/dep-open"] = (
            BackendClient(f"{upstream}/v1")
        )
        path = "/health" if mode == "health" else "/federated/v1/chat/completions"
        cfg = StepConfig(
            url=gateway + path,
            mode=mode,
            model="open/model",
            users=3,
            start_at=time.time(),
            warmup=0.0,
            duration=1.0,
            ramp=0.0,
            output_tokens=4,
            token=USER_TOKEN,
        )
        records = await run_users(cfg)

    assert len(records) >= 3
    assert {r["err"] or r["st"] for r in records if not r["ok"]} == set()
    for r in records:
        assert 0 <= r["gw"] <= r["ttfb"] <= r["e2e"]
        if mode == "health":
            assert r["up"] is None
            continue
        assert r["att"] == 1 and r["dep"] == "dep-open" and r["out"] == 4
        assert 0 <= r["up"] <= r["gw"]
        if mode == "unary":
            assert r["up"] >= 30  # the whole upstream round trip
        else:
            # Headers precede the body: the first token arrives after them.
            assert r["ttft"] >= 30 and r["n"] >= 5 and r["gap"] < 1000

    summary = summarize(
        records, users=3, warmup=0.0, duration=1.0, stream=mode == "stream"
    )
    assert summary["error_rate"] == 0 and summary["rps"] >= 3
    if mode != "health":
        assert summary["overhead"]["p50"] >= 0


def test_run_step_across_processes(tmp_path: Path) -> None:
    """The multi-process path: workers start together and their records merge."""
    config = uvicorn.Config(
        mock_upstream.app, host="127.0.0.1", port=0, log_level="warning"
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        while not server.started:
            time.sleep(0.01)
        port = server.servers[0].sockets[0].getsockname()[1]
        cfg = StepConfig(
            url=f"http://127.0.0.1:{port}/v1/chat/completions",
            mode="stream",
            model="m",
            users=5,
            start_at=0.0,
            warmup=0.5,
            duration=1.0,
            ramp=0.2,
        )
        summary = run_step(cfg, processes=2, out_dir=tmp_path)
    finally:
        server.should_exit = True
        thread.join()

    assert summary["processes"] == 2
    assert summary["failed"] == 0 and summary["requests_ok"] > 10
    assert summary["gateway"] is None  # the mock sends no Server-Timing
    assert (tmp_path / "requests.jsonl").read_text().count("\n") >= 10
    assert not list(tmp_path.glob("worker-*.jsonl"))


@pytest.mark.parametrize(
    "delta, counts",
    [
        (b'{"role":"assistant","content":""}', False),
        (b'{"content":"Hi"}', True),
        (b'{"reasoning_content":"The"}', True),
        (b'{"reasoning":"The"}', True),  # vLLM 0.29 + gpt-oss
    ],
)
def test_stream_first_token_matches_reasoning_deltas(
    delta: bytes, counts: bool
) -> None:
    from .load.harness import _CONTENT

    assert bool(_CONTENT.search(b'{"choices":[{"delta":' + delta + b"}]}")) is counts


def test_ports_spread_users_round_robin() -> None:
    def cfg(url: str, ports: tuple[int, ...] = ()) -> StepConfig:
        return StepConfig(
            url=url,
            mode="health",
            model=None,
            users=4,
            start_at=0.0,
            warmup=0.0,
            duration=1.0,
            ramp=0.0,
            ports=ports,
        )

    spread = cfg(
        "https://localhost:8002/resource_server/health?x=1", (8002, 8021, 8022)
    )
    assert [spread.url_for(i) for i in range(4)] == [
        "https://localhost:8002/resource_server/health?x=1",
        "https://localhost:8021/resource_server/health?x=1",
        "https://localhost:8022/resource_server/health?x=1",
        "https://localhost:8002/resource_server/health?x=1",
    ]
    assert (
        cfg("http://localhost:8001/health").url_for(7) == "http://localhost:8001/health"
    )
