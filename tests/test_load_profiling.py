"""
Load-test profiling (tests/load/PROFILING.md): the gateway's phase split in
Server-Timing, the per-worker probe, and the report that joins them with
py-spy stacks.
"""

import asyncio
import json
from pathlib import Path

import httpx
import pytest
from aiohttp import web

from first_gateway.apiserver import profiling
from first_gateway.apiserver.api import app

from .fixtures import backend
from .fixtures.auth import USER_TOKEN, auth_header
from .load.profile import (
    component,
    load_stacks,
    load_worker_samples,
    profile_report,
    stack_tables,
    worker_summary,
)
from .load.stats import PHASES, parse_server_timing, phase_table, summarize
from .test_structured_logs import _CHAT, _seed_streaming_config

pytestmark = pytest.mark.usefixtures("stop_backends")

_UNARY_BODY = {
    "id": "x",
    "object": "chat.completion",
    "choices": [],
    "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
}


async def test_unary_phases_partition_the_gateway_time(
    client: httpx.AsyncClient,
) -> None:
    _seed_streaming_config()

    async def slow_backend(request: web.Request) -> web.StreamResponse:
        await asyncio.sleep(0.05)
        return web.json_response(_UNARY_BODY)

    app.state.backend_client_manager._clients[
        "static_deployment/dep-open"
    ] = await backend.serve_backend(slow_backend)
    resp = await client.post(
        "/federated/v1/chat/completions", headers=auth_header(USER_TOKEN), json=_CHAT
    )
    assert resp.status_code == 200
    timing = parse_server_timing(resp.headers["server-timing"])
    phases = {k[2:]: v for k, v in timing.items() if k.startswith("p_")}

    # Every phase of the unary hot path is reported, in hot-path order
    # (`record_stats` is only reported by builds before it joined `settle`).
    assert list(phases) == [p for p in PHASES if p != "record_stats"]
    assert phases["upstream"] >= 50
    # Laps are consecutive, so they add up to the whole request.
    assert sum(phases.values()) == pytest.approx(timing["gateway"], abs=0.5)


async def test_health_reports_only_the_respond_phase(
    client: httpx.AsyncClient,
) -> None:
    resp = await client.get("/health")
    timing = parse_server_timing(resp.headers["server-timing"])
    assert [k for k in timing if k.startswith("p_")] == ["p_respond"]


def test_phase_table_from_records() -> None:
    def rec(i: int) -> dict[str, object]:
        return {
            "t0": i * 0.1,
            "t1": i * 0.1 + 0.05,
            "st": 200,
            "ok": True,
            "err": None,
            "e2e": 50.0,
            "ttfb": 50.0,
            "out": 4,
            "gw": 40.0,
            "up": 30.0,
            "q": 0.5,
            "att": 1,
            "dep": "d",
            "ph": {"validate": 0.2, "upstream": 30.0, "render": 0.1, "auth": 9.7},
        }

    step = summarize(
        [rec(i) for i in range(20)], users=4, warmup=0, duration=10, stream=False
    )
    assert list(step["phases"]) == ["auth", "validate", "upstream", "render"]
    assert step["phases_cpu"]["mean"] == pytest.approx(0.3)
    assert step["phases_wait"]["mean"] == pytest.approx(39.7)
    table = phase_table(step)
    assert table is not None
    assert "| upstream | wait | 30 | 30 | 30 | 30 | 75.0% |" in table


async def test_probe_writes_worker_samples(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(profiling, "SNAPSHOT_INTERVAL_SEC", 0.05)
    monkeypatch.setenv(profiling.PROFILE_DIR_ENV, str(tmp_path))
    task = profiling.start_probe()
    assert task is not None
    await asyncio.sleep(0.3)
    task.cancel()

    workers = load_worker_samples(tmp_path)
    (samples,) = workers.values()
    assert len(samples) >= 2
    assert set(samples[0]) == {
        "t",
        "dt",
        "started",
        "finished",
        "inflight",
        "cpu_proc",
        "cpu_loop",
        "lag_ms",
    }
    assert samples[0]["lag_ms"] and min(samples[0]["lag_ms"]) >= 0


def test_probe_is_off_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(profiling.PROFILE_DIR_ENV, raising=False)
    assert profiling.start_probe() is None


def _sample(t: float, finished: int, cpu: float, lag: list[float]) -> dict[str, object]:
    return {
        "t": t,
        "dt": 1.0,
        "started": finished,
        "finished": finished,
        "inflight": 3,
        "cpu_proc": cpu,
        "cpu_loop": cpu - 0.1,
        "lag_ms": lag,
    }


def test_worker_summary_windows_and_totals() -> None:
    workers = {
        11: [_sample(100.0 + i, 50, 0.9, [1.0, 2.0]) for i in range(10)],
        12: [_sample(100.0 + i, 150, 0.9, [3.0, 40.0]) for i in range(10)],
    }
    summary = worker_summary(workers, (102.0, 106.0))  # samples ending 103..106
    assert summary["workers"][11]["finished"] == 200
    assert summary["workers"][11]["cpu_loop"] == pytest.approx(0.8)
    total = summary["total"]
    assert total["rps"] == pytest.approx(200.0)
    assert total["cpu_proc"] == pytest.approx(1.8)  # cores, over both workers
    assert total["cpu_ms_per_request"] == pytest.approx(9.0)
    assert total["lag"]["max"] == 40.0


_SITE = "/app/.venv/lib/python3.12/site-packages"
_STACKS = f"""\
process 7:"gunicorn";_run (/usr/local/lib/python3.12/asyncio/events.py:84);submit_inference (/app/packages/gateway/first_gateway/apiserver/inference.py:119);_handle_unary (/app/packages/gateway/first_gateway/apiserver/inference.py:371);dumps (/usr/local/lib/python3.12/json/__init__.py:183) 30
process 7:"gunicorn";_run (/usr/local/lib/python3.12/asyncio/events.py:84);submit_inference (/app/packages/gateway/first_gateway/apiserver/inference.py:119);settle (/app/packages/gateway/first_gateway/database/redis/admission.py:221);execute_command ({_SITE}/redis/asyncio/client.py:700) 50
process 8:"gunicorn";data_received ({_SITE}/uvicorn/protocols/http/httptools_impl.py:240) 20
"""


@pytest.mark.parametrize(
    "file, expected",
    [
        (f"{_SITE}/pydantic/main.py", "pydantic"),
        (
            f"{_SITE}/pydantic_core/_pydantic_core.cpython-312-x86_64-linux-gnu.so",
            "pydantic_core",
        ),
        ("/app/packages/common/first_common/schema/structured_logs.py", "first_common"),
        ("/usr/local/lib/python3.12/json/encoder.py", "stdlib:json"),
        ("/usr/local/lib/python3.12/uuid.py", "stdlib:uuid"),
        ("redis/asyncio/client.py", "redis"),  # py-spy's shortened names
        ("<frozen importlib._bootstrap>", "stdlib:importlib"),
        ("", "(native)"),
    ],
)
def test_component(file: str, expected: str) -> None:
    assert component(file) == expected


def test_stack_tables_charge_samples_to_gateway_functions(tmp_path: Path) -> None:
    path = tmp_path / "pyspy-test.txt"
    path.write_text(_STACKS)
    tables = stack_tables(load_stacks(path))
    assert "100 samples." in tables
    assert "| 50.0% | 50% | `redis` |" in tables
    assert "`settle (first_gateway/database/redis/admission.py) → redis`" in tables
    assert (
        "| 30.0% | 80% | `_handle_unary (first_gateway/apiserver/inference.py) → stdlib:json` |"
        in tables
    )
    assert "`(outside gateway code) uvicorn`" in tables
    assert "`dumps (json/__init__.py)`" in tables


def test_profile_report_joins_run_and_gateway_profile(tmp_path: Path) -> None:
    run_dir, profile_dir = tmp_path / "run", tmp_path / "profile"
    run_dir.mkdir()
    profile_dir.mkdir()
    dist = {"mean": 2.0, "max": 2.0, "p50": 2.0, "p95": 2.0, "p99": 2.0}
    step = {
        "users": 32,
        "rps": 200.0,
        "window": [102.0, 106.0],
        "phases": {"validate": dist, "upstream": dist},
        "phases_cpu": dist,
        "phases_wait": dist,
    }
    (run_dir / "meta.json").write_text(
        json.dumps({"name": "v2-prof", "url": "http://x", "started_at": "now"})
    )
    (run_dir / "summary.json").write_text(json.dumps([step]))
    with open(profile_dir / "worker-11.jsonl", "w") as f:
        for i in range(10):
            f.write(json.dumps(_sample(100.0 + i, 100, 0.9, [1.5])) + "\n")
        f.write('{"t": 111.0, "dt"')  # a line cut off mid-write
    (profile_dir / "pyspy-a.txt").write_text(_STACKS)

    report = profile_report(run_dir, profile_dir)
    assert "## 32 users (200.0 req/s)" in report
    assert "| pid 11 | 100.0 |" in report
    assert "measured on the workers: **9 ms**" in report
    assert "the `cpu` phases account for 2 ms (22%)" in report
    assert "## CPU samples: `pyspy-a.txt`" in report

    # A gateway clock 1000 s ahead of the client's: no overlap without the offset.
    assert "No worker samples" in profile_report(run_dir, profile_dir, clock_offset=1e3)
