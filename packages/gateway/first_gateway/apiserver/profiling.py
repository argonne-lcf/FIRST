"""Per-worker saturation gauges for load-test profiling (tests/load/PROFILING.md).

Off unless ``FIRST_PROFILE_DIR`` is set.  Each gateway worker then appends one
JSON line per second to ``<dir>/worker-<pid>.jsonl``:

    t          wall clock at the end of the interval (epoch seconds)
    dt         length of the interval, seconds
    started    requests that entered the middleware in the interval
    finished   requests that left it
    inflight   requests inside the worker at the end of the interval
    cpu_proc   CPU seconds used by the whole worker process (all threads)
    cpu_loop   CPU seconds used by the event-loop thread alone
    lag_ms     event-loop lag samples: how late each ``sleep(10 ms)`` woke up

``cpu_loop / dt`` is how busy the event loop is (1.0 = saturated);
``cpu_proc - cpu_loop`` is CPU spent on other threads (body-file writes);
``cpu_proc / finished`` is the CPU cost of one request.  ``lag_ms`` is what
every ``await`` in the hot path pays on top of the I/O it waits for.
"""

import asyncio
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path

PROFILE_DIR_ENV = "FIRST_PROFILE_DIR"
LAG_INTERVAL_SEC = 0.010
SNAPSHOT_INTERVAL_SEC = 1.0


@dataclass
class WorkerStats:
    """Request counters, bumped by ``ResponseLogMiddleware``."""

    started: int = 0
    finished: int = 0


STATS = WorkerStats()


async def _probe(path: Path) -> None:
    lags: list[float] = []
    last_wall = time.time()
    last = (STATS.started, STATS.finished, time.process_time(), time.thread_time())
    # Line-buffered: a line is complete on disk as soon as it is written.
    with open(path, "a", buffering=1) as out:
        while True:
            # perf_counter, not loop.time(): uvloop's clock ticks in whole ms.
            # Its timers do too, so an idle loop reads up to ~1 ms of "lag".
            due = time.perf_counter() + LAG_INTERVAL_SEC
            await asyncio.sleep(LAG_INTERVAL_SEC)
            lags.append(round(max(0.0, time.perf_counter() - due) * 1000, 3))
            now_wall = time.time()
            if now_wall - last_wall < SNAPSHOT_INTERVAL_SEC:
                continue
            now = (
                STATS.started,
                STATS.finished,
                time.process_time(),
                time.thread_time(),
            )
            snapshot = {
                "t": round(now_wall, 3),
                "dt": round(now_wall - last_wall, 4),
                "started": now[0] - last[0],
                "finished": now[1] - last[1],
                "inflight": now[0] - now[1],
                "cpu_proc": round(now[2] - last[2], 5),
                "cpu_loop": round(now[3] - last[3], 5),
                "lag_ms": lags,
            }
            out.write(json.dumps(snapshot, separators=(",", ":")) + "\n")
            lags, last_wall, last = [], now_wall, now


def start_probe() -> asyncio.Task[None] | None:
    """Start this worker's probe if ``FIRST_PROFILE_DIR`` is set."""
    directory = os.environ.get(PROFILE_DIR_ENV)
    if not directory:
        return None
    Path(directory).mkdir(parents=True, exist_ok=True)
    path = Path(directory) / f"worker-{os.getpid()}.jsonl"
    return asyncio.create_task(_probe(path), name="profiling-probe")
