"""Join a load run with the gateway's own profile of it (see PROFILING.md).

Inputs, all optional but the run:

- the run directory written by `python -m tests.load run` (phases per request
  come back in `Server-Timing`, so they are already in its `summary.json`);
- the gateway's profile directory, as copied off the gateway host by
  `tests/load/profile.sh collect`:
    `worker-<pid>.jsonl`   per-second gauges from each gateway worker
                           (`first_gateway/apiserver/profiling.py`)
    `pyspy-*.txt`          py-spy collapsed stacks (`profile.sh pyspy`)

Stdlib only.
"""

import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

from .stats import PHASES, Summary, _table, _us, distribution, phase_table

# --- per-worker gauges -----------------------------------------------------------


def load_worker_samples(profile_dir: Path) -> dict[int, list[dict[str, Any]]]:
    """`worker-<pid>.jsonl` -> samples by pid (a half-written last line is skipped)."""
    workers: dict[int, list[dict[str, Any]]] = {}
    for path in sorted(profile_dir.glob("worker-*.jsonl")):
        samples = []
        for line in path.read_text().splitlines():
            try:
                samples.append(json.loads(line))
            except json.JSONDecodeError:
                pass
        workers[int(path.stem.removeprefix("worker-"))] = samples
    return workers


def _aggregate(samples: Iterable[dict[str, Any]]) -> dict[str, Any] | None:
    samples = list(samples)
    dt = sum(s["dt"] for s in samples)
    if not dt:
        return None
    finished = sum(s["finished"] for s in samples)
    cpu_proc = sum(s["cpu_proc"] for s in samples)
    cpu_loop = sum(s["cpu_loop"] for s in samples)
    return {
        "seconds": dt,
        "finished": finished,
        "rps": finished / dt,
        "inflight": sum(s["inflight"] for s in samples) / len(samples),
        "cpu_proc": cpu_proc / dt,  # cores
        "cpu_loop": cpu_loop / dt,  # event-loop utilization, 1.0 = saturated
        "cpu_other": (cpu_proc - cpu_loop) / dt,  # other threads
        "cpu_ms_per_request": cpu_proc / finished * 1000 if finished else None,
        "lag": distribution(v for s in samples for v in s["lag_ms"]),
    }


def worker_summary(
    workers: dict[int, list[dict[str, Any]]], window: tuple[float, float]
) -> dict[str, Any]:
    """Gauges of each worker, and of all of them together, inside `window`
    (epoch seconds on the gateway's clock)."""
    lo, hi = window
    inside = {
        pid: [s for s in samples if lo <= s["t"] - s["dt"] and s["t"] <= hi]
        for pid, samples in workers.items()
    }
    per_worker = {pid: agg for pid, s in inside.items() if (agg := _aggregate(s))}
    everything = [s for samples in inside.values() for s in samples]
    total = _aggregate(everything)
    if total:
        # `seconds` summed over workers: put the totals back on one clock.
        n = len(per_worker) or 1
        for key in ("rps", "cpu_proc", "cpu_loop", "cpu_other"):
            total[key] *= n
        total["inflight"] *= n
    return {"workers": per_worker, "total": total}


def worker_table(summary: dict[str, Any]) -> str:
    header = ["worker", "req/s", "in flight", "loop busy", "process CPU"]
    header += [
        "other threads",
        "CPU ms/req",
        "lag p50",
        "lag p95",
        "lag p99",
        "lag max",
    ]

    def row(label: str, agg: dict[str, Any], *, total: bool = False) -> list[str]:
        cpu_req = agg["cpu_ms_per_request"]
        return [
            label,
            f"{agg['rps']:.1f}",
            f"{agg['inflight']:.1f}",
            # Summed over workers this is cores, not a fraction of one loop.
            f"{agg['cpu_loop']:.2f} cores" if total else f"{agg['cpu_loop']:.0%}",
            f"{agg['cpu_proc']:.2f} cores",
            f"{agg['cpu_other']:.2f} cores",
            "–" if cpu_req is None else f"{cpu_req:.3g}",
            *(_us(agg["lag"], k) for k in ("p50", "p95", "p99", "max")),
        ]

    rows = [row(f"pid {pid}", agg) for pid, agg in summary["workers"].items()]
    if summary["total"]:
        rows.append(row("all", summary["total"], total=True))
    return _table(header, rows)


# --- py-spy collapsed stacks -----------------------------------------------------

# `function (path/to/file.py:123)`; native frames may have no line or no file.
_FRAME = re.compile(r"^(?P<func>.*?)(?: \((?P<file>[^()]*?)(?::(?P<line>\d+))?\))?$")
_STDLIB = re.compile(r"(?:^|/)lib/python3\.\d+/(?!site-packages/)")
_OURS = ("first_gateway", "first_common")

Frame = tuple[str, str]  # (function, file)


def _parse_frame(text: str) -> Frame:
    match = _FRAME.match(text)
    assert match is not None  # every group is optional
    return match["func"], match["file"] or ""


def component(file: str) -> str:
    """The library a source file belongs to: `first_gateway`, `pydantic`,
    `stdlib:json`, ...  Works on py-spy's full and shortened file names."""
    for ours in _OURS:
        if f"{ours}/" in file:
            return ours
    if "site-packages/" in file:
        rest = file.split("site-packages/", 1)[1]
    elif _STDLIB.search(file):
        return "stdlib:" + _first_part(_STDLIB.split(file, maxsplit=1)[1])
    else:
        rest = file
    if not rest:
        return "(native)"
    if rest.startswith("<"):  # <frozen importlib._bootstrap>, <string>
        return "stdlib:" + rest.strip("<>").split()[-1].split(".")[0]
    return _first_part(rest.lstrip("/"))


def _first_part(path: str) -> str:
    head = path.split("/", 1)[0]
    return head.split(".", 1)[0] or "(native)"


def load_stacks(path: Path) -> list[tuple[list[Frame], int]]:
    """py-spy `--format raw` output: `frame;frame;... count` per line, root
    first.  The `process <pid>:"..."` frames of `--subprocesses` are dropped."""
    stacks = []
    for line in path.read_text().splitlines():
        text, _, count = line.rpartition(" ")
        if not text or not count.isdigit():
            continue
        frames = [
            _parse_frame(f)
            for f in text.split(";")
            if not f.startswith(("process ", "thread "))
        ]
        if frames:
            stacks.append((frames, int(count)))
    return stacks


def _short(file: str) -> str:
    for marker in ("site-packages/", *(f"{o}/" for o in _OURS)):
        if marker in file:
            keep = "" if marker == "site-packages/" else marker
            return keep + file.split(marker, 1)[1]
    return _STDLIB.split(file, maxsplit=1)[-1]


def stack_tables(stacks: list[tuple[list[Frame], int]], *, top: int = 25) -> str:
    """Three views of where the sampled CPU went.

    - by library: the leaf frame's library (self time);
    - by gateway function: each sample is charged to the innermost
      `first_gateway`/`first_common` frame on its stack, i.e. to the line of
      our code that the work was done on behalf of, however deep in a library
      it happened (`→ library` names the library it happened in).  Samples with none of our frames (the ASGI server, the event
      loop's own callbacks) are charged to `(outside gateway code)` by library;
    - by leaf function: the hottest individual functions.  A leaf of
      `run (asyncio/runners.py)` is uvloop itself (socket I/O, HTTP parsing):
      capture with NATIVE=1 to see inside it.
    """
    total = sum(count for _, count in stacks) or 1
    by_lib: Counter[str] = Counter()
    by_owner: Counter[str] = Counter()
    by_leaf: Counter[str] = Counter()
    for frames, count in stacks:
        func, file = frames[-1]
        lib = component(file)
        by_lib[lib] += count
        by_leaf[f"{func} ({_short(file)})"] += count
        owner = next((f for f in reversed(frames) if component(f[1]) in _OURS), None)
        if owner:
            where = f"{owner[0]} ({_short(owner[1])})"
            by_owner[where if lib in _OURS else f"{where} → {lib}"] += count
        else:
            by_owner[f"(outside gateway code) {lib}"] += count

    def table(title: str, counter: Counter[str]) -> str:
        rows, running = [], 0
        for name, count in counter.most_common(top):
            running += count
            rows.append([f"{count / total:.1%}", f"{running / total:.0%}", f"`{name}`"])
        return f"**{title}**\n\n" + _table(["CPU", "cumulative", "where"], rows)

    return "\n\n".join(
        [
            f"{total} samples.",
            table("By library (self time)", by_lib),
            table("By gateway function (innermost frame of ours)", by_owner),
            table("By leaf function", by_leaf),
        ]
    )


# --- the report ------------------------------------------------------------------


def _reconcile(step: Summary, total: dict[str, Any] | None) -> str | None:
    """How much of the measured CPU per request the `cpu` phases explain."""
    cpu_phases = step.get("phases_cpu")
    if not (total and cpu_phases and total["cpu_ms_per_request"]):
        return None
    measured = total["cpu_ms_per_request"]
    explained = cpu_phases["mean"]
    # The phases this build reported (`record_stats` exists only before the
    # hot-path optimizations folded it into `settle`).
    seen = step.get("phases") or PHASES
    waits = sum(1 for name in seen if PHASES.get(name, ("",))[0] == "wait")
    lines = [
        f"- CPU per request, measured on the workers: **{measured:.3g} ms** "
        f"(so one worker tops out near {1000 / measured:.0f} req/s).",
        f"- Of that, the `cpu` phases account for {explained:.3g} ms "
        f"({explained / measured:.0%}).  The rest is CPU inside the `wait` "
        "phases (client libraries), the ASGI server, and work after the "
        "response headers: read it off the py-spy tables.",
    ]
    if total["lag"]:
        lag = total["lag"]["p50"]
        lines.append(
            f"- Median loop lag {lag:.3g} ms × {waits} `wait` phases ≈ "
            f"{lag * waits:.3g} ms of each request is spent waiting for the "
            "loop, at the least (a phase may take several loop turns)."
        )
    return "\n".join(lines)


def profile_report(
    run_dir: Path, profile_dir: Path | None, *, clock_offset: float = 0.0
) -> str:
    meta = json.loads((run_dir / "meta.json").read_text())
    steps: list[Summary] = json.loads((run_dir / "summary.json").read_text())
    workers = load_worker_samples(profile_dir) if profile_dir else {}
    parts = [f"# Gateway profile: {meta['name']}\n", f"- target: `{meta['url']}`"]
    parts.append(f"- started {meta['started_at']}; times in ms\n")

    for step in steps:
        parts.append(f"## {step['users']} users ({step['rps']:.1f} req/s)\n")
        table = phase_table(step)
        parts.append(
            "### Gateway time by phase\n\n"
            + (
                table
                or "No `p_*` entries in Server-Timing: is the gateway "
                "running the profiling branch?"
            )
            + "\n"
        )
        window = step.get("window")
        total = None
        if workers and window:
            lo, hi = (t + clock_offset for t in window)
            summary = worker_summary(workers, (lo, hi))
            total = summary["total"]
            parts.append(
                "### Gateway workers\n\n"
                + (
                    worker_table(summary)
                    if total
                    else "No worker samples inside this step's window (clock "
                    "skew between client and gateway? see --clock-offset)."
                )
                + "\n"
            )
        if reconciled := _reconcile(step, total):
            parts.append("### Reading the two together\n\n" + reconciled + "\n")

    if profile_dir:
        for path in sorted(profile_dir.glob("pyspy-*.txt")):
            stacks = load_stacks(path)
            if stacks:
                parts.append(f"## CPU samples: `{path.name}`\n")
                parts.append(stack_tables(stacks) + "\n")
    return "\n".join(parts)
