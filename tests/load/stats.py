"""Per-request records -> per-step statistics and tables (stdlib only).

A record is one request, as written by `harness.py` (times in ms unless noted):

    t0, t1   start / end, seconds since the step began
    st       HTTP status (0 when the request raised)
    ok       200, and for a stream: content arrived and it ended with [DONE]
    err      failure label when not ok
    e2e      client round trip, request sent -> last byte
    ttfb     request sent -> response headers
    ttft     stream: request sent -> first content token
    gap      stream: longest pause between two events after the first token
    n        stream: number of events
    out      completion tokens reported by the backend
    gw, up, q, att   Server-Timing: gateway, upstream, queue, attempts
    dep      X-First-Deployment

Latency differences are taken per request and then aggregated, because
percentiles do not subtract:

    overhead = gw - up            the gateway's own processing
    network  = e2e - gw - q       everything outside the gateway (unary)
             = ttfb - gw - q      ... up to the response headers (stream)

For a stream, `gw` and `up` stop when the response headers are sent, so
`overhead` is the cost of opening the stream, not of relaying it.
"""

import math
from collections import Counter
from typing import Any, Iterable

Record = dict[str, Any]
Summary = dict[str, Any]

QUANTILES = (("p50", 0.50), ("p95", 0.95), ("p99", 0.99))


def parse_server_timing(value: str | None) -> dict[str, float]:
    """`gateway;dur=41.3, upstream;dur=38.9;desc="attempts=1"` -> ms by name.

    The attempt count is returned as `attempts`.  Entries added by other hops
    (a CDN appends its own) are kept if they carry a `dur`, ignored otherwise.
    """
    out: dict[str, float] = {}
    for entry in (value or "").split(","):
        name, *params = (part.strip() for part in entry.split(";"))
        for param in params:
            key, _, raw = param.partition("=")
            raw = raw.strip('"')
            try:
                if key == "dur":
                    out[name] = float(raw)
                elif key == "desc" and raw.startswith("attempts="):
                    out["attempts"] = float(raw.removeprefix("attempts="))
            except ValueError:
                pass
    return out


def distribution(values: Iterable[float | None]) -> dict[str, float] | None:
    """Nearest-rank percentiles, mean and max; None when there are no values."""
    xs = sorted(v for v in values if v is not None)
    if not xs:
        return None
    out = {"mean": sum(xs) / len(xs), "max": xs[-1]}
    for name, q in QUANTILES:
        out[name] = xs[max(0, math.ceil(q * len(xs)) - 1)]
    return out


def _diff(a: float | None, *rest: float | None) -> float | None:
    """a minus the rest; None when `a` or the first subtrahend is missing."""
    if a is None or rest[0] is None:
        return None
    return a - sum(r or 0.0 for r in rest)


def summarize(
    records: list[Record], *, users: int, warmup: float, duration: float, stream: bool
) -> Summary:
    """Statistics for the measurement window [warmup, warmup + duration].

    Throughput and latency use successful requests that *completed* inside the
    window.  Errors are counted over every request *started* after the warm-up,
    so a request that hangs past the end of the step still counts against it.
    """
    end = warmup + duration
    ok = [r for r in records if r["ok"] and warmup <= r["t1"] <= end]
    started = [r for r in records if r["t0"] >= warmup]
    failed = [r for r in started if not r["ok"]]

    summary: Summary = {
        "users": users,
        "duration_sec": duration,
        "requests_ok": len(ok),
        "rps": len(ok) / duration if duration else 0.0,
        "started": len(started),
        "failed": len(failed),
        "error_rate": len(failed) / len(started) if started else 0.0,
        "errors": dict(Counter(r["err"] or f"http {r['st']}" for r in failed)),
        "retried": sum(1 for r in ok if (r["att"] or 1) > 1),
        "deployments": dict(Counter(r["dep"] for r in ok if r["dep"])),
        "e2e": distribution(r["e2e"] for r in ok),
        "ttfb": distribution(r["ttfb"] for r in ok),
        "gateway": distribution(r["gw"] for r in ok),
        "upstream": distribution(r["up"] for r in ok),
        "queue": distribution(r["q"] for r in ok),
        "overhead": distribution(_diff(r["gw"], r["up"]) for r in ok),
        "network": distribution(
            _diff(r["ttfb"] if stream else r["e2e"], r["gw"], r["q"]) for r in ok
        ),
        "out_tokens": distribution(r["out"] for r in ok),
    }
    if stream:
        summary["ttft"] = distribution(r["ttft"] for r in ok)
        summary["max_gap"] = distribution(r["gap"] for r in ok)
    return summary


# --- tables --------------------------------------------------------------------


def _ms(dist: dict[str, float] | None, key: str) -> str:
    if dist is None:
        return "–"
    value = dist[key]
    return f"{value:.0f}" if value >= 100 else f"{value:.1f}"


def _replicas(step: Summary) -> str:
    rep = step.get("replicas")
    if not rep:
        return "–"
    lo, hi = rep["min"], rep["max"]
    return str(lo) if lo == hi else f"{lo}–{hi} ⚠"


def _table(header: list[str], rows: list[list[str]]) -> str:
    lines = ["| " + " | ".join(header) + " |", "|" + "---:|" * len(header)]
    lines += ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join(lines)


def steps_table(steps: list[Summary], *, stream: bool) -> str:
    """One row per user count.  Times are ms, shown as p50 / p95 / p99."""

    def trio(dist: dict[str, float] | None) -> str:
        return " / ".join(_ms(dist, k) for k, _ in QUANTILES)

    header = ["users", "replicas", "req/s", "errors", "e2e"]
    header += ["ttft", "max gap"] if stream else []
    header += ["gateway", "upstream", "overhead", "queue", "network", "client CPU"]
    rows = []
    for s in steps:
        row = [
            str(s["users"]),
            _replicas(s),
            f"{s['rps']:.1f}",
            f"{s['error_rate'] * 100:.1f}%",
            trio(s["e2e"]),
        ]
        if stream:
            row += [trio(s.get("ttft")), trio(s.get("max_gap"))]
        row += [trio(s[k]) for k in ("gateway", "upstream", "overhead")]
        row += [trio(s["queue"]), trio(s["network"])]
        cpu = s.get("client_cpu")
        row.append(
            "–" if cpu is None else f"{cpu * 100:.0f}%" + (" ⚠" if cpu > 0.85 else "")
        )
        rows.append(row)
    return _table(header, rows)


def peak(steps: list[Summary], *, max_error_rate: float = 0.01) -> Summary | None:
    """The step with the highest throughput among those that stayed healthy."""
    healthy = [s for s in steps if s["error_rate"] <= max_error_rate and s["rps"] > 0]
    return max(healthy, key=lambda s: s["rps"]) if healthy else None


def comparison_table(runs: list[dict[str, Any]]) -> str:
    """Peak throughput of each run, relative to the first, and per replica.

    `scaling` is throughput per replica relative to the first run that has a
    replica count: 100% means throughput grew in proportion to replicas.
    """
    header = ["run", "mode", "replicas", "peak req/s", "at users", "vs first"]
    header += ["req/s per replica", "scaling", "e2e p50 / p95", "overhead p50 / p95"]
    rows = []
    first_rps: float | None = None
    first_per_replica: float | None = None
    for run in runs:
        meta = run["meta"]
        best = peak(run["steps"])
        if best is None:
            rows.append([meta["name"], meta["mode"]] + ["–"] * 8)
            continue
        first_rps = first_rps or best["rps"]
        replicas = (best.get("replicas") or {}).get("min")
        per_replica = best["rps"] / replicas if replicas else None
        if per_replica and first_per_replica is None:
            first_per_replica = per_replica
        rows.append(
            [
                meta["name"],
                meta["mode"],
                _replicas(best),
                f"{best['rps']:.1f}",
                str(best["users"]),
                f"{best['rps'] / first_rps:.2f}×",
                f"{per_replica:.1f}" if per_replica else "–",
                f"{per_replica / first_per_replica * 100:.0f}%"
                if per_replica and first_per_replica
                else "–",
                f"{_ms(best['e2e'], 'p50')} / {_ms(best['e2e'], 'p95')}",
                f"{_ms(best['overhead'], 'p50')} / {_ms(best['overhead'], 'p95')}",
            ]
        )
    return _table(header, rows)


CSV_COLUMNS = ["users", "replicas_min", "replicas_max", "rps", "error_rate"]
CSV_DISTS = ["e2e", "ttfb", "ttft", "max_gap", "gateway", "upstream"]
CSV_DISTS += ["overhead", "queue", "network"]


def steps_csv(steps: list[Summary]) -> str:
    """Flat per-step table for plotting."""
    cols = CSV_COLUMNS + [f"{d}_{k}" for d in CSV_DISTS for k, _ in QUANTILES]
    lines = [",".join(cols + ["client_cpu"])]
    for s in steps:
        rep = s.get("replicas") or {}
        row = [s["users"], rep.get("min", ""), rep.get("max", "")]
        row += [f"{s['rps']:.3f}", f"{s['error_rate']:.5f}"]
        for d in CSV_DISTS:
            dist = s.get(d)
            row += [f"{dist[k]:.3f}" if dist else "" for k, _ in QUANTILES]
        row.append("" if s.get("client_cpu") is None else f"{s['client_cpu']:.3f}")
        lines.append(",".join(str(c) for c in row))
    return "\n".join(lines) + "\n"
