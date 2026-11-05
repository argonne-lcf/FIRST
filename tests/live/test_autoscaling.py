"""
Autoscaling ladder walk on the fleet model (long-running).

Uses the profile's two-rung scaling strategy on every autoscaled
PilotDeployment of the fleet model (short scale_down_sustain_sec).

Phases, with a 5s timeline of desired replicas / replica states / in-flight /
capacity rejects / the controller's demand EWMA:

  P0  forced idle (desired 0, no live replicas)
  P1  cold start: first request 503s with a startup message, desired 0 -> 1
  P2  demand below rung 2: desired stays at 1
  P3  demand above rung 2: desired 1 -> 2, the new replica takes traffic
  P4  demand back below rung 2: 2 -> 1, but not before the sustain period
  P5  no demand: 1 -> 0, again only after the sustain period
  then the controller's "desired_replicas a -> b (reason)" log lines are
  checked against the timeline.
"""

import asyncio
import math
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from .backend import Admin, Compose, log_message
from .conftest import LIVE
from .harness import (
    LIVE_STATES,
    STARTUP_TIMEOUT,
    DemandPump,
    Gateway,
    LiveConfig,
    Run,
    Timeline,
    all_deps,
)

pytestmark = LIVE


async def test_autoscaling_ladder(
    gw: Gateway, cfg: LiveConfig, run: Run, admin: Admin, compose: Compose
) -> None:
    """The autoscaler walks the replica ladder in response to demand: it
    starts a replica on the first request to an idle model, adds a second
    only when demand passes the second rung, and removes replicas only after
    demand has stayed low for the sustain period."""
    model = cfg.fleet_model
    summary = await gw.model(model)
    deps: list[str] = []
    strategies: dict[str, dict[str, Any]] = {}
    for d in summary["pilot_deployments"]:
        detail = await gw.pilot_deployment(d["name"])
        st = detail.get("scaling_strategy")
        if st and detail["max_replicas"] >= 2 and len(st["scaling_thresholds"]) >= 2:
            deps.append(d["name"])
            strategies[d["name"]] = st
    if not deps:
        pytest.skip(f"{model}: no deployment with a 2-rung scaling_strategy")
    sustain = max(s["scale_down_sustain_sec"] for s in strategies.values())
    if sustain > 900:
        pytest.skip(f"scale_down_sustain_sec={sustain}: apply the test profile")
    rung2 = min(s["scaling_thresholds"][1][0] for s in strategies.values())
    cold = all(s["immediate_cold_start"] for s in strategies.values())
    low = max(1, math.floor(rung2 * 0.5))
    high = math.ceil(rung2 * 1.7)
    run.note(
        f"Deployments {deps}: second rung at demand {rung2}, scale-down sustain "
        f"{sustain}s; low demand = {low} concurrent streams, high = {high}"
    )
    since = datetime.now(timezone.utc) - timedelta(seconds=2)

    # Honor the full Retry-After: fast retries are themselves demand
    # (reject_rate * avg_request_duration_sec) and would skip rungs.
    pump = DemandPump(gw, model, max_tokens=6000, max_backoff=30.0)
    tl = Timeline(gw, model, pump, run)
    tl.start()
    try:
        tl.phase = "P0"
        for d in deps:
            await admin.set_desired_replicas(d, 0)
        run.step("P0: set desired replicas to 0 on every deployment (force idle)")
        await tl.until(
            "all deployments idle at 0 replicas",
            all_deps(
                deps,
                lambda d: (
                    d["desired"] == 0
                    and not any(n for s, n in d["replicas"].items() if s in LIVE_STATES)
                ),
            ),
            timeout=600,
        )
        await asyncio.sleep(30)  # let the demand EWMA settle at 0
        since = datetime.now(timezone.utc)

        tl.phase = "P1"
        t1 = time.time()
        run.step("P1: sent one request to the idle model")
        first = await gw.chat(model, max_tokens=4, tag="cold")
        run.check(
            first.status == 503
            and "saturated" not in first.message
            and {"startup_eta_sec", "typical_startup_sec"} <= set(first.info),
            "It got a 503 explaining the model is starting, with startup timing",
            f"{first.summary()}; info {first.info}",
        )
        await tl.until(
            "cold start 0 -> 1",
            all_deps(deps, lambda d: d["desired"] >= 1),
            timeout=60 if cold else 180,
        )
        run.check(
            all(tl.desired(d) == 1 for d in deps),
            "Each deployment went to exactly 1 replica",
            f"desired {[tl.desired(d) for d in deps]}",
        )
        await pump.set_level(1)
        await tl.until(
            "a replica serves the first request",
            lambda _snap: pump.statuses[200] > 0,
            timeout=STARTUP_TIMEOUT,
        )
        run.note(
            f"First success {time.time() - t1:.0f}s after the cold request; 503s "
            f"while starting: {dict(pump.messages)}"
        )

        tl.phase = "P2"
        await pump.set_level(low)
        run.step(f"P2: held demand at {low} streams (below the second rung) for 90s")
        t2 = time.time()
        peak = 0
        while time.time() - t2 < 90:
            await asyncio.sleep(10)
            peak = max([peak, *(tl.desired(d) for d in deps)])
        run.check(
            peak <= 1,
            "It did not scale up below the second rung",
            f"highest desired replicas over 90s: {peak}",
        )

        tl.phase = "P3"
        await pump.set_level(high)
        run.step(f"P3: raised demand to {high} streams (above the second rung)")
        t3 = time.time()
        await tl.until(
            "scale up 1 -> 2", all_deps(deps, lambda d: d["desired"] == 2), timeout=120
        )
        run.check(
            True,
            "It scaled up to 2",
            f"desired=2 {time.time() - t3:.0f}s after demand rose",
        )
        await tl.until(
            "a second replica ready",
            lambda snap: any(
                snap["deployments"][d]["replicas"].get("ready", 0) >= 2 for d in deps
            ),
            timeout=STARTUP_TIMEOUT,
        )

        tl.phase = "P4"
        await pump.set_level(low)
        run.step(f"P4: dropped demand back to {low} streams")
        t4 = time.time()
        await tl.until(
            "scale down 2 -> 1",
            all_deps(deps, lambda d: d["desired"] <= 1),
            timeout=sustain + 120,
        )
        after4 = {d: tl.first_time(d, 1, t4) for d in deps}
        run.check(
            all(at is not None and at - t4 >= sustain - 15 for at in after4.values())
            and all(tl.desired(d) == 1 for d in deps),
            f"It went 2 -> 1, but not before the {sustain}s sustain period",
            ", ".join(
                f"{d}: after {at - t4:.0f}s" if at else f"{d}: never"
                for d, at in after4.items()
            ),
        )

        tl.phase = "P5"
        await pump.stop()
        run.step("P5: stopped all demand")
        t5 = time.time()
        await tl.until(
            "scale down 1 -> 0",
            all_deps(deps, lambda d: d["desired"] == 0),
            timeout=sustain + 180,
        )
        after5 = {d: tl.first_time(d, 0, t5) for d in deps}
        run.check(
            all(at is not None and at - t5 >= sustain - 15 for at in after5.values()),
            f"It went 1 -> 0, again only after the {sustain}s sustain period",
            ", ".join(
                f"{d}: after {at - t5:.0f}s" if at else f"{d}: never"
                for d, at in after5.items()
            ),
        )
        await tl.until(
            "no reservations left", lambda snap: snap["total_inflight"] == 0, timeout=60
        )
    finally:
        await pump.stop()
        await tl.stop()

    bad = {s: n for s, n in pump.statuses.items() if s not in (200, 429, 503)}
    run.check(
        not bad,
        "Users only ever saw 200, 429 or 503",
        f"status counts {dict(pump.statuses)}",
    )

    rows = await compose.logs(cfg.controller_service, since)
    pat = re.compile(r"deployment (\S+) desired_replicas (\d+) -> (\d+) \((.*)\)")
    seen: dict[str, list[tuple[int, int, str]]] = {}
    for row in rows:
        if m := pat.search(log_message(row)):
            seen.setdefault(m[1], []).append((int(m[2]), int(m[3]), m[4]))
    run.step("Read the controller's desired_replicas log lines")
    for d in deps:
        reasons = {(a, b): r for a, b, r in seen.get(d, [])}
        sustained = f"sustained >= {strategies[d]['scale_down_sustain_sec']}s"
        ok = (
            all(w in reasons for w in ((0, 1), (1, 2), (2, 1), (1, 0)))
            and (not cold or reasons[(0, 1)].startswith("cold start"))
            and reasons[(1, 2)].startswith("scale up")
            and reasons[(2, 1)].startswith("scale down")
            and sustained in reasons[(1, 0)]
        )
        run.check(
            ok,
            f"{d}: the controller logged 0->1, 1->2, 2->1, 1->0 with the right reasons",
            "; ".join(f"{a}->{b} ({r})" for (a, b), r in reasons.items()) or "none",
        )
    run.evidence(
        f"0->1 on first request, 1->2 above rung {rung2}, 2->1 and 1->0 after "
        f">= {sustain}s"
    )
