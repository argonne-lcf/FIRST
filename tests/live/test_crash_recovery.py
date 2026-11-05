"""
Sweeper recovery after an apiserver crash.

Held streams on the chaos model (slow, never-ending) own reservations whose
leases the apiserver renews every ~10s.  SIGKILLing the apiserver leaves those
reservations in Redis with nobody to settle or renew them: the controller's
InflightObserver must sweep them within about lease_sec (30s) + its poll
interval (30s).
"""

import asyncio
import time
from datetime import datetime, timedelta, timezone

from first_common.schema.auth import UserAuthEvent

from .backend import CHAOS_MODEL, Compose, RedisState, log_message
from .conftest import LIVE, Chaos
from .harness import Gateway, LiveConfig, Run, wait_for

pytestmark = LIVE

LEASE_SEC = 30
SWEEP_SEC = 30
SLACK_SEC = 15


async def test_sweeper_recovers_reservations_after_apiserver_crash(
    gw: Gateway,
    cfg: LiveConfig,
    run: Run,
    compose: Compose,
    redis: RedisState,
    chaos: Chaos,
    me: UserAuthEvent,
) -> None:
    """If the apiserver crashes mid-request, the requests' reservations are
    orphaned in Redis (nobody is left to settle or renew them).  The
    controller's sweeper must clean them up within about lease (30s) + sweep
    interval (30s), and the user's slots must work again afterwards."""
    await chaos.reset_all(redis, me.id)
    for b in chaos.backends:
        await chaos.set(b, "slow", interval=1.0)
    since = datetime.now(timezone.utc) - timedelta(seconds=2)
    n = 4
    before = await redis.model_inflight(CHAOS_MODEL)
    hold = None
    try:  # (the apiserver is restarted before any check can fail)
        run.step(f"Opened {n} never-ending streams on the chaos model")
        async with gw.hold(CHAOS_MODEL, n, max_tokens=64, tag="crash") as hold:
            ours = await redis.model_inflight(CHAOS_MODEL) - before
            run.check(
                len(hold.admitted) == n and len(ours) == n,
                f"All {n} streams were admitted and hold reservations",
                f"{len(hold.admitted)} admitted, {len(ours)} reservations in Redis",
            )

            t_kill = time.monotonic()
            await compose.kill(cfg.apiserver_service, "KILL")
            run.step(
                f"Crashed the apiserver (docker compose kill -s KILL {cfg.apiserver_service})"
            )
            left = {rid for rid in ours if await redis.reservation(rid)}
            deadlines = [await redis.deadline(rid) for rid in left]
            now = await redis.now()
            expiries = sorted(round(d - now) for d in deadlines if d is not None)

            await compose.start(cfg.apiserver_service)
            run.step("Started the apiserver again")
            await wait_for(
                "the apiserver to be healthy", gw.healthy, timeout=90, interval=1
            )
            run.check(
                left == ours,
                "The crash left the reservations orphaned (nothing settled them)",
                f"{len(left)}/{n} still in Redis right after the kill; their "
                f"leases run out in {expiries}s",
            )
            await asyncio.sleep(1)
            cut = [s.stream_error or "closed" for s in hold.streams]
            run.check(
                not hold.live, "The users' streams were cut by the crash", f"{cut}"
            )

            async def swept() -> bool:
                live = await redis.model_inflight(CHAOS_MODEL)
                return not (live & ours)

            budget = LEASE_SEC + SWEEP_SEC + SLACK_SEC
            await wait_for(
                "the sweeper to clear the orphaned reservations",
                swept,
                timeout=max(5.0, budget - (time.monotonic() - t_kill)),
                interval=1,
            )
            t_swept = time.monotonic() - t_kill
            gone = [
                await redis.reservation(rid) is None
                and await redis.deadline(rid) is None
                for rid in ours
            ]
            run.check(
                all(gone),
                "Every orphaned reservation was removed (blob and deadline)",
                f"{sum(gone)}/{n} removed",
            )
            run.check(
                LEASE_SEC - 5 <= t_swept <= budget,
                f"Clean-up happened after the lease lapsed and within lease + sweep "
                f"+ {SLACK_SEC}s slack",
                f"{t_swept:.0f}s after the crash (window {LEASE_SEC - 5}-{budget}s)",
            )
    finally:
        await chaos.reset_all(redis, me.id)

    total = await gw.total_inflight(CHAOS_MODEL)
    run.check(
        total == len(before),
        "The catalog's in-flight count is back to its baseline",
        f"total_inflight {total} (baseline {len(before)})",
    )
    r = await gw.chat(CHAOS_MODEL, tag="after-crash")
    run.check(r.ok, "A new request from the same user succeeds", r.summary())

    rows = await compose.logs(cfg.controller_service, since)
    sweeps = [
        log_message(r) for r in rows if "Reservation sweeper settled" in log_message(r)
    ]
    run.check(bool(sweeps), "The controller logged the sweep", f"{sweeps}")
    run.evidence(f"{n} orphaned reservations swept {t_swept:.0f}s after a SIGKILL")
