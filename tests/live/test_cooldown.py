"""
Backend cooldown ("benching").

record_error.lua counts upstream errors per backend; once the count reaches
cooldown_threshold within cooldown_window_sec, the key's TTL is re-armed to
cooldown_bench_sec and admit.lua skips the backend until it expires.  When
every candidate is benched the user gets 503 "Backend in cooldown phase."
"""

import asyncio
import time
from datetime import datetime, timedelta, timezone

import pytest

from first_common.schema.auth import UserAuthEvent

from .backend import CHAOS_MODEL, Compose, RedisState, traces
from .conftest import CHAOS_ROUTER, LIVE, Chaos, require_model
from .harness import Gateway, LiveConfig, Run, wait_for

pytestmark = LIVE

THRESHOLD = CHAOS_ROUTER["cooldown_threshold"]
BENCH_SEC = CHAOS_ROUTER["cooldown_bench_sec"]
WINDOW_SEC = CHAOS_ROUTER["cooldown_window_sec"]


async def _static_cooldown_errors(gw: Gateway, deployment: str) -> int:
    for sd in await gw.get_json("/catalog/v1/deployments/static"):
        if sd["name"] == deployment:
            return int(sd["runtime"]["cooldown_errors"])
    raise LookupError(deployment)


async def test_errors_bench_backend_until_bench_expires(
    gw: Gateway,
    run: Run,
    compose: Compose,
    redis: RedisState,
    chaos: Chaos,
    me: UserAuthEvent,
) -> None:
    """A backend that keeps failing is taken out of rotation for a while.
    After `cooldown_threshold` errors it is benched: requests pinned to it are
    refused with "Backend in cooldown phase." without contacting it, other
    traffic routes around it, and it serves again once the bench period
    ends."""
    await chaos.reset_all(redis, me.id)
    a, a_id = chaos.deployment("a"), chaos.ids["a"]
    await chaos.set("a", "status", status=503)
    run.step(
        f"Made chaos backend a return HTTP 503 (threshold {THRESHOLD} errors in "
        f"{WINDOW_SEC}s, bench {BENCH_SEC}s)"
    )
    since = datetime.now(timezone.utc) - timedelta(seconds=2)
    try:
        # Enter cooldown --------------------------------------------------------
        run.step(f"Sent {THRESHOLD} requests pinned to a")
        for i in range(THRESHOLD):
            r = await gw.chat(CHAOS_MODEL, deployment=a, tag=f"bench-{i}")
            count, ttl = await redis.errors(a_id)
            benched = count >= THRESHOLD
            limit = BENCH_SEC if benched else WINDOW_SEC
            run.check(
                r.status == 503
                and "All 1 backend(s) failed" in r.message
                and count == i + 1
                and 0 < ttl <= limit,
                f"Error {i + 1} was relayed as a 503 and counted"
                + (" (and benched the backend)" if benched else ""),
                f"{r.summary()}; error counter {count}, TTL {ttl}s (limit {limit}s: "
                + ("the bench period" if benched else "the counting window")
                + ")",
            )
        t_benched = time.monotonic()
        catalog = await _static_cooldown_errors(gw, a)
        run.check(
            catalog == THRESHOLD,
            "The catalog reports the backend's cooldown errors",
            f"runtime.cooldown_errors = {catalog}",
        )

        # Benched: refused before routing -----------------------------------
        hits = (await chaos.hits())["a"]
        run.step("Sent another request pinned to a while it is benched")
        r = await gw.chat(CHAOS_MODEL, deployment=a, tag="benched")
        run.check(
            r.status == 503 and "Backend in cooldown phase." in r.message,
            'It was refused with 503 "Backend in cooldown phase."',
            r.summary(),
        )
        run.check(
            r.info.get("reject_reason") == "capacity:all_benched"
            and r.retry_after is not None,
            "The error says why (capacity:all_benched) and when to retry",
            f"reject_reason={r.info.get('reject_reason')}, Retry-After {r.retry_after}",
        )
        now_hits = (await chaos.hits())["a"]
        run.check(
            now_hits == hits,
            "The benched backend was not contacted",
            f"backend a request count stayed at {now_hits}",
        )
        tr = (await traces(compose, since, [r.marker]))[r.marker]
        run.check(
            tr.outcomes == ["admission_rejected"]
            and tr.inference[0]["error"] == "capacity:all_benched",
            "It is logged as an admission rejection",
            f"InferenceLog outcomes {tr.outcomes}",
        )

        # Federated traffic routes around it ---------------------------------
        run.step("Sent 5 unpinned requests (a has the highest routing weight)")
        landed = []
        for i in range(5):
            r = await gw.chat(CHAOS_MODEL, tag=f"around-{i}")
            run.check(r.ok, f"Request {i + 1} succeeded", r.summary(), soft=True)
            landed.append(r.deployment)
        run.check(
            a not in landed and (await chaos.hits())["a"] == hits,
            "Unpinned traffic routed around the benched backend",
            f"served by {sorted(set(map(str, landed)))}",
        )

        # The backend comes back after the bench -----------------------------
        await chaos.set("a", "ok")
        run.step("Made backend a healthy again and kept retrying requests pinned to it")

        async def served() -> bool:
            return (await gw.chat(CHAOS_MODEL, deployment=a, tag="return")).ok

        await wait_for("backend a to serve again", served, timeout=BENCH_SEC + 15)
        back_after = time.monotonic() - t_benched
        run.check(
            back_after >= BENCH_SEC - 3,
            "It came back only after the bench period",
            f"served again {back_after:.0f}s after benching (bench {BENCH_SEC}s)",
        )
        count, _ = await redis.errors(a_id)
        run.check(count == 0, "Its error counter expired", f"counter {count}")
        run.evidence(
            f"benched after {THRESHOLD} errors, refused while benched, back after "
            f"{back_after:.0f}s (bench {BENCH_SEC}s)"
        )
    finally:
        await chaos.reset_all(redis, me.id)


async def test_all_backends_benched_503(
    gw: Gateway, cfg: LiveConfig, run: Run, redis: RedisState
) -> None:
    """With the fleet model's backends benched directly in Redis: when only
    one deployment is left unbenched, all traffic goes there; when every
    backend is benched, users get 503 "Backend in cooldown phase."; and when
    the bench expires, the model serves again by itself."""
    model = cfg.fleet_model
    await require_model(gw, model)
    m = await gw.model_config(model)
    by_dep = {
        d["name"]: (
            d["router_params"]["cooldown_threshold"],
            [b["id"] for b in d["backends"]],
        )
        for d in m["deployments"]
        if d["backends"] and d["router_params"]["weight"] > 0
    }
    all_ids = [b for _, ids in by_dep.values() for b in ids]
    if not all_ids:
        pytest.skip(f"{model} has no routable backends")
    bench_sec = 15
    try:
        if len(by_dep) >= 2:
            spared, *rest = sorted(by_dep)
            for name in rest:
                threshold, ids = by_dep[name]
                for b in ids:
                    await redis.bench(b, threshold, 60)
            run.step(
                f"Benched every backend except those of {spared} "
                "(error counter set to the threshold in Redis)"
            )
            landed = {
                (await gw.chat(model, max_tokens=1, tag="spared")).deployment
                for _ in range(8)
            }
            run.check(
                landed == {spared},
                f"All 8 requests went to the one unbenched deployment {spared}",
                f"served by {landed}",
            )
        else:
            run.note("Only one routable deployment: skipped the partial-bench step")

        for _name, (threshold, ids) in by_dep.items():
            for b in ids:
                await redis.bench(b, threshold, bench_sec)
        run.step(f"Benched all {len(all_ids)} backends for {bench_sec}s")
        t0 = time.monotonic()
        r = await gw.chat(model, max_tokens=1, tag="all-benched")
        run.check(
            r.status == 503 and "Backend in cooldown phase." in r.message,
            'A request got 503 "Backend in cooldown phase."',
            r.summary(),
        )
        overload = (await gw.model(model))["overload"]
        run.check(
            r.info.get("reject_reason") == "capacity:all_benched"
            and r.retry_after is not None
            and r.retry_after >= overload["short_retry_sec"],
            "It carries capacity:all_benched and the model's short Retry-After",
            f"reject_reason={r.info.get('reject_reason')}, Retry-After "
            f"{r.retry_after} (short_retry_sec {overload['short_retry_sec']})",
        )

        await asyncio.sleep(max(0.0, bench_sec - (time.monotonic() - t0)) + 1)
        run.step(f"Waited for the {bench_sec}s bench to expire on its own")
        r = await gw.chat(model, max_tokens=1, tag="bench-lapsed")
        run.check(r.ok, "The model serves again", r.summary())
        run.evidence(
            f"all benched -> 503 cooldown; after {bench_sec}s -> {r.status} via "
            f"{r.deployment}"
        )
    finally:
        await redis.unbench(*all_ids)
