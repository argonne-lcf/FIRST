"""
Admission control, observed end to end.

Roles (see README.md for the manifest profile):
- cfg.rpm_model: small RPM bucket + small max_user_concurrency
- cfg.tpm_model: small TPM bucket, RPM unlimited
- cfg.fleet_model: unlimited quota; only backend capacity binds

Expectations come from the limits the catalog reports for *you* (default +
overrides, resolved like the gateway does).  Buckets are emptied and token
estimates pinned directly in Redis, so nothing waits for a refill or an EWMA
to converge.
"""

import asyncio
import math
import time
from datetime import datetime, timezone

import pytest

from first_common.schema.auth import UserAuthEvent
from first_common.schema.types import OverloadPolicy

from .backend import CHAOS_MODEL, RedisState
from .conftest import LIVE, Chaos, require_model
from .harness import Gateway, Gcra, LiveConfig, Reply, Run, filler_text, wait_for

pytestmark = LIVE

# Predictions closer than this to the GCRA decision boundary are not asserted:
# client send time vs. Redis TIME differ by up to ~one request latency.
BOUNDARY_SLACK_SEC = 0.75


async def _overload(gw: Gateway, model: str) -> OverloadPolicy:
    return OverloadPolicy.model_validate((await gw.model(model))["overload"])


def _jitter(policy: OverloadPolicy) -> float:
    return max(0, policy.retry_jitter_percent) / 100


def _short_retry_window(policy: OverloadPolicy) -> tuple[int, int]:
    """Bounds of a Retry-After built from `short_retry_sec`: rounded up and
    stretched by up to `retry_jitter_percent`, never shortened."""
    base = policy.short_retry_sec
    return math.ceil(base), math.ceil(base * (1 + _jitter(policy)))


# ---------------------------------------------------------------------------
# RPM
# ---------------------------------------------------------------------------


async def _rpm_setup(
    gw: Gateway, cfg: LiveConfig, redis: RedisState, me: UserAuthEvent
) -> tuple[str, Gcra, int]:
    model = cfg.rpm_model
    await require_model(gw, model)
    lim = await gw.effective_limits(model)
    if lim.rpm <= 0:
        pytest.skip(f"{model}: rpm is unlimited for you")
    await redis.reset_quota(model, me.id)
    return model, Gcra(lim.requests_per_sec, lim.burst_requests), lim.rpm


def _pct(xs: list[bool]) -> str:
    return f"{sum(xs)}/{len(xs)}"


async def test_rpm_gcra_matches_reference(
    gw: Gateway, cfg: LiveConfig, run: Run, redis: RedisState, me: UserAuthEvent
) -> None:
    """Requests-per-minute limiting admits and rejects exactly as the
    configured GCRA bucket says it should.  Every decision, and every
    Retry-After the gateway hands out, is compared with a client-side copy of
    the algorithm."""
    model, ref, rpm = await _rpm_setup(gw, cfg, redis, me)
    burst = int(ref.burst)
    jitter = _jitter(await _overload(gw, model))
    interval = 1 / ref.rate_per_sec
    run.step(
        f"Emptied the RPM bucket of {model} (rpm {rpm}, burst {burst}: one "
        f"request per {interval:.1f}s after the burst)"
    )

    t0 = time.time()
    replies: list[tuple[Reply, bool, float]] = []

    async def send() -> None:
        reply = await gw.chat(model, max_tokens=1, tag="rpm")
        allowed, margin = ref.evaluate(reply.sent_at)
        replies.append((reply, allowed, margin))
        run.note(
            f"t={reply.sent_at - t0:5.1f}s: {reply.summary()} (reference says "
            f"{'admit' if allowed else 'reject'}, {margin:.1f}s from the boundary)"
        )

    run.step(f"Sent {burst + 2} requests back to back")
    for _ in range(burst + 2):
        await send()
    run.step(f"Waited {interval * 1.5:.1f}s (1.5 refill intervals), then sent 2 more")
    await asyncio.sleep(interval * 1.5)
    await send()
    await send()

    judged = [(r, a, m) for r, a, m in replies if m >= BOUNDARY_SLACK_SEC]
    run.check(
        all(r.status in (200, 429) for r, _, _ in replies),
        "Every response was an admit (200) or an RPM reject (429)",
        f"statuses {[r.status for r, _, _ in replies]}",
    )
    run.check(
        len(judged) >= len(replies) - 2,
        "Enough decisions were clear of the timing boundary to judge",
        f"{len(judged)}/{len(replies)} at least {BOUNDARY_SLACK_SEC}s from it",
    )
    matches = [r.ok == a for r, a, _ in judged]
    run.check(
        all(matches),
        "Every decision matched the reference bucket",
        f"{_pct(matches)} matched",
    )
    rejects = [(r, m) for r, a, m in judged if not r.ok]
    good = [
        f"Requests per minute above {rpm}" in r.message
        and r.retry_after is not None
        and m - 1 <= r.retry_after <= m * (1 + jitter) + 1.5
        for r, m in rejects
    ]
    run.check(
        all(good),
        "Each reject names the limit and gives the right Retry-After (the "
        f"reference wait, rounded up, plus up to {jitter:.0%} jitter)",
        "; ".join(f"Retry-After {r.retry_after} vs wait {m:.1f}s" for r, m in rejects)
        or "no rejects to judge",
    )
    first = sum(r.ok for r, _, _ in replies[: burst + 2])
    run.check(
        first == burst,
        "The initial burst admitted exactly burst_requests",
        f"{first} of {burst + 2} admitted (burst {burst})",
    )
    run.evidence(
        f"{_pct(matches)} decisions matched the reference; {len(rejects)} rejects "
        "with correct Retry-After"
    )


async def test_rpm_retry_after_is_sufficient(
    gw: Gateway, cfg: LiveConfig, run: Run, redis: RedisState, me: UserAuthEvent
) -> None:
    """A client that is told to retry after N seconds, and does exactly that,
    gets in.  (The wait is rounded up and only ever jittered upward.)"""
    model, ref, _ = await _rpm_setup(gw, cfg, redis, me)
    interval = 1 / ref.rate_per_sec
    results = []
    for trial in range(3):
        # The burst is filled one request at a time (max_user_concurrency is
        # below the burst).  A slow backend can stretch that past one refill
        # interval, and then the "over-limit" request is legitimately
        # admitted, so such a fill is redone rather than judged.
        for attempt in range(5):
            await redis.reset_quota(model, me.id)
            t_fill = time.monotonic()
            for _ in range(int(ref.burst)):
                await gw.chat(model, max_tokens=1, tag="rpm-fill")
            filled_in = time.monotonic() - t_fill
            if filled_in < 0.8 * interval:
                break
            run.note(
                f"Trial {trial + 1}: filling the burst took {filled_in:.1f}s "
                f"(refill interval {interval:.1f}s); redoing it"
            )
        else:
            pytest.skip(
                f"{model} is too slow to fill the burst within one refill "
                f"interval ({interval:.1f}s)"
            )
        rejected = await gw.chat(model, max_tokens=1, tag="rpm-over")
        run.step(
            f"Trial {trial + 1}: emptied the bucket, used up the burst in "
            f"{filled_in:.1f}s, and got {rejected.summary()}"
        )
        run.check(
            rejected.status == 429 and rejected.retry_after is not None,
            "The over-limit request was rejected with a Retry-After",
            rejected.summary(),
        )
        assert rejected.retry_after is not None
        await asyncio.sleep(rejected.retry_after)
        retried = await gw.chat(model, max_tokens=1, tag="rpm-retry")
        results.append(retried.ok)
        run.check(
            retried.ok,
            f"Retrying after exactly {rejected.retry_after}s was admitted",
            retried.summary(),
            soft=True,
        )
    run.check(all(results), "Retry-After was long enough every time", _pct(results))
    run.evidence(f"{_pct(results)} retries after exactly Retry-After were admitted")


# ---------------------------------------------------------------------------
# max_user_concurrency
# ---------------------------------------------------------------------------


async def test_max_user_concurrency(
    gw: Gateway, cfg: LiveConfig, run: Run, redis: RedisState, me: UserAuthEvent
) -> None:
    """A user can only have max_user_concurrency requests running at once.
    The next one is refused with a 429 even though the RPM bucket has room,
    it is not counted as a capacity problem (so it doesn't trigger
    autoscaling), and the slots free up when the streams close."""
    model = cfg.rpm_model
    await require_model(gw, model)
    lim = await gw.effective_limits(model)
    c = lim.max_user_concurrency
    if c > 8:
        pytest.skip(f"{model}: max_user_concurrency={c} too large for this test")
    retry_lo, retry_hi = _short_retry_window(await _overload(gw, model))
    await redis.reset_quota(model, me.id)
    rejects_before = (await gw.model_runtime(model))["capacity_rejects_total"]

    async with gw.hold(model, c, max_tokens=4000, tag="conc") as hold:
        run.step(f"Opened {c} long streams on {model} (max_user_concurrency {c})")
        run.check(
            len(hold.admitted) == c,
            f"All {c} were admitted",
            f"{len(hold.admitted)} admitted",
        )
        await redis.reset_quota(model, me.id)
        run.step(
            "Emptied the RPM bucket (so RPM cannot be the reason) and sent one more"
        )
        extra = await gw.chat(model, max_tokens=1, tag="conc-over")
        hold.require_live(c, "the probe")
        run.check(
            extra.status == 429
            and f"Concurrent requests above {c}" in extra.message
            and extra.info.get("reject_reason") == "quota:user_concurrency",
            "It was refused for concurrency",
            extra.summary(),
        )
        run.check(
            extra.retry_after is not None and retry_lo <= extra.retry_after <= retry_hi,
            "It carries the model's short Retry-After",
            f"Retry-After {extra.retry_after} (allowed {retry_lo}-{retry_hi})",
        )
        rt = await gw.model_runtime(model)
        run.check(
            rt["capacity_rejects_total"] == rejects_before,
            "It was not counted as a capacity reject (no autoscaling demand)",
            f"capacity_rejects_total stayed at {rejects_before}",
        )

    run.step("Closed the streams")

    async def released() -> bool:
        return await gw.total_inflight(model) <= int(rt["total_inflight"]) - c

    await wait_for("the user's slots to be released", released, timeout=20, interval=1)
    await redis.reset_quota(model, me.id)
    after = await gw.chat(model, max_tokens=1, tag="conc-after")
    run.check(after.ok, "A new request is admitted", after.summary())
    run.evidence(f"request {c + 1} refused for concurrency; admitted after closing")


# ---------------------------------------------------------------------------
# TPM
# ---------------------------------------------------------------------------

# Pinned estimation EWMAs: estimate = int(chars / CPT) + min(OUT, max_tokens).
CPT = 4.0
OUT = 8


async def _tpm_setup(
    gw: Gateway, cfg: LiveConfig, redis: RedisState, me: UserAuthEvent
) -> tuple[str, Gcra, int, int]:
    """(model, reference GCRA, max_user_concurrency, max_model_len), with an
    empty bucket."""
    model = cfg.tpm_model
    await require_model(gw, model)
    lim = await gw.effective_limits(model)
    if lim.tpm <= 0:
        pytest.skip(f"{model}: tpm is unlimited for you")
    if 0 < lim.rpm < 120:
        pytest.skip(f"{model}: rpm={lim.rpm} would interfere; set rpm: 0")
    await redis.reset_quota(model, me.id)
    max_model_len = int((await gw.model(model))["max_model_len"])
    return (
        model,
        Gcra(lim.tokens_per_sec, lim.burst_tokens),
        lim.max_user_concurrency,
        max_model_len,
    )


def _estimate(prompt_chars: int, max_tokens: int, out: float | None) -> int:
    """estimate_total_tokens for a text-only chat with chars-per-token CPT."""
    input_tokens = max(1, int(prompt_chars / CPT))
    output = max_tokens if out is None else min(round(out), max_tokens)
    return input_tokens + output


async def test_tpm_burst_and_refill(
    gw: Gateway, cfg: LiveConfig, run: Run, redis: RedisState, me: UserAuthEvent
) -> None:
    """Tokens-per-minute limiting: a burst of concurrent requests admits as
    many as fit in the token bucket and rejects the rest, and over a longer
    period the tokens actually used track the configured rate."""
    model, ref, max_conc, _ = await _tpm_setup(gw, cfg, redis, me)
    burst = int(ref.burst)
    prompt = filler_text(int(burst / 5 * CPT), seed=1)
    # Markers are ~30-45 characters; that is <4 tokens of estimate either way.
    est_lo = _estimate(len(prompt) + 30, 8, OUT)
    est_hi = _estimate(len(prompt) + 45, 8, OUT)
    fit_lo, fit_hi = burst // est_hi, burst // est_lo
    k = fit_hi + 3
    if k > max_conc:
        pytest.skip(f"need max_user_concurrency >= {k}, have {max_conc}")

    await redis.set_token_stats(model, me.id, chars_per_token=CPT, output_tokens=OUT)
    run.step(
        f"Emptied the TPM bucket of {model} (burst {burst} tokens) and fixed the "
        f"estimator in Redis, so each request reserves {est_lo}-{est_hi} tokens"
    )
    run.step(f"Sent {k} identical requests at once")
    replies = await asyncio.gather(
        *(gw.chat(model, prompt, max_tokens=8, tag="tpm-burst") for _ in range(k))
    )
    ok = [r for r in replies if r.ok]
    rejected = [r for r in replies if not r.ok]
    run.check(
        fit_lo <= len(ok) <= fit_hi,
        "The burst admitted exactly as many as fit in the bucket",
        f"{len(ok)} admitted; {burst} // {est_hi}..{est_lo} = {fit_lo}..{fit_hi}",
    )
    run.check(
        all(
            r.status == 429
            and "Tokens per minute above" in r.message
            and r.info.get("reject_reason") == "quota:user_tpm"
            and r.retry_after is not None
            for r in rejected
        ),
        "The rest were refused for TPM with a Retry-After",
        "; ".join(sorted({r.summary() for r in rejected})) or "none rejected",
    )

    window = 45.0
    run.step(
        f"Kept sending for {window:.0f}s, waiting out every Retry-After like a "
        "polite client"
    )
    t_start = time.time()
    admitted_tokens = sum(int(r.usage["total_tokens"]) for r in ok if r.usage)
    attempts = 0
    while time.time() - t_start < window:
        r = await gw.chat(model, prompt, max_tokens=8, tag="tpm-sustain")
        attempts += 1
        if r.ok and r.usage:
            admitted_tokens += int(r.usage["total_tokens"])
        elif r.status == 429:
            await asyncio.sleep(max(r.retry_after or 0, 0.25))
        else:
            run.check(False, "Only 200s and TPM 429s were returned", r.summary())
    elapsed = time.time() - t_start
    budget = burst + ref.rate_per_sec * elapsed
    ratio = admitted_tokens / budget
    run.check(
        admitted_tokens <= budget + 2 * est_hi,
        "Tokens used never exceeded the budget (burst + rate x time)",
        f"{admitted_tokens} tokens used over {elapsed:.0f}s; budget {budget:.0f}",
    )
    run.check(
        ratio >= 0.6,
        "A polite client still got most of the budget",
        f"{ratio:.0%} of the budget ({attempts} attempts)",
    )
    run.evidence(
        f"burst admitted {len(ok)} (expected {fit_lo}-{fit_hi}); sustained "
        f"{ratio:.0%} of the TPM budget"
    )


async def test_tpm_settle_refunds_overestimate(
    gw: Gateway, cfg: LiveConfig, run: Run, redis: RedisState, me: UserAuthEvent
) -> None:
    """The gateway reserves an estimated token count up front and corrects it
    to the actual usage when the request finishes.  Requests that each
    reserve 60% of the bucket but use only a few tokens must therefore be
    admitted back to back: without the refund the second one would be
    refused."""
    model, ref, _, max_model_len = await _tpm_setup(gw, cfg, redis, me)
    burst = int(ref.burst)
    prompt = "Reply with the single word OK."
    # With no learned output EWMA the reservation is the full max_tokens.
    max_tokens = int(burst * 0.6)
    est = _estimate(len(prompt) + 45, max_tokens, None)
    run.check(
        est < burst < 2 * est and max_tokens < max_model_len,
        "Setup: one reservation fits in the bucket, two do not",
        f"reservation ~{est} tokens, bucket {burst}",
    )

    used = 0
    debts = []
    t0 = await redis.now()
    run.step(
        f"Emptied the TPM bucket, then sent 4 requests one after another, each "
        f"with max_tokens={max_tokens} and the output estimate cleared (so each "
        f"reserves ~{est} tokens) and a prompt that needs only a short answer"
    )
    for i in range(4):
        await redis.set_token_stats(
            model, me.id, chars_per_token=CPT, output_tokens=None
        )
        r = await gw.chat(model, prompt, max_tokens=max_tokens, tag=f"refund-{i}")
        run.check(r.ok and bool(r.usage), f"Request {i + 1} was admitted", r.summary())
        assert r.usage
        used += int(r.usage["total_tokens"])
        tat = await redis.quota_tat(model, me.id, "tokens")
        now = await redis.now()
        debt = max(0.0, (tat or now) - now) * ref.rate_per_sec
        debts.append(round(debt))
        run.check(
            debt <= used + 1,
            f"After request {i + 1} the bucket holds only the tokens actually used",
            f"bucket debt {debt:.0f} tokens; used so far {used}; without the refund "
            f"it would be ~{(i + 1) * est}",
        )
    elapsed = await redis.now() - t0
    run.check(
        elapsed < ref.tau,
        "This happened faster than the bucket could have refilled on its own",
        f"{elapsed:.0f}s, vs {ref.tau:.0f}s to refill the whole bucket",
    )
    run.evidence(
        f"4 x ~{est}-token reservations admitted back to back in a {burst}-token "
        f"bucket; debt after each {debts} vs {used} tokens used"
    )


async def test_tpm_request_larger_than_burst(
    gw: Gateway, cfg: LiveConfig, run: Run, redis: RedisState, me: UserAuthEvent
) -> None:
    """A single request too large to ever fit in the token bucket is refused
    at once with a 400 that names the limit and has no Retry-After (waiting
    would not help), and the bucket is left untouched."""
    model, ref, _, _ = await _tpm_setup(gw, cfg, redis, me)
    await redis.set_token_stats(model, me.id, chars_per_token=CPT, output_tokens=OUT)
    prompt = filler_text(int(ref.burst * CPT * 2), seed=2)
    run.step(
        f"Emptied the bucket and sent one request estimated at ~{2 * int(ref.burst)} "
        f"tokens (bucket {int(ref.burst)})"
    )
    huge = await gw.chat(model, prompt, max_tokens=8, tag="tpm-huge")
    run.check(
        huge.status == 400 and huge.error.get("code") == "request_too_large",
        "It was refused with 400 request_too_large",
        huge.summary(),
    )
    run.check(
        huge.retry_after is None, "There is no Retry-After", f"{huge.retry_after}"
    )
    run.check(
        huge.info.get("reject_reason") == "request_too_large"
        and huge.info.get("burst_tokens") == int(ref.burst)
        and huge.info.get("estimated_tokens", 0) > ref.burst
        and f"{int(ref.burst):,} tokens" in huge.message,
        "The error names the limit and the estimate",
        f"info {huge.info}",
    )
    tat = await redis.quota_tat(model, me.id, "tokens")
    run.check(tat is None, "The token bucket was not touched", f"bucket state {tat}")
    run.evidence(f"400 request_too_large, estimate {huge.info.get('estimated_tokens')}")


# ---------------------------------------------------------------------------
# In-flight accounting, leases, settle paths
# ---------------------------------------------------------------------------


async def test_inflight_visible_in_catalog(
    gw: Gateway, cfg: LiveConfig, run: Run, redis: RedisState
) -> None:
    """Running requests are counted: N open streams show up as N in-flight in
    the catalog (per model and per deployment) and as N reservations in
    Redis, and closing them returns every count to where it was."""
    model = cfg.fleet_model
    await require_model(gw, model)
    cap, per_dep = await gw.routable_capacity(model)
    base_ids = await redis.model_inflight(model)
    base = len(base_ids)
    base_deps = await gw.deployment_inflight(model)
    n = min(6, cap - base)
    if n < 2:
        pytest.skip(f"not enough free capacity ({cap=} {base=})")

    async with gw.hold(model, n, tag="inflight") as hold:
        run.step(f"Opened {n} long streams on {model} ({base} already in flight)")
        ours = await redis.model_inflight(model) - base_ids
        rows = [await redis.reservation(rid) for rid in ours]
        run.check(
            len(hold.admitted) == n
            and len(ours) == n
            and all(r and r["model_name"] == model for r in rows),
            f"Redis holds {n} new reservations",
            f"{len(ours)} new members of the in-flight set, each with a reservation",
        )

        async def counted() -> dict[str, int] | None:
            total = await gw.total_inflight(model)
            deps = await gw.deployment_inflight(model)
            ok = total == base + n and sum(deps.values()) == sum(base_deps.values()) + n
            return deps if ok else None

        deps = await wait_for(f"the catalog to count {n} more", counted, timeout=15)
        run.check(
            all(v <= per_dep.get(k, 0) for k, v in deps.items()),
            f"The catalog counts {n} more, and no deployment is over its capacity",
            f"per deployment {deps}, capacity {per_dep}",
        )

    run.step("Closed the streams (client disconnect)")

    async def released() -> bool:
        live = await redis.model_inflight(model)
        return not (live & ours) and await gw.total_inflight(model) <= base

    await wait_for("in-flight back to baseline", released, timeout=10, interval=1)
    gone = [
        await redis.reservation(rid) is None and await redis.deadline(rid) is None
        for rid in ours
    ]
    run.check(
        all(gone),
        "The disconnects released every reservation (not left to the sweeper)",
        f"{_pct(gone)} reservation blobs and deadlines removed within 10s",
    )
    run.evidence(
        f"{n} streams counted in Redis and catalog; all released on disconnect"
    )


async def test_lease_renewal_keeps_long_request_counted(
    gw: Gateway, run: Run, redis: RedisState, chaos: Chaos, me: UserAuthEvent
) -> None:
    """A request that runs longer than the 30s reservation lease keeps its
    reservation, because the apiserver keeps renewing it.  Otherwise the
    sweeper would release a slot that is still in use."""
    await chaos.reset_all(redis, me.id)
    for b in chaos.backends:
        await chaos.set(b, "slow", interval=1.0)
    before = await redis.model_inflight(CHAOS_MODEL)
    async with gw.hold(CHAOS_MODEL, 1, max_tokens=64, tag="lease") as hold:
        run.step("Opened one slow, never-ending stream on the chaos model")
        run.check(bool(hold.admitted), "It was admitted")
        (rid,) = await redis.model_inflight(CHAOS_MODEL) - before
        deadlines, lapsed = [], []
        t0 = time.monotonic()
        run.step("Read its lease deadline from Redis every 5s for 45s")
        while time.monotonic() - t0 < 45:
            await asyncio.sleep(5)
            run.check(bool(hold.live), "The stream is still running", soft=True)
            deadline, now = await redis.deadline(rid), await redis.now()
            if deadline is None or deadline <= now:
                lapsed.append(round(time.monotonic() - t0))
            else:
                deadlines.append(deadline)
        moved = [round(d - deadlines[0]) for d in deadlines] if deadlines else []
        run.check(
            not lapsed,
            "The lease never lapsed",
            f"deadline always in the future (lapsed at {lapsed}s)"
            if lapsed
            else "deadline was in the future at every sample",
        )
        run.check(
            bool(moved) and moved[-1] >= 30,
            "The deadline kept moving forward (renewed)",
            f"deadline relative to the first sample: {moved}s",
        )
        run.check(
            await redis.reservation(rid) is not None,
            "The reservation is still there after 45s",
        )
    await chaos.reset()
    run.evidence(
        f"lease renewed {len(deadlines)} times over 45s; deadline moved {moved[-1]}s"
    )


async def test_upstream_4xx_releases_slot_without_benching(
    gw: Gateway, cfg: LiveConfig, run: Run, redis: RedisState
) -> None:
    """A 4xx from the model server (the user's fault) is passed back to the
    user, frees the request's slot, and does not count against the backend:
    more 4xx than the cooldown threshold must not bench anything."""
    model = cfg.fleet_model
    await require_model(gw, model)
    m = await gw.model_config(model)
    threshold = max(d["router_params"]["cooldown_threshold"] for d in m["deployments"])
    ids = [b for bs in (await gw.backend_ids(model)).values() for b in bs]
    errors_before = {b: (await redis.errors(b))[0] for b in ids}
    base = await redis.model_inflight(model)

    run.step(f"Sent {threshold + 1} invalid requests (temperature=-1) to {model}")
    bad = [
        await gw.chat(model, max_tokens=4, extra={"temperature": -1}, tag="4xx")
        for _ in range(threshold + 1)
    ]
    run.check(
        all(400 <= r.status < 500 and r.deployment for r in bad),
        "Each was relayed as the model server's 4xx (naming the deployment)",
        "; ".join(sorted({f"{r.summary()[:100]} via {r.deployment}" for r in bad})),
    )
    good = await gw.chat(model, max_tokens=4, tag="after-4xx")
    run.check(good.ok, "A valid request right after succeeds", good.summary())
    left = await redis.model_inflight(model)
    run.check(
        left <= base,
        "No slot was left reserved",
        f"{len(left)} in flight (baseline {len(base)})",
    )
    after = {b: (await redis.errors(b))[0] for b in ids}
    run.check(
        after == errors_before,
        "No backend's error counter moved",
        f"error counters before {errors_before}, after {after}",
    )
    run.evidence(
        f"{threshold + 1} upstream 4xx relayed; no error counted, nothing benched"
    )


async def test_backend_saturation_503(gw: Gateway, cfg: LiveConfig, run: Run) -> None:
    """When every backend slot is in use, further requests are refused with
    503 "Backend saturated" and the model's short Retry-After, and counted as
    capacity rejects (which the autoscaler treats as demand)."""
    model = cfg.fleet_model
    await require_model(gw, model)
    cap, per_dep = await gw.routable_capacity(model)
    lim = await gw.effective_limits(model)
    base = await gw.total_inflight(model)
    free = cap - base
    extra = 3
    if lim.max_user_concurrency < free + extra:
        pytest.skip(
            f"max_user_concurrency={lim.max_user_concurrency} < {free + extra}: "
            "quota would bind before capacity"
        )
    if free > 40:
        pytest.skip(f"{free} free slots: lower max_backend_concurrency to test")
    retry_lo, retry_hi = _short_retry_window(await _overload(gw, model))
    rt0 = await gw.model_runtime(model)
    t_start = datetime.now(timezone.utc)

    async with gw.hold(model, free, tag="fill") as hold:
        run.step(f"Opened {free} streams to fill every free slot (capacity {cap})")
        run.check(len(hold.admitted) == free, f"All {free} were admitted")
        over = await hold.open(extra)
        run.step(f"Sent {extra} more")
        run.check(
            all(
                s.status == 503
                and "Backend saturated" in s.message
                and retry_lo <= int(s.headers.get("retry-after", -1)) <= retry_hi
                for s in over
            ),
            f'All {extra} were refused with 503 "Backend saturated" and the short '
            "Retry-After",
            "; ".join(
                f"{s.status} {s.message!r} Retry-After {s.headers.get('retry-after')}"
                for s in over
            )
            + f" (allowed {retry_lo}-{retry_hi})",
        )
        deps = await gw.deployment_inflight(model)
        run.check(
            all(deps.get(k, 0) == v for k, v in per_dep.items()),
            "Every deployment was full",
            f"in flight {deps}, capacity {per_dep}",
        )

    rt1 = await gw.model_runtime(model)
    last = datetime.fromisoformat(rt1["last_capacity_reject"])
    run.check(
        rt1["capacity_rejects_total"] >= rt0["capacity_rejects_total"] + extra
        and last >= t_start.replace(microsecond=0),
        "The rejects were counted as capacity rejects",
        f"capacity_rejects_total {rt0['capacity_rejects_total']} -> "
        f"{rt1['capacity_rejects_total']}, last at {last:%H:%M:%S}",
    )
    run.evidence(
        f"{free} slots filled; {extra} extra requests got 503 Backend saturated"
    )
