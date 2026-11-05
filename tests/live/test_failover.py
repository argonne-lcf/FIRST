"""
Upstream failures on the chaos model: failover before the response starts, and
a stream that breaks after it has started.

The chaos deployments are weighted a=1000, b=1, c=1000, d=1000, so "b" (the
healthy fallback) is drawn after every other routable backend almost always.
Backends a test doesn't use are benched in Redis, which removes them from
admission without touching their weights.
"""

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from first_common.schema.auth import UserAuthEvent

from .backend import CHAOS_MODEL, Compose, RedisState, traces
from .conftest import CHAOS_LIMITS, CHAOS_ROUTER, LIVE, Chaos
from .harness import Gateway, Reply, Run, wait_for

pytestmark = LIVE

RPM_INTERVAL = 60 / CHAOS_LIMITS["rpm"]  # seconds of bucket per request
TOKENS_PER_SEC = CHAOS_LIMITS["tpm"] / 60
MAX_TOKENS = 2000  # reserved per attempt (output EWMA cleared)
USAGE = {"prompt_tokens": 10, "completion_tokens": 5}  # reported by "ok"


def _since() -> datetime:
    return datetime.now(timezone.utc) - timedelta(seconds=2)


async def _debts(redis: RedisState, user: str) -> tuple[float, float]:
    """(RPM debt in requests, TPM debt in tokens) left in the chaos buckets."""
    now = await redis.now()
    rpm = await redis.quota_tat(CHAOS_MODEL, user, "rpm")
    tpm = await redis.quota_tat(CHAOS_MODEL, user, "tokens")
    return (
        max(0.0, (rpm or now) - now) / RPM_INTERVAL,
        max(0.0, (tpm or now) - now) * TOKENS_PER_SEC,
    )


async def _send(
    run: Run,
    gw: Gateway,
    chaos: Chaos,
    redis: RedisState,
    me: UserAuthEvent,
    first: set[str],
    tag: str,
    **kwargs: Any,
) -> tuple[Reply, dict[str, int]]:
    """Send one request from empty buckets with a known reservation; retry
    (rarely needed) until the weighted draw tried the ``first`` backends
    before "b".  Returns the reply and the per-backend hit counts."""
    for _ in range(5):
        await redis.unbench(*(chaos.ids[b] for b in first))
        await redis.reset_quota(CHAOS_MODEL, me.id)
        await redis.set_token_stats(
            CHAOS_MODEL, me.id, chars_per_token=4.0, output_tokens=None
        )
        before = await chaos.hits()
        reply = await gw.chat(CHAOS_MODEL, max_tokens=MAX_TOKENS, tag=tag, **kwargs)
        after = await chaos.hits()
        hits = {b: after[b] - before[b] for b in chaos.backends}
        if all(hits[b] == 1 for b in first):
            return reply, hits
        run.note(f"Weighted draw put b before {sorted(first)} ({hits}); sending again")
    pytest.fail(f"weighted draw never tried {first} first: {hits}")


@pytest.mark.parametrize(
    "failure", ["status:502", "status:503", "status:504", "disconnect"]
)
async def test_failover_to_next_backend(
    failure: str,
    gw: Gateway,
    run: Run,
    compose: Compose,
    redis: RedisState,
    chaos: Chaos,
    me: UserAuthEvent,
) -> None:
    """When a backend fails with a retryable error, the gateway retries the
    request on the next backend: the user gets a normal answer and is charged
    one request of RPM, the failed attempt's token reservation is refunded and
    counted against the failing backend, and the logs show both attempts.

    Backend "a" fails, "b" is healthy, "c" and "d" are benched out of the way.
    """
    mode, _, status = failure.partition(":")
    what = f"HTTP {status}" if status else "closing the connection without a reply"
    await chaos.reset_all(redis, me.id)
    await chaos.set("a", mode, **({"status": int(status)} if status else {}))
    await chaos.set("b", "ok", **USAGE)
    for b in ("c", "d"):
        await redis.bench(chaos.ids[b], CHAOS_ROUTER["cooldown_threshold"], 300)
    run.step(
        f"Made chaos backend a fail every request by {what}; b answers normally "
        f"reporting {sum(USAGE.values())} tokens; benched c and d in Redis"
    )
    since = _since()
    try:
        run.step(
            f"Emptied the RPM/TPM buckets, then sent one chat request reserving "
            f"~{MAX_TOKENS} tokens"
        )
        reply, hits = await _send(
            run, gw, chaos, redis, me, {"a"}, f"failover-{failure}"
        )
        rpm_debt, tpm_debt = await _debts(redis, me.id)
        errors_a = await redis.errors(chaos.ids["a"])
    finally:
        await chaos.reset_all(redis, me.id)

    actual = sum(USAGE.values())
    run.check(reply.ok, "The user got a successful answer", reply.summary())
    run.check(
        reply.deployment == chaos.deployment("b"),
        "It was served by the fallback backend b",
        f"X-First-Deployment: {reply.deployment}",
    )
    run.check(
        hits == {"a": 1, "b": 1, "c": 0, "d": 0},
        "a was tried once, then b once; benched c and d were never contacted",
        f"requests seen by the chaos upstream: {hits}",
    )
    run.check(
        0.8 <= rpm_debt <= 1.2,
        "The user was charged exactly one request of RPM",
        f"RPM bucket holds {rpm_debt:.2f} requests (expected 1; a double charge "
        "would show 2)",
    )
    run.check(
        tpm_debt <= actual + 1,
        "The failed attempt's token reservation was refunded",
        f"TPM bucket holds {tpm_debt:.0f} tokens; b used {actual}, each attempt "
        f"reserved ~{MAX_TOKENS}",
    )
    run.check(
        errors_a[0] == 1,
        "The failure counts toward backend a's cooldown",
        f"a's error counter is {errors_a[0]} (TTL {errors_a[1]}s)",
    )
    left = await redis.model_inflight(CHAOS_MODEL)
    run.check(
        not left, "No reservation was left behind", f"in-flight set: {left or 'empty'}"
    )

    tr = (await traces(compose, since, [reply.marker], min_inference=2))[reply.marker]
    attempts = "; ".join(
        f"attempt {r['attempt']} {r['outcome']} on {r['deployment']} "
        f"(error={r['error']}, tokens={r['total_tokens']})"
        for r in tr.inference
    )
    run.check(
        tr.outcomes == ["upstream_error", "success"],
        "The logs show attempt 1 upstream_error, then attempt 2 success",
        f"InferenceLogs for request {tr.request_id}: {attempts}",
    )
    failed, ok = tr.inference
    want_error = f"HTTP {status}" if status else "RemoteProtocolError"
    run.check(
        [failed["attempt"], ok["attempt"]] == [1, 2]
        and failed["deployment"] == chaos.deployment("a")
        and failed["backend_id"] == chaos.ids["a"]
        and failed["error"] == want_error
        and failed["total_tokens"] is None,
        f"Attempt 1 is logged against backend a with error {want_error!r} and no tokens",
        f"backend_id={failed['backend_id']}, error={failed['error']!r}",
    )
    run.check(
        ok["deployment"] == chaos.deployment("b") and ok["total_tokens"] == actual,
        f"Attempt 2 is logged against b with its {actual} tokens",
        f"deployment={ok['deployment']}, total_tokens={ok['total_tokens']}",
    )
    run.check(
        bool(tr.response) and tr.response["status_code"] == 200,  # type: ignore[index]
        "The response log records status 200",
    )
    run.evidence(
        f"{what} on a -> answered by b; RPM charged {rpm_debt:.2f}, "
        f"TPM left {tpm_debt:.0f}/{actual} tokens, logs {tr.outcomes}"
    )


async def test_failover_gives_up_after_three_attempts(
    gw: Gateway,
    run: Run,
    compose: Compose,
    redis: RedisState,
    chaos: Chaos,
    me: UserAuthEvent,
) -> None:
    """The gateway makes at most three attempts.  With three failing backends
    drawn first, it returns a 503 naming every failure and never tries the
    healthy fourth, and the user is charged one request and no tokens."""
    await chaos.reset_all(redis, me.id)
    for b in ("a", "c", "d"):
        await chaos.set(b, "status", status=503)
    run.step("Made chaos backends a, c and d return HTTP 503; b stays healthy")
    since = _since()
    try:
        run.step("Emptied the buckets and sent one chat request")
        reply, hits = await _send(run, gw, chaos, redis, me, {"a", "c", "d"}, "give-up")
        rpm_debt, tpm_debt = await _debts(redis, me.id)
    finally:
        await chaos.reset_all(redis, me.id)

    want = (
        f"All 3 backend(s) failed for model {CHAOS_MODEL}. "
        "Encountered: HTTP 503, HTTP 503, HTTP 503."
    )
    run.check(
        reply.status == 503 and want in reply.message,
        "The user got a 503 naming all three failures",
        reply.summary(),
    )
    run.check(
        hits == {"a": 1, "b": 0, "c": 1, "d": 1},
        "Exactly three backends were tried; the healthy fourth (b) never was",
        f"requests seen by the chaos upstream: {hits}",
    )
    run.check(
        0.8 <= rpm_debt <= 1.2,
        "The user was charged one request of RPM, not three",
        f"RPM bucket holds {rpm_debt:.2f} requests",
    )
    run.check(
        tpm_debt < 1,
        "No tokens stayed charged",
        f"TPM bucket holds {tpm_debt:.1f} tokens after three ~{MAX_TOKENS}-token "
        "reservations",
    )
    left = await redis.model_inflight(CHAOS_MODEL)
    run.check(
        not left, "No reservation was left behind", f"in-flight set: {left or 'empty'}"
    )

    tr = (await traces(compose, since, [reply.marker], min_inference=3))[reply.marker]
    run.check(
        tr.outcomes == ["upstream_error"] * 3
        and [r["attempt"] for r in tr.inference] == [1, 2, 3],
        "The logs show attempts 1, 2 and 3, all upstream_error",
        f"{[(r['attempt'], r['outcome'], r['deployment']) for r in tr.inference]}",
    )
    run.evidence(f"3 attempts (a, c, d) then 503; b untouched; RPM {rpm_debt:.2f}")


async def test_stream_broken_midway(
    gw: Gateway,
    run: Run,
    compose: Compose,
    redis: RedisState,
    chaos: Chaos,
    me: UserAuthEvent,
) -> None:
    """If a backend dies after it has started streaming a 200 response, it is
    too late to fail over.  The user's stream must visibly end early (no
    [DONE]), the slot must be released at once rather than by the sweeper,
    and the failure must be logged as an upstream error and counted toward
    the backend's cooldown.  Since no usage was reported, the token estimate
    stays charged."""
    await chaos.reset_all(redis, me.id)
    await chaos.set("a", "break", chunks=5, interval=0.2)
    await redis.set_token_stats(
        CHAOS_MODEL, me.id, chars_per_token=4.0, output_tokens=None
    )
    run.step(
        "Made chaos backend a answer 200, stream 5 chunks 0.2s apart, then reset "
        "the connection"
    )
    since = _since()
    try:
        run.step("Sent one streaming request pinned to backend a")
        reply = await gw.chat(
            CHAOS_MODEL,
            max_tokens=MAX_TOKENS,
            stream=True,
            deployment=chaos.deployment("a"),
            tag="broken-stream",
        )
        hits = await chaos.hits()

        async def released() -> bool:
            return not await redis.model_inflight(CHAOS_MODEL)

        await wait_for(
            "the reservation to be released", released, timeout=3, interval=0.5
        )
        _, tpm_debt = await _debts(redis, me.id)
        errors_a = await redis.errors(chaos.ids["a"])
    finally:
        await chaos.reset_all(redis, me.id)

    chunks = reply.text.count('"content"')
    run.check(
        reply.status == 200, "The stream started normally", f"status {reply.status}"
    )
    run.check(
        chunks == 5,
        "The user received the 5 chunks sent before the break",
        f"{chunks} chunks",
    )
    run.check(
        "[DONE]" not in reply.text,
        "The user's stream visibly ended early (no [DONE])",
        f"client saw {reply.stream_error or 'the connection close'} after {chunks} chunks",
    )
    run.check(
        hits["a"] == 1,
        "The request was not retried",
        f"backend a saw {hits['a']} request(s)",
    )
    run.note(
        "The reservation was released by the request itself within 3s "
        "(the sweeper would take 30s or more)"
    )
    run.check(
        errors_a[0] == 1,
        "The failure counts toward backend a's cooldown",
        f"a's error counter is {errors_a[0]}",
    )

    tr = (await traces(compose, since, [reply.marker]))[reply.marker]
    row = tr.inference[0] if len(tr.inference) == 1 else None
    run.check(
        row is not None,
        "Exactly one attempt was logged",
        f"{len(tr.inference)} InferenceLogs",
    )
    assert row is not None
    run.check(
        row["outcome"] == "upstream_error",
        "It is logged as upstream_error, not success",
        f"outcome={row['outcome']!r}, error={row['error']!r}",
    )
    run.check(
        row["error"] in ("RemoteProtocolError", "ReadError")
        and row["deployment"] == chaos.deployment("a")
        and row["total_tokens"] is None,
        "The log names the transport error and backend a, with no token usage",
        f"error={row['error']!r}, deployment={row['deployment']}, "
        f"total_tokens={row['total_tokens']}",
    )
    run.check(
        bool(tr.response) and tr.response["status_code"] == 200,  # type: ignore[index]
        "The response log records the 200 that was already sent",
    )
    run.check(
        tpm_debt >= row["estimated_tokens"] - 2 * TOKENS_PER_SEC,
        "The token estimate stays charged (usage unknown)",
        f"TPM bucket holds {tpm_debt:.0f} tokens; the estimate was "
        f"{row['estimated_tokens']}",
    )
    run.evidence(
        f"cut after {chunks} chunks, no retry, logged {row['outcome']} "
        f"({row['error']}), estimate {row['estimated_tokens']} kept"
    )
