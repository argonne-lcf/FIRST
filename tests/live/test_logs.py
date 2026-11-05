"""
Structured logs + large-body storage + token-estimation EWMA.

1. Send a tagged mix of requests: unary/stream, chat/responses/messages, an
   upstream 4xx, a quota 429, a request over burst_tokens (400, no
   Retry-After), a large request and a large response, then an EWMA sequence
   (short outputs, then long outputs, same prompt).
2. Read the apiserver's JSON logs for the run window (docker compose logs).
3. Verify every request's RequestLog / ResponseLog / InferenceLog against what
   the client observed (including the X-First-Deployment header naming the
   logged deployment, and an ``admission_rejected`` InferenceLog for every
   admission rejection), and the EWMA calibration fields against their
   recurrence and the estimate formula.
4. Check the large-request / large-response files inside the apiserver
   container.
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from first_common.schema.auth import UserAuthEvent
from first_common.schema.endpoints.token_estimation import (
    CHARS_PER_TOKEN,
    MAX_OUTPUT_ESTIMATE,
)
from first_common.schema.structured_logs import INLINE_BODY_LIMIT
from first_gateway.database.redis.admission import USAGE_EWMA_ALPHA

from .backend import Compose, RedisState
from .conftest import LIVE, require_model
from .harness import Gateway, LiveConfig, Reply, Run, filler_text

pytestmark = LIVE

# Skip the over-burst request rather than POST a body this large.
MAX_OVERSIZE_CHARS = 2_000_000


@dataclass
class Sent:
    label: str
    reply: Reply
    prompt_chars: int  # chars the estimator counts (message text)
    max_tokens: int
    expect_inference: str  # expected InferenceLog outcome
    phase: str = ""
    # For admission_rejected: the expected InferenceLog.error reason code.
    expect_reason: str | None = None


async def _send_chat(
    gw: Gateway,
    model: str,
    label: str,
    prompt: str,
    max_tokens: int,
    *,
    stream: bool = False,
    extra: dict[str, Any] | None = None,
    phase: str = "",
) -> Sent:
    reply = await gw.chat(
        model, prompt, max_tokens=max_tokens, stream=stream, extra=extra, tag=label
    )
    # Admission tags its rejections with info.reject_reason; anything else
    # non-200 was relayed from the backend.
    reason = reply.info.get("reject_reason")
    outcome = (
        "success"
        if reply.ok
        else "admission_rejected"
        if reason
        else "upstream_rejected"
    )
    return Sent(
        label,
        reply,
        len(f"{reply.marker} {prompt}"),
        max_tokens,
        outcome,
        phase,
        expect_reason=reason,
    )


async def _send_mix(
    gw: Gateway, cfg: LiveConfig, redis: RedisState, me: UserAuthEvent
) -> list[Sent]:
    model = cfg.fleet_model
    ign = cfg.hold_extra(model, 1200)
    sent = [
        await _send_chat(gw, model, "small-unary", "Say OK.", 8),
        await _send_chat(gw, model, "small-stream", "Say OK.", 8, stream=True),
        await _send_chat(gw, model, "large-request", filler_text(6000, seed=3), 8),
        await _send_chat(gw, model, "large-response", "Count upward.", 1200, extra=ign),
        await _send_chat(
            gw, model, "upstream-4xx", "Say OK.", 8, extra={"temperature": -1}
        ),
    ]

    marker = gw.marker("responses")
    r = await gw.post(
        "responses",
        {"model": model, "input": f"{marker} Say OK.", "max_output_tokens": 16},
        marker=marker,
    )
    sent.append(Sent("responses", r, len(f"{marker} Say OK."), 16, "success"))

    marker = gw.marker("messages")
    r = await gw.post(
        "messages",
        {
            "model": model,
            "max_tokens": 16,
            "messages": [{"role": "user", "content": f"{marker} Say OK."}],
        },
        marker=marker,
    )
    sent.append(Sent("messages", r, len(f"{marker} Say OK."), 16, "success"))

    # A quota rejection on the RPM model (if it has a finite bucket).
    lim = await gw.effective_limits(cfg.rpm_model)
    if lim.rpm > 0:
        await redis.reset_quota(cfg.rpm_model, me.id)
        for _ in range(lim.burst_requests + 1):
            s = await _send_chat(gw, cfg.rpm_model, "quota-429", "Say OK.", 1)
            if s.reply.status == 429:
                sent.append(s)
                break

    # A request whose estimate exceeds the TPM bucket depth fails fast (400, no
    # Retry-After).  Sized at 8 chars per burst token, so it's oversize for any
    # plausible learned chars-per-token.
    lim = await gw.effective_limits(cfg.tpm_model)
    n_chars = lim.burst_tokens * 8
    if lim.tpm > 0 and n_chars <= MAX_OVERSIZE_CHARS:
        sent.append(
            await _send_chat(
                gw, cfg.tpm_model, "over-burst", filler_text(n_chars, seed=5), 8
            )
        )

    # EWMA sequence: same prompt, short outputs then long outputs, starting
    # from no learned state.  Unary, so each update lands before the next
    # request is estimated.
    await redis.set_token_stats(model, me.id, chars_per_token=None, output_tokens=None)
    prompt = filler_text(1200, seed=4)
    for phase, max_tokens in (("short", 32), ("long", 400)):
        for i in range(4):
            sent.append(
                await _send_chat(
                    gw,
                    model,
                    f"ewma-{phase}-{i}",
                    prompt,
                    max_tokens,
                    extra=ign,
                    phase=phase,
                )
            )
    return sent


def _expected_estimate(row: dict[str, Any], s: Sent, max_model_len: int) -> int:
    """Re-derive admission's estimate from the logged calibration inputs
    (chat/completions without tools: token_estimation.estimate_total_tokens)."""
    cpt = row["est_chars_per_token"] or CHARS_PER_TOKEN
    input_tokens = max(1, int(s.prompt_chars / cpt))
    out = row["est_output_tokens"]
    output = round(out) if out is not None else s.max_tokens
    output = min(output, s.max_tokens, MAX_OUTPUT_ESTIMATE)
    output = min(output, max(0, max_model_len - input_tokens))
    return input_tokens + max(0, output)


async def test_structured_logs_and_large_bodies(
    gw: Gateway,
    cfg: LiveConfig,
    run: Run,
    compose: Compose,
    redis: RedisState,
    me: UserAuthEvent,
) -> None:
    """Every request leaves an accurate trail in the structured logs: a
    RequestLog, a ResponseLog and InferenceLog(s) that agree with what the
    client actually saw (user, path, status, tokens, deployment, errors).
    Large bodies are written to disk instead of inline, and the
    token-estimate EWMAs update exactly as specified."""
    model = cfg.fleet_model
    await require_model(gw, model)
    max_model_len = int((await gw.model(model))["max_model_len"])
    since = datetime.now(timezone.utc) - timedelta(seconds=2)
    sent = await _send_mix(gw, cfg, redis, me)
    run.step(
        f"Sent {len(sent)} tagged requests: unary and streaming chat, a large "
        "request, a large response, responses and messages endpoints, an invalid "
        "request (upstream 4xx), an RPM 429, an over-burst 400, and 8 requests "
        "for the EWMA sequence"
    )
    for s in sent:
        run.note(f"{s.label}: {s.reply.summary()[:140]}")
    bad = [s.label for s in sent if s.expect_inference == "success" and not s.reply.ok]
    run.check(not bad, "Every request meant to succeed did", f"failed: {bad or 'none'}")
    for label, status, reason in (
        ("quota-429", 429, "quota:"),
        ("over-burst", 400, "request_too_large"),
    ):
        for s in (s for s in sent if s.label == label):
            run.check(
                s.reply.status == status and (s.expect_reason or "").startswith(reason),
                f"{label} was refused as intended",
                f"{s.reply.summary()[:120]}; reject_reason {s.expect_reason!r}",
            )

    await asyncio.sleep(2)
    rows = await compose.logs(cfg.apiserver_service, since)
    requests = [r for r in rows if r.get("stream") == "request"]
    responses = {r["request_id"]: r for r in rows if r.get("stream") == "response"}
    inferences: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        if r.get("stream") == "inference":
            inferences.setdefault(r["request_id"], []).append(r)
    run.step(
        f"Read the apiserver's logs (docker compose logs): {len(rows)} JSON rows, "
        f"{len(requests)} RequestLogs, {len(responses)} ResponseLogs, "
        f"{sum(map(len, inferences.values()))} InferenceLogs"
    )
    ids = [r["event_id"] for r in rows if "event_id" in r]
    run.check(
        len(ids) == len(set(ids)),
        "Every log event has a unique event_id",
        f"{len(ids)} ids, {len(set(ids))} distinct",
    )

    # ---- correlate each sent request with its RequestLog --------------------
    rid_of: dict[str, str] = {}
    for s in sent:
        hits = [
            r
            for r in requests
            if (r.get("body") and s.reply.marker in r["body"])
            or (
                r.get("body_stored")
                and r.get("content_length") == s.reply.request_bytes
                and r["path"].endswith(s.reply.endpoint)
                and r["user_id"] == me.id
            )
        ]
        run.check(
            len(hits) == 1,
            f"{s.label}: its RequestLog was found",
            f"{len(hits)} RequestLogs carry its marker",
        )
        rid_of[s.label] = hits[0]["request_id"]

    # Field-by-field checks are grouped into one recorded check per request
    # (or per topic): its evidence is the key logged values, or every mismatch.
    problems: list[str] = []
    all_problems: list[str] = []
    pending: list[tuple[str, str]] = []

    def check(cond: bool, what: str) -> None:
        if not cond:
            problems.append(what)

    def topic(claim: str, evidence: str) -> None:
        flush()
        pending.append((claim, evidence))

    def flush() -> None:
        if pending:
            claim, evidence = pending.pop()
            run.check(not problems, claim, "; ".join(problems) or evidence, soft=True)
            all_problems.extend(problems)
            problems.clear()

    stored: list[tuple[str, str, str | None]] = []  # (label, path, marker)
    for s in sent:
        rid = rid_of[s.label]
        infs_ = inferences.get(rid, [])
        topic(
            f"{s.label}: the logs match what the client saw",
            f"request {rid}: client got {s.reply.status}; logged status "
            f"{(responses.get(rid) or {}).get('status_code')}, InferenceLog "
            + ", ".join(
                f"{i['outcome']} on {i.get('deployment')} tokens "
                f"{i.get('input_tokens')}/{i.get('output_tokens')} "
                f"(estimated {i.get('estimated_tokens')})"
                + (f" error {i['error']!r}" if i.get("error") else "")
                for i in infs_
            ),
        )
        req = next(r for r in requests if r["request_id"] == rid)
        tag = f"{s.label} ({rid})"
        check(req["user_id"] == me.id, f"{tag}: RequestLog.user_id")
        check(req["username"] == me.username, f"{tag}: RequestLog.username")
        check(req["method"] == "POST", f"{tag}: method")
        check(req["path"].endswith("/" + s.reply.endpoint), f"{tag}: path")
        check(req["content_length"] == s.reply.request_bytes, f"{tag}: length")
        large_req = s.reply.request_bytes >= INLINE_BODY_LIMIT
        check(req["body_stored"] == large_req, f"{tag}: request body_stored")
        check((req["body"] is None) == large_req, f"{tag}: request body inline")
        if large_req:
            stored.append((s.label, _body_path(req, rid), s.reply.marker))

        resp = responses.get(rid)
        check(resp is not None, f"{tag}: ResponseLog missing")
        if resp:
            check(resp["status_code"] == s.reply.status, f"{tag}: status_code")
            check(resp["streaming"] == s.reply.stream, f"{tag}: streaming flag")
            check(
                0 <= resp["duration_ms"] <= s.reply.elapsed * 1000 + 1000,
                f"{tag}: duration_ms {resp['duration_ms']} vs {s.reply.elapsed:.2f}s",
            )

        infs = inferences.get(rid, [])
        final = [i for i in infs if i["outcome"] == s.expect_inference]
        check(len(final) == 1, f"{tag}: {len(final)} {s.expect_inference} logs")
        if not final:
            continue
        inf = final[0]
        check(inf["user_id"] == me.id, f"{tag}: InferenceLog.user_id")
        check(inf["model"] == s.reply.model, f"{tag}: model")
        check(inf["endpoint"] == s.reply.endpoint, f"{tag}: endpoint")
        check(inf["latency_sec"] <= s.reply.elapsed + 1, f"{tag}: latency")
        check(inf["estimated_tokens"] is not None, f"{tag}: estimated_tokens")
        # Text-only prompts without tools: the estimator counts exactly the
        # prompt text on every endpoint, and no images.
        check(
            inf["input_chars"] == s.prompt_chars,
            f"{tag}: input_chars {inf['input_chars']} != {s.prompt_chars}",
        )
        check(inf["input_images"] == 0, f"{tag}: input_images {inf['input_images']}")
        header = s.reply.deployment

        if inf["outcome"] == "admission_rejected":
            # Refused before routing: no backend, and (federated) no deployment.
            for f in ("deployment", "cluster", "backend_id", "backend_model_url"):
                check(inf.get(f) is None, f"{tag}: {f} set on admission reject")
            check(header is None, f"{tag}: deployment header on admission reject")
            check(inf["attempt"] == 1, f"{tag}: attempt {inf['attempt']}")
            check(
                inf["error"] == s.expect_reason,
                f"{tag}: error {inf['error']!r} != reject_reason {s.expect_reason!r}",
            )
            check(len(infs) == 1, f"{tag}: {len(infs)} InferenceLogs, expected 1")
            if s.expect_reason == "request_too_large":
                check(s.reply.retry_after is None, f"{tag}: Retry-After on 400")
                check(
                    inf["estimated_tokens"] > s.reply.info["burst_tokens"],
                    f"{tag}: estimated_tokens within burst_tokens",
                )
            else:
                check(
                    s.reply.retry_after is not None and s.reply.retry_after >= 1,
                    f"{tag}: Retry-After {s.reply.retry_after}",
                )
            continue

        for f in ("deployment", "cluster", "backend_id", "backend_model_url"):
            check(bool(inf.get(f)), f"{tag}: {f} empty")
        # The response names the deployment the request landed on.
        check(
            header == inf["deployment"],
            f"{tag}: deployment header {header!r} != logged {inf['deployment']!r}",
        )
        if inf["outcome"] == "success":
            _check_usage(s, inf, check, tag)
            raw_large = inf["body_stored"]
            check(raw_large or inf["body"] is not None, f"{tag}: response body lost")
            if raw_large:
                stored.append((s.label, _body_path(inf, rid), None))
        else:
            check(bool(inf["error"]), f"{tag}: error summary missing")

    topic(
        "Large bodies were stored out of line",
        f"{len(stored)} bodies marked body_stored: {[x[0] for x in stored]}",
    )
    check(
        any(x[0] == "large-response" for x in stored),
        "large-response: InferenceLog.body_stored is False",
    )
    check(
        any(x[0] == "large-request" for x in stored),
        "large-request: RequestLog.body_stored is False",
    )

    # ---- EWMA calibration ----------------------------------------------------
    ewma = [
        (s, next(i for i in inferences[rid_of[s.label]] if i["outcome"] == "success"))
        for s in sent
        if s.phase
    ]
    ewma.sort(key=lambda pair: datetime.fromisoformat(pair[1]["timestamp"]))
    run.step(
        "Cleared the EWMAs, then sent the same prompt 4 times with max_tokens 32 "
        "and 4 times with max_tokens 400"
    )
    run.note(
        "EWMA sequence (est chars/token, sampled chars/token, est output, output, "
        "estimated_tokens):"
    )
    for s, row in ewma:
        sample = row["input_chars"] / row["input_tokens"]
        run.note(
            f"`{s.label}` {row['est_chars_per_token']} | {sample:.3f} | "
            f"{row['est_output_tokens']} | {row['output_tokens']} | "
            f"{row['estimated_tokens']}"
        )
    topic(
        "Each logged estimate equals the estimator formula applied to the logged "
        "EWMA inputs",
        f"{len(ewma)} rows recomputed",
    )
    for s, row in ewma:
        check(
            row["estimated_tokens"] == _expected_estimate(row, s, max_model_len),
            f"{s.label}: estimated_tokens {row['estimated_tokens']} != formula "
            f"{_expected_estimate(row, s, max_model_len)}",
        )
    first = ewma[0][1]
    topic(
        f"Each EWMA step follows alpha={USAGE_EWMA_ALPHA}: new = alpha x sample + "
        "(1 - alpha) x previous, starting from the first sample",
        f"{len(ewma) - 1} transitions checked from a cleared start",
    )
    check(
        first["est_output_tokens"] is None and first["est_chars_per_token"] is None,
        "EWMA sequence did not start from a cleared state",
    )
    a = USAGE_EWMA_ALPHA
    for (s0, r0), (s1, r1) in zip(ewma, ewma[1:]):
        out0 = float(r0["output_tokens"])
        want_out = (
            out0
            if r0["est_output_tokens"] is None
            else (a * out0 + (1 - a) * r0["est_output_tokens"])
        )
        check(
            abs(r1["est_output_tokens"] - want_out) < 1e-6 * max(1, want_out),
            f"{s1.label}: est_output_tokens {r1['est_output_tokens']} != {want_out}",
        )
        if r0["cache_read_tokens"] or r0["cache_write_tokens"]:
            # Cache activity: no chars-per-token sample, EWMA must not move.
            check(
                r1["est_chars_per_token"] == r0["est_chars_per_token"],
                f"{s1.label}: est_chars_per_token moved after a cached request",
            )
        else:
            sample = r0["input_chars"] / r0["input_tokens"]
            want_cpt = (
                sample
                if r0["est_chars_per_token"] is None
                else (a * sample + (1 - a) * r0["est_chars_per_token"])
            )
            check(
                abs(r1["est_chars_per_token"] - want_cpt) < 1e-6,
                f"{s1.label}: est_chars_per_token {r1['est_chars_per_token']} "
                f"!= {want_cpt}",
            )
    if all(r["cache_read_tokens"] for _, r in ewma):
        run.note(
            "NOTE: every EWMA request reported cache_read_tokens, so the "
            "chars-per-token EWMA is never learned for this model (prefix "
            "caching on the chat template preamble)."
        )
    long_rows = [r for s, r in ewma if s.phase == "long"]
    outs = [r["est_output_tokens"] for r in long_rows]
    topic(
        "With longer outputs, the learned output and the reservation both grow",
        f"est_output_tokens {[round(x or 0) for x in outs]}, estimated_tokens "
        f"{[r['estimated_tokens'] for r in long_rows]}",
    )
    check(
        all(x < y for x, y in zip(outs, outs[1:])),
        f"est_output_tokens should climb toward 400 in the long phase: {outs}",
    )
    reserved = [r["estimated_tokens"] for r in long_rows]
    check(
        all(x < y for x, y in zip(reserved, reserved[1:])),
        f"TPM reservation should grow with the learned output: {reserved}",
    )

    # ---- large bodies on disk --------------------------------------------------
    if stored:
        listing = await compose.run(
            "exec", "-T", cfg.apiserver_service, "sh", "-s", stdin=_body_check(stored)
        )
        run.step("Listed the stored body files inside the apiserver container")
        topic(
            "Every stored body exists on disk, is over the inline limit, and (for "
            "requests) contains the request's marker",
            " ; ".join(ln for ln in listing.splitlines() if ln.strip()),
        )
        for label, path, marker in stored:
            line = next((ln for ln in listing.splitlines() if path in ln), "")
            parts = line.split()
            if not parts or parts[0] != "OK":
                problems.append(f"{label}: {path} missing on disk ({line!r})")
                continue
            size, n_marker = int(parts[2]), int(parts[3])
            check(size >= INLINE_BODY_LIMIT, f"{label}: {path} only {size} bytes")
            if marker:
                check(n_marker >= 1, f"{label}: {path} lacks the request marker")

    flush()
    run.check(
        not all_problems,
        "All log checks passed",
        f"{len(all_problems)} problems (listed above)" if all_problems else "",
    )
    run.evidence(
        f"{len(sent)} requests traced through Request/Response/InferenceLogs; "
        f"{len(stored)} large bodies on disk; EWMA recurrence exact"
    )


def _check_usage(s: Sent, inf: dict[str, Any], check: Any, tag: str) -> None:
    usage = s.reply.usage or {}
    if s.reply.endpoint in ("messages", "responses"):
        want_in, want_out = usage.get("input_tokens"), usage.get("output_tokens")
    else:
        want_in = usage.get("prompt_tokens")
        want_out = usage.get("completion_tokens")
    got_in, got_out = inf["input_tokens"], inf["output_tokens"]
    check(got_in == want_in, f"{tag}: input_tokens {got_in} != {want_in}")
    check(got_out == want_out, f"{tag}: output_tokens {got_out} != {want_out}")


def _body_path(row: dict[str, Any], rid: str) -> str:
    ts = datetime.fromisoformat(row["timestamp"])
    sub = "large-requests" if row["stream"] == "request" else "large-responses"
    return f"{sub}/{ts:%Y/%m/%d}/{rid}.json"


def _body_check(stored: list[tuple[str, str, str | None]]) -> str:
    """Shell script run in the apiserver container: one line per file,
    ``OK <path> <bytes> <marker count>`` or ``MISSING <path>``."""
    # Markers contain spaces, so entries go one per line.
    entries = "\n".join(f"{path}|{marker or '-'}" for _, path, marker in stored)
    return (
        'D="${FIRST_PROMPT_STORAGE_DIR:-prompt-records}"\n'
        "while IFS= read -r e; do\n"
        '  p="${e%%|*}"; m="${e#*|}"\n'
        '  if [ -s "$D/$p" ]; then\n'
        '    n=0; [ "$m" != "-" ] && n=$(grep -cF -- "$m" "$D/$p")\n'
        '    echo "OK $p $(wc -c < "$D/$p") $n"\n'
        '  else echo "MISSING $p"; fi\n'
        "done <<'EOF'\n"
        f"{entries}\n"
        "EOF\n"
    )
