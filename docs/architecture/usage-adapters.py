"""
usage_adapters.py — off-hot-path token usage extraction for an LLM gateway.

Supports 3 protocols x 2 pathways:

    OpenAI Chat Completions   /v1/chat/completions   streaming + unary
    OpenAI Responses API      /v1/responses          streaming + unary
    Anthropic Messages        /v1/messages           streaming + unary

Design constraints:
  * The relay hot path stays byte-for-byte passthrough. The tap performs, per
    chunk: one bounded memcpy (bytearray +=), delimiter scans (C-level
    bytes.find), and a per-event substring containment check. NO JSON parsing,
    NO dict construction, NO awaits on the hot path.
  * JSON parsing, usage normalization, quota settlement, and structured
    logging all run in a detached asyncio task scheduled AFTER the upstream
    connection is closed. Payloads > 128 KiB are parsed in the default
    executor so a giant `response.completed` event can't stall the loop.
  * A report is emitted for EVERY request outcome — success, upstream error,
    read timeout, client abort — so the admission controller can always
    settle (or release) its tokens/minute reservation. `usage=None` means
    "upstream never told us"; the settlement policy for that case belongs to
    the sink, not the adapter.

Where usage lives, per protocol (this drives the whole design):

  Chat Completions  stream: final SSE data event before `data: [DONE]`,
                    ONLY if the request set stream_options.include_usage
                    (adapter.prepare_request injects it — see note below).
                    Intermediate chunks then carry `"usage": null`.
                    unary:  top-level `usage` {prompt_tokens,
                    completion_tokens, prompt_tokens_details.cached_tokens,
                    completion_tokens_details.reasoning_tokens}.

  Responses API     stream: terminal event (response.completed /
                    .incomplete / .failed) whose data embeds the FULL
                    response object, usage included. This event can be tens
                    of KB — the reason the tap frames SSE events instead of
                    keeping a fixed-size tail buffer (a tail buffer can trim
                    off the usage field of a large terminal event).
                    unary:  top-level `usage` {input_tokens, output_tokens,
                    input_tokens_details.cached_tokens,
                    output_tokens_details.reasoning_tokens}.

  Anthropic         stream: input_tokens (+ cache_read/cache_creation) arrive
                    in the FIRST event (message_start); cumulative
                    output_tokens arrive in the LAST usage-bearing event
                    (message_delta). Hence the tap keeps first + last
                    usage-bearing events. Useful property: input cost is
                    known even when the stream is aborted mid-generation.
                    unary:  top-level `usage` {input_tokens, output_tokens,
                    cache_read_input_tokens, cache_creation_input_tokens}.
                    NOTE: Anthropic's input_tokens EXCLUDES cached tokens —
                    they are reported separately. Apply billing weights in
                    the sink.

Capture trick: the tap retains the first and the most recent SSE event whose
raw bytes contain b'"usage"'. That single cheap test covers all three
protocols (message_start / message_delta / usage chunks / terminal response
events all contain it; bulk token-delta events do not). False positives —
e.g. generated text that literally contains "usage" — are harmless: the
finalizer validates JSON shape and either finds a later real usage event
overwrote it, or reports usage=None.

include_usage injection caveat (Chat Completions only): the injected final
usage chunk has `"choices": []`. Well-behaved SDKs handle it; naive clients
doing chunk.choices[0] will break. If you must hide it from clients that did
not request it, use `relay_events_filtered` (event-aligned relay, slightly
more hot-path work) instead of raw chunk relay for those requests only.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
from dataclasses import asdict, dataclass, field
from typing import Any, AsyncIterator, Protocol

logger = logging.getLogger("gateway.usage")

_USAGE_MARK = b'"usage"'
_LARGE_PARSE_THRESHOLD = 128 * 1024  # parse bigger payloads in executor
_MAX_PENDING = 8 * 1024 * 1024  # guard: largest single SSE event framed

# --------------------------------------------------------------------------
# Normalized usage + report
# --------------------------------------------------------------------------


@dataclass(slots=True)
class TokenUsage:
    input_tokens: int | None = None  # excludes cache reads for Anthropic
    output_tokens: int | None = None
    cache_read_tokens: int | None = None
    cache_write_tokens: int | None = None  # Anthropic cache_creation only
    reasoning_tokens: int | None = None  # subset of output_tokens
    total_tokens: int | None = None

    def any(self) -> bool:
        return (
            self.input_tokens is not None
            or self.output_tokens is not None
            or self.cache_read_tokens is not None
            or self.cache_write_tokens is not None
        )


@dataclass(slots=True)
class UsageReport:
    request_id: str
    protocol: str  # "chat" | "responses" | "anthropic"
    stream: bool
    backend: str
    model: str | None
    usage: TokenUsage | None  # None => upstream never reported usage
    completed: bool  # stream drained normally / unary 2xx
    status_code: int | None
    ttfb_ms: float | None  # first upstream byte (TTFT proxy)
    duration_ms: float
    bytes_relayed: int
    events: int  # SSE events framed (0 for unary)
    truncated: bool  # framing guard tripped; usage best-effort
    context: dict[str, Any] = field(default_factory=dict)  # reservation_id etc.
    error: str | None = None


class UsageSink(Protocol):
    """Settlement target. `settle` may be sync or return an awaitable."""

    def settle(self, report: UsageReport) -> Any: ...


# --------------------------------------------------------------------------
# SSE framing primitives (hot path — keep C-level)
# --------------------------------------------------------------------------


def _next_event(buf: bytearray) -> bytes | None:
    """Pop one complete SSE event off the front of `buf`, handling both
    \\n\\n and \\r\\n\\r\\n delimiters. Returns None if no complete event."""
    i = buf.find(b"\n\n")
    j = buf.find(b"\r\n\r\n")
    if i == -1 and j == -1:
        return None
    if j != -1 and (i == -1 or j < i):
        evt = bytes(buf[:j])
        del buf[: j + 4]
    else:
        evt = bytes(buf[:i])
        del buf[: i + 2]
    return evt


def _data_payload(evt: bytes) -> bytes | None:
    """Concatenate the `data:` lines of one SSE event (spec: join with \\n)."""
    out = []
    for line in evt.split(b"\n"):
        line = line.rstrip(b"\r")
        if line.startswith(b"data:"):
            v = line[5:]
            if v[:1] == b" ":
                v = v[1:]
            out.append(v)
    return b"\n".join(out) if out else None


async def _loads(raw: bytes) -> Any:
    if len(raw) > _LARGE_PARSE_THRESHOLD:
        return await asyncio.get_running_loop().run_in_executor(None, json.loads, raw)
    return json.loads(raw)


async def _event_json(evt: bytes | None) -> dict | None:
    """Parse one retained SSE event to a dict; tolerant of [DONE]/garbage."""
    if evt is None:
        return None
    data = _data_payload(evt)
    if data is None or data.strip() == b"[DONE]":
        return None
    try:
        obj = await _loads(data)
    except (ValueError, UnicodeDecodeError):
        return None
    return obj if isinstance(obj, dict) else None


# --------------------------------------------------------------------------
# Stream tap
# --------------------------------------------------------------------------


class StreamUsageTap:
    """Passive observer fed a copy of every relayed chunk.

    feed() cost per chunk: bytearray append + event framing + one substring
    containment check per complete event. Retains at most two raw events
    (first / last usage-bearing) plus the current partial event.
    """

    __slots__ = (
        "adapter",
        "meta",
        "buf",
        "first",
        "last",
        "t0",
        "t_first",
        "bytes_relayed",
        "events",
        "truncated",
    )

    def __init__(self, adapter: "BaseAdapter", meta: dict[str, Any]):
        self.adapter = adapter
        self.meta = meta
        self.buf = bytearray()
        self.first: bytes | None = None
        self.last: bytes | None = None
        self.t0 = time.perf_counter()
        self.t_first: float | None = None
        self.bytes_relayed = 0
        self.events = 0
        self.truncated = False

    # ---- hot path ----
    def feed(self, chunk: bytes) -> None:
        if self.t_first is None:
            self.t_first = time.perf_counter()
        self.bytes_relayed += len(chunk)
        buf = self.buf
        buf += chunk
        while (evt := _next_event(buf)) is not None:
            self.observe_event(evt)
        if len(buf) > _MAX_PENDING:
            # Pathological single event; drop and degrade to best-effort.
            # Framing may desync afterwards — finalizer JSON-validates, so
            # worst case is usage=None, never a wrong number.
            self.truncated = True
            del buf[:]

    def observe_event(self, evt: bytes) -> None:
        self.events += 1
        if _USAGE_MARK in evt:
            if self.first is None:
                self.first = evt
            self.last = evt

    def note_bytes(self, n: int) -> None:
        """For event-aligned relays that frame outside of feed()."""
        if self.t_first is None:
            self.t_first = time.perf_counter()
        self.bytes_relayed += n


# --------------------------------------------------------------------------
# Protocol adapters (parsers run OFF the hot path, in the settlement task)
# --------------------------------------------------------------------------


class BaseAdapter:
    protocol: str = "?"

    def prepare_request(self, payload: dict) -> dict:
        """Mutate/augment the upstream request so usage will be reported."""
        return payload

    async def parse_stream(
        self, first: bytes | None, last: bytes | None
    ) -> TokenUsage | None:
        raise NotImplementedError

    async def parse_unary(self, body: bytes) -> TokenUsage | None:
        raise NotImplementedError


def _from_chat(u: Any) -> TokenUsage | None:
    if not isinstance(u, dict):
        return None
    ptd = u.get("prompt_tokens_details") or {}
    ctd = u.get("completion_tokens_details") or {}
    tu = TokenUsage(
        input_tokens=u.get("prompt_tokens"),
        output_tokens=u.get("completion_tokens"),
        cache_read_tokens=ptd.get("cached_tokens") if isinstance(ptd, dict) else None,
        reasoning_tokens=ctd.get("reasoning_tokens") if isinstance(ctd, dict) else None,
        total_tokens=u.get("total_tokens"),
    )
    return tu if tu.any() else None


class ChatCompletionsAdapter(BaseAdapter):
    protocol = "chat"

    def prepare_request(self, payload: dict) -> dict:
        # Without include_usage the stream carries NO usage at all
        # (OpenAI and vLLM alike). Inject it; see module docstring for the
        # empty-choices final chunk caveat.
        if payload.get("stream"):
            so = payload.setdefault("stream_options", {})
            if isinstance(so, dict):
                so.setdefault("include_usage", True)
        return payload

    async def parse_stream(self, first, last):
        # Intermediate chunks carry "usage": null; only the terminal usage
        # chunk parses to a real dict. `last` is exactly that on completion.
        obj = await _event_json(last)
        return _from_chat(obj.get("usage")) if obj else None

    async def parse_unary(self, body):
        obj = await _loads(body)
        return _from_chat(obj.get("usage")) if isinstance(obj, dict) else None


def _from_responses(u: Any) -> TokenUsage | None:
    if not isinstance(u, dict):
        return None
    itd = u.get("input_tokens_details") or {}
    otd = u.get("output_tokens_details") or {}
    tu = TokenUsage(
        input_tokens=u.get("input_tokens"),
        output_tokens=u.get("output_tokens"),
        cache_read_tokens=itd.get("cached_tokens") if isinstance(itd, dict) else None,
        reasoning_tokens=otd.get("reasoning_tokens") if isinstance(otd, dict) else None,
        total_tokens=u.get("total_tokens"),
    )
    return tu if tu.any() else None


class ResponsesAdapter(BaseAdapter):
    protocol = "responses"

    async def parse_stream(self, first, last):
        # Terminal events (response.completed / .incomplete / .failed) embed
        # the full response object; usage lives inside it. Early events
        # (response.created) carry "usage": null and parse to None here.
        obj = await _event_json(last)
        if not obj:
            return None
        resp = obj.get("response")
        return _from_responses(resp.get("usage")) if isinstance(resp, dict) else None

    async def parse_unary(self, body):
        obj = await _loads(body)
        return _from_responses(obj.get("usage")) if isinstance(obj, dict) else None


class AnthropicMessagesAdapter(BaseAdapter):
    protocol = "anthropic"

    async def parse_stream(self, first, last):
        u = TokenUsage()
        fobj = await _event_json(first)
        if isinstance(fobj, dict) and fobj.get("type") == "message_start":
            mu = (fobj.get("message") or {}).get("usage")
            if isinstance(mu, dict):
                u.input_tokens = mu.get("input_tokens")
                u.cache_read_tokens = mu.get("cache_read_input_tokens")
                u.cache_write_tokens = mu.get("cache_creation_input_tokens")
                u.output_tokens = mu.get("output_tokens")  # provisional (~1)
        if last is not None and last is not first:
            lobj = await _event_json(last)
            if isinstance(lobj, dict) and lobj.get("type") == "message_delta":
                du = lobj.get("usage")
                if isinstance(du, dict):
                    # cumulative counts; last delta wins
                    if (v := du.get("output_tokens")) is not None:
                        u.output_tokens = v
                    if (v := du.get("input_tokens")) is not None:
                        u.input_tokens = v
        return u if u.any() else None

    async def parse_unary(self, body):
        obj = await _loads(body)
        if not isinstance(obj, dict):
            return None
        mu = obj.get("usage")
        if not isinstance(mu, dict):
            return None
        u = TokenUsage(
            input_tokens=mu.get("input_tokens"),
            output_tokens=mu.get("output_tokens"),
            cache_read_tokens=mu.get("cache_read_input_tokens"),
            cache_write_tokens=mu.get("cache_creation_input_tokens"),
        )
        return u if u.any() else None


ADAPTERS: dict[str, BaseAdapter] = {
    "chat": ChatCompletionsAdapter(),
    "responses": ResponsesAdapter(),
    "anthropic": AnthropicMessagesAdapter(),
}

PATHS = {
    "chat": "/v1/chat/completions",
    "responses": "/v1/responses",
    "anthropic": "/v1/messages",
}


# --------------------------------------------------------------------------
# Service: taps, deferred settlement tasks, sinks
# --------------------------------------------------------------------------


class UsageService:
    """Owns the sink and the detached settlement tasks.

    finalize_stream()/settle_unary() are synchronous, never raise into the
    caller, and only schedule work — they are safe to call from a relay
    generator's `finally` block during cancellation.
    """

    def __init__(self, sink: UsageSink):
        self._sink = sink
        self._tasks: set[asyncio.Task] = set()

    # ---- lifecycle ----
    async def aclose(self) -> None:
        """Drain in lifespan shutdown so final settlements aren't lost."""
        if self._tasks:
            await asyncio.gather(*tuple(self._tasks), return_exceptions=True)

    def _spawn(self, coro) -> None:
        try:
            t = asyncio.get_running_loop().create_task(coro)
        except RuntimeError:  # loop tearing down
            coro.close()
            logger.warning("usage settlement dropped: event loop closed")
            return
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    # ---- streaming pathway ----
    def tap(
        self,
        adapter: BaseAdapter,
        *,
        request_id: str,
        backend: str,
        model: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> StreamUsageTap:
        return StreamUsageTap(
            adapter,
            {
                "request_id": request_id,
                "backend": backend,
                "model": model,
                "context": context or {},
            },
        )

    def finalize_stream(
        self,
        tap: StreamUsageTap,
        *,
        completed: bool,
        status_code: int | None = None,
        error: str | None = None,
    ) -> None:
        # Flush any final partial event (streams may end without a trailing
        # blank line after the last event).
        if tap.buf:
            tap.observe_event(bytes(tap.buf))
            del tap.buf[:]
        self._spawn(self._settle_stream(tap, completed, status_code, error))

    async def _settle_stream(self, tap, completed, status_code, error):
        usage: TokenUsage | None = None
        try:
            usage = await tap.adapter.parse_stream(tap.first, tap.last)
        except Exception:
            logger.exception("usage parse failed (stream)")
        now = time.perf_counter()
        report = UsageReport(
            request_id=tap.meta["request_id"],
            protocol=tap.adapter.protocol,
            stream=True,
            backend=tap.meta["backend"],
            model=tap.meta.get("model"),
            usage=usage,
            completed=completed,
            status_code=status_code,
            ttfb_ms=(tap.t_first - tap.t0) * 1000 if tap.t_first else None,
            duration_ms=(now - tap.t0) * 1000,
            bytes_relayed=tap.bytes_relayed,
            events=tap.events,
            truncated=tap.truncated,
            context=tap.meta.get("context") or {},
            error=error,
        )
        await self._deliver(report)

    # ---- unary pathway ----
    def settle_unary(
        self,
        adapter: BaseAdapter,
        body: bytes | None,
        *,
        request_id: str,
        backend: str,
        model: str | None,
        status_code: int | None,
        started: float,
        context: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        self._spawn(
            self._settle_unary(
                adapter,
                body,
                request_id,
                backend,
                model,
                status_code,
                started,
                context or {},
                error,
            )
        )

    async def _settle_unary(
        self,
        adapter,
        body,
        request_id,
        backend,
        model,
        status_code,
        started,
        context,
        error,
    ):
        usage: TokenUsage | None = None
        ok = status_code is not None and 200 <= status_code < 300
        if ok and body:
            try:
                usage = await adapter.parse_unary(body)
            except Exception:
                logger.exception("usage parse failed (unary)")
        report = UsageReport(
            request_id=request_id,
            protocol=adapter.protocol,
            stream=False,
            backend=backend,
            model=model,
            usage=usage,
            completed=ok,
            status_code=status_code,
            ttfb_ms=None,
            duration_ms=(time.perf_counter() - started) * 1000,
            bytes_relayed=len(body) if body else 0,
            events=0,
            truncated=False,
            context=context,
            error=error,
        )
        await self._deliver(report)

    async def _deliver(self, report: UsageReport) -> None:
        try:
            res = self._sink.settle(report)
            if inspect.isawaitable(res):
                await res
        except Exception:
            logger.exception("usage sink failed request_id=%s", report.request_id)


# --------------------------------------------------------------------------
# Sinks
# --------------------------------------------------------------------------


class LoggingSink:
    """Structured JSON log line per request (json.dumps runs in the
    settlement task, off the hot path)."""

    def settle(self, report: UsageReport) -> None:
        logger.info(json.dumps(asdict(report), default=str, separators=(",", ":")))


class CompositeSink:
    def __init__(self, *sinks: UsageSink):
        self._sinks = sinks

    async def settle(self, report: UsageReport) -> None:
        for s in self._sinks:
            res = s.settle(report)
            if inspect.isawaitable(res):
                await res


class AdmissionSettlementSink:
    """Example: settle tokens/minute reservations.

    Policy notes for the controller (belongs HERE, not in the adapters):
      * usage present  -> settle actual = input + output (+ weighted cache
        reads/writes per your billing model).
      * usage None, completed=False (abort/timeout) -> conservative: charge
        the full reservation, or estimate. For Anthropic aborts, usage often
        still carries exact input_tokens (message_start) — charge that plus
        an output estimate from report.events (~1 content delta per few
        tokens) if you want tighter accounting.
      * usage None, completed=False, bytes_relayed==0 (connect fail / 5xx
        before first byte) -> release the reservation entirely.
    """

    def __init__(self, controller: Any):
        self._c = controller

    def settle(self, report: UsageReport) -> None:
        rid = report.context.get("reservation_id")
        if rid is None:
            return
        u = report.usage
        if u is not None and (
            u.input_tokens is not None or u.output_tokens is not None
        ):
            self._c.settle(
                rid,
                input_tokens=u.input_tokens or 0,
                output_tokens=u.output_tokens or 0,
                cache_read_tokens=u.cache_read_tokens or 0,
            )
        elif report.bytes_relayed == 0 and not report.completed:
            self._c.release(rid)
        else:
            self._c.settle_unknown(rid, report)  # controller's estimate policy


# --------------------------------------------------------------------------
# Optional: event-aligned relay that can drop the injected usage chunk
# (Chat Completions only; use ONLY for clients that did not request
# include_usage themselves — see module docstring).
# --------------------------------------------------------------------------


def _looks_like_injected_usage_chunk(evt: bytes) -> bool:
    return (b'"choices":[]' in evt or b'"choices": []' in evt) and (
        b'"usage":{' in evt or b'"usage": {' in evt
    )


async def relay_events_filtered(
    chunks: AsyncIterator[bytes], tap: StreamUsageTap, *, drop_injected_usage: bool
) -> AsyncIterator[bytes]:
    """Yields whole SSE events instead of raw chunks. Slightly more hot-path
    work than tap.feed + raw yield; only use when filtering is required."""
    buf = bytearray()
    async for chunk in chunks:
        tap.note_bytes(len(chunk))
        buf += chunk
        while (evt := _next_event(buf)) is not None:
            tap.observe_event(evt)
            if drop_injected_usage and _looks_like_injected_usage_chunk(evt):
                continue
            yield evt + b"\n\n"
    if buf:
        evt = bytes(buf)
        tap.observe_event(evt)
        yield evt


# --------------------------------------------------------------------------
# EXAMPLE WIRING (matches the relay patterns from the gateway spec)
# --------------------------------------------------------------------------
#
# usage_service = UsageService(CompositeSink(
#     AdmissionSettlementSink(admission_controller),
#     LoggingSink(),
# ))
#
# async def proxy_stream(origin: str, protocol: str, payload: dict,
#                        *, request_id: str, reservation_id: str):
#     adapter = ADAPTERS[protocol]
#     payload = adapter.prepare_request(payload)          # chat: include_usage
#     client = CLIENTS[origin]
#     req = client.build_request("POST", PATHS[protocol], json=payload)
#     upstream = await client.send(req, stream=True)
#
#     if upstream.status_code != 200:
#         body = await upstream.aread()
#         await upstream.aclose()
#         # Errors settle too — otherwise the reservation leaks.
#         usage_service.settle_unary(adapter, body, request_id=request_id,
#             backend=origin, model=payload.get("model"),
#             status_code=upstream.status_code, started=time.perf_counter(),
#             context={"reservation_id": reservation_id}, error="upstream_error")
#         return JSONResponse(status_code=upstream.status_code,
#                             content=json.loads(body or b"{}"))
#
#     tap = usage_service.tap(adapter, request_id=request_id, backend=origin,
#                             model=payload.get("model"),
#                             context={"reservation_id": reservation_id})
#
#     async def relay():
#         completed = False
#         error = None
#         try:
#             async for chunk in upstream.aiter_raw():
#                 tap.feed(chunk)          # <-- entire hot-path cost of usage
#                 yield chunk
#             completed = True
#         except httpx.ReadTimeout:
#             error = "read_timeout"
#             raise
#         finally:                          # runs on completion AND disconnect
#             with anyio.CancelScope(shield=True):
#                 await upstream.aclose()   # frees pool conn; aborts vLLM
#             usage_service.finalize_stream(tap, completed=completed,
#                 status_code=upstream.status_code,
#                 error=error if completed is False else None)
#
#     return StreamingResponse(relay(), media_type="text/event-stream",
#         headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
#
#
# async def proxy_unary(origin: str, protocol: str, payload: dict,
#                       *, request_id: str, reservation_id: str):
#     adapter = ADAPTERS[protocol]
#     client = CLIENTS[origin]
#     started = time.perf_counter()
#     try:
#         async with asyncio.timeout(910):
#             r = await client.post(PATHS[protocol], json=payload,
#                                   timeout=UNARY_TIMEOUT)
#     except Exception as e:
#         usage_service.settle_unary(adapter, None, request_id=request_id,
#             backend=origin, model=payload.get("model"), status_code=None,
#             started=started, context={"reservation_id": reservation_id},
#             error=type(e).__name__)
#         raise
#     # Response already fully buffered; parsing is deferred to a task, so
#     # the client gets its bytes before any JSON work happens.
#     usage_service.settle_unary(adapter, r.content, request_id=request_id,
#         backend=origin, model=payload.get("model"),
#         status_code=r.status_code, started=started,
#         context={"reservation_id": reservation_id})
#     return Response(content=r.content, status_code=r.status_code,
#                     media_type=r.headers.get("content-type"))
