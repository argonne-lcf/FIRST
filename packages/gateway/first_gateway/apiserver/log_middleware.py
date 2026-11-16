"""Pure-ASGI middleware that emits the Response event and owns the correlation id.

It generates one UUID per request into ``_request_id_ctx`` (propagating down to
deps, the route, ``InferenceService``, and the logging ``RequestIdFilter``),
observes the real ``http.response.start``/``http.response.body`` messages, and
emits a :class:`ResponseLog` after the response is sent.

It buffers a response body ONLY for error responses (``status_code >= 400``,
which are small, and are inlined but never spilled to disk); success bodies —
unary and streaming alike — are never buffered here, so a 2xx ResponseLog
always has ``body=None`` (inference content is persisted by
``InferenceService``).

Every response also carries a ``Server-Timing`` header, so a client can split
its own end-to-end latency without access to the logs::

    Server-Timing: queue;dur=0.8, gateway;dur=41.3, upstream;dur=38.9;desc="attempts=1"

- ``gateway``: this middleware's start until the response headers are sent.
- ``upstream``: time spent waiting on inference backends, summed over attempts
  (omitted when no backend was called).  ``gateway - upstream`` is the
  gateway's own overhead.
- ``queue``: time between the reverse proxy forwarding the request (its
  ``X-Request-Start: t=<epoch seconds>`` stamp) and this middleware starting,
  i.e. connection setup and waiting for a worker.  Omitted without the stamp.

For a streaming response the headers precede the body, so ``gateway`` and
``upstream`` both stop at the first byte: they cover the time to open the
stream, not to relay it.
"""

import time
import uuid

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from first_common.schema.structured_logs import ResponseLog

from .context import RequestTiming, _request_id_ctx, _request_timing_ctx

# Constant probe traffic; the only anonymous route worth suppressing.
_SKIP_PATHS = {"/health"}

# A stamp further off than this is a clock or client artefact, not queueing.
_MAX_QUEUE_SEC = 3600.0


def _queue_ms(scope: Scope, now: float) -> float | None:
    """Milliseconds since the proxy's ``X-Request-Start: t=<epoch>`` stamp."""
    for name, value in scope.get("headers", ()):
        if name == b"x-request-start":
            try:
                queued = now - float(value.decode("latin-1").removeprefix("t="))
            except ValueError:
                return None
            return queued * 1000 if 0 <= queued < _MAX_QUEUE_SEC else None
    return None


def _server_timing(queue_ms: float | None, t0: float, timing: RequestTiming) -> bytes:
    parts = [] if queue_ms is None else [f"queue;dur={queue_ms:.2f}"]
    parts.append(f"gateway;dur={(time.perf_counter() - t0) * 1000:.2f}")
    if timing.attempts:
        parts.append(
            f"upstream;dur={timing.upstream_sec * 1000:.2f}"
            f';desc="attempts={timing.attempts}"'
        )
    return ", ".join(parts).encode()


class ResponseLogMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        queue_ms = _queue_ms(scope, time.time())
        cid = str(uuid.uuid4())
        token = _request_id_ctx.set(cid)
        timing = RequestTiming()
        timing_token = _request_timing_ctx.set(timing)
        t0 = time.perf_counter()

        status_code = 500
        started = False
        streaming = False
        error_body = bytearray()

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code, started, streaming
            if message["type"] == "http.response.start":
                started = True
                status_code = message["status"]
                headers = message.get("headers") or []
                for name, value in headers:
                    if name.lower() == b"content-type":
                        streaming = value.lower().startswith(b"text/event-stream")
                        break
                message = {
                    **message,
                    "headers": [
                        *headers,
                        (b"server-timing", _server_timing(queue_ms, t0, timing)),
                    ],
                }
            elif message["type"] == "http.response.body":
                # Only small error bodies are captured here; success/streaming
                # content is persisted by InferenceService.
                if status_code >= 400 and not streaming:
                    error_body.extend(message.get("body", b""))
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            # An uncaught exception propagates un-statused (ServerErrorMiddleware,
            # which emits the 500, is above us). Log our own ResponseLog so the
            # request_id is preserved, then re-raise.  If the status line already
            # went out (a stream broken mid-body), that status is what the client
            # got, not a 500.
            if started:
                self._emit(cid, status_code, t0, streaming, raw_body=None)
            else:
                self._emit(cid, 500, t0, streaming=False, raw_body=None)
            raise
        else:
            if scope["path"] not in _SKIP_PATHS:
                raw = bytes(error_body) if status_code >= 400 else None
                self._emit(cid, status_code, t0, streaming, raw)
        finally:
            _request_timing_ctx.reset(timing_token)
            _request_id_ctx.reset(token)

    @staticmethod
    def _emit(
        cid: str,
        status_code: int,
        t0: float,
        streaming: bool,
        raw_body: bytes | None,
    ) -> None:
        # No storage_dir: error bodies are inlined when small, never spilled.
        ResponseLog(
            request_id=cid,
            status_code=status_code,
            duration_ms=(time.perf_counter() - t0) * 1000,
            streaming=streaming,
        ).emit(raw_body=raw_body)
