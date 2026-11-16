"""``Server-Timing`` response header: where a request's time went.

Lets a client split its own end-to-end latency without access to the logs::

    Server-Timing: queue;dur=0.8, gateway;dur=41.3, upstream;dur=38.9;desc="attempts=1"

- ``gateway``: this middleware's start until the response object is returned.
- ``upstream``: time spent waiting on the compute backend (a direct API call,
  or the whole Globus Compute round trip).  ``gateway - upstream`` is the
  gateway's own overhead.  Omitted when no backend was called.
- ``queue``: time between nginx forwarding the request (its
  ``X-Request-Start: t=<epoch seconds>`` stamp) and this middleware starting,
  i.e. connection setup, waiting for a worker and reading the body.  Omitted
  without the stamp.

A streaming response is returned before its body is produced (and, for most
adapters, before the backend is even contacted), so it reports only the time to
open the stream; ``upstream`` is usually absent.

The header format matches the V2 gateway's, so one load-test harness reads both.
"""

import time
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass
from inspect import iscoroutinefunction

from asgiref.sync import markcoroutinefunction
from django.http import HttpRequest, HttpResponse, StreamingHttpResponse

# A stamp further off than this is a clock or client artefact, not queueing.
_MAX_QUEUE_SEC = 3600.0


@dataclass
class RequestTiming:
    upstream_sec: float = 0.0
    attempts: int = 0


_request_timing: ContextVar[RequestTiming] = ContextVar("_request_timing")


def record_upstream_time(seconds: float) -> None:
    """Add one backend call's duration to the current request's timing."""
    timing = _request_timing.get(None)
    if timing is not None:
        timing.upstream_sec += seconds
        timing.attempts += 1


def _queue_ms(request: HttpRequest, now: float) -> float | None:
    stamp = request.headers.get("X-Request-Start")
    if stamp is None:
        return None
    try:
        queued = now - float(stamp.removeprefix("t="))
    except ValueError:
        return None
    return queued * 1000 if 0 <= queued < _MAX_QUEUE_SEC else None


class ServerTimingMiddleware:
    """Outermost middleware, so ``gateway`` covers the whole Django stack."""

    sync_capable = False
    async_capable = True

    def __init__(
        self,
        get_response: Callable[
            [HttpRequest], Awaitable[HttpResponse | StreamingHttpResponse]
        ],
    ):
        self.get_response = get_response
        if iscoroutinefunction(self.get_response):
            markcoroutinefunction(self)

    async def __call__(
        self, request: HttpRequest
    ) -> HttpResponse | StreamingHttpResponse:
        queue_ms = _queue_ms(request, time.time())
        timing = RequestTiming()
        token = _request_timing.set(timing)
        t0 = time.perf_counter()
        try:
            response = await self.get_response(request)
        finally:
            _request_timing.reset(token)

        parts = [] if queue_ms is None else [f"queue;dur={queue_ms:.2f}"]
        parts.append(f"gateway;dur={(time.perf_counter() - t0) * 1000:.2f}")
        if timing.attempts:
            parts.append(
                f"upstream;dur={timing.upstream_sec * 1000:.2f}"
                f';desc="attempts={timing.attempts}"'
            )
        response["Server-Timing"] = ", ".join(parts)
        return response
