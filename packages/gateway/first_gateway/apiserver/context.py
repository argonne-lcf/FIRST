import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Iterator

_request_id_ctx: ContextVar[str] = ContextVar("_request_id")


def get_request_id() -> str | None:
    """Return the correlation id for the current request, or None if unset.

    Set by ``ResponseLogMiddleware`` at the very start of each request and read
    by ``InferenceService`` and the logging ``RequestIdFilter``.
    """
    return _request_id_ctx.get(None)


@dataclass
class RequestTiming:
    """Upstream time accumulated over a request's backend attempts.

    Reported to the caller in the ``Server-Timing`` response header (see
    ``ResponseLogMiddleware``), next to the gateway's own request time.
    """

    upstream_sec: float = 0.0
    attempts: int = 0


_request_timing_ctx: ContextVar[RequestTiming] = ContextVar("_request_timing")


@contextmanager
def upstream_timer() -> Iterator[None]:
    """Time one backend attempt and add it to the current request's timing.

    For a unary request, wrap the full upstream round trip; for a stream, wrap
    the wait for the backend's response headers (the gateway's own headers go
    out before the stream body exists).  Counts failed attempts too.  No-op
    outside a request.
    """
    t0 = time.perf_counter()
    try:
        yield
    finally:
        timing = _request_timing_ctx.get(None)
        if timing is not None:
            timing.upstream_sec += time.perf_counter() - t0
            timing.attempts += 1
