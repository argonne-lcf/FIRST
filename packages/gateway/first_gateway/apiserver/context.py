import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
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
    # Profiling (tests/load/PROFILING.md): the request's wall time, cut into
    # consecutive named phases.  `last` is where the current phase began.
    last: float = field(default_factory=time.perf_counter)
    phases: dict[str, float] = field(default_factory=dict)


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


def lap(name: str) -> None:
    """Close the current phase of the request: charge the time since the last
    ``lap`` (or the request's start) to ``name``.

    A request runs its hot path sequentially, so consecutive laps partition its
    wall time with nothing left over.  A phase with no ``await`` in it is pure
    CPU; one with an ``await`` also contains event-loop wait (see
    tests/load/PROFILING.md).  Repeated names (retries) add up.  No-op outside
    a request.
    """
    timing = _request_timing_ctx.get(None)
    if timing is not None:
        now = time.perf_counter()
        timing.phases[name] = timing.phases.get(name, 0.0) + now - timing.last
        timing.last = now
