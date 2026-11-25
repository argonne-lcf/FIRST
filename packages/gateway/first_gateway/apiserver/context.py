import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Iterator

from starlette.types import Scope

from first_common.schema.structured_logs import InferenceLog

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


# InferenceLog fields repeated on the request's ResponseLog.
_SUMMARY_INFERENCE_FIELDS = (
    "endpoint",
    "model",
    "deployment",
    "cluster",
    "outcome",
    "attempt",
    "latency_sec",
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "cache_read_tokens",
    "cache_write_tokens",
    "reasoning_tokens",
)


@dataclass
class RequestSummary:
    """What a request's other events learned, repeated on its ResponseLog.

    ``ResponseLogMiddleware`` puts one in ``_request_summary_ctx`` per request;
    ``get_auth_user`` and ``InferenceService`` fill it in. It is mutated in
    place, never re-set, so writes made in child tasks (a streaming body is
    relayed in one) are visible to the middleware.
    """

    auth_attempted: bool = False
    user_id: str | None = None
    # The request's most recent attempt: after the request, its final one.
    last_inference: InferenceLog | None = None

    def response_fields(self) -> dict[str, Any]:
        fields: dict[str, Any] = {
            "auth_attempted": self.auth_attempted,
            "user_id": self.user_id,
        }
        if self.last_inference is not None:
            for name in _SUMMARY_INFERENCE_FIELDS:
                fields[name] = getattr(self.last_inference, name)
        return fields


_request_summary_ctx: ContextVar[RequestSummary] = ContextVar("_request_summary")


def get_request_summary() -> RequestSummary | None:
    """The current request's summary, or None outside a request."""
    return _request_summary_ctx.get(None)


def origin_ip_from_scope(scope: Scope) -> str | None:
    """The client address: the fronting NGINX's ``X-Real-IP``, else the peer.

    NGINX sets X-Real-IP to the client address it resolved from trusted proxies
    only. X-Forwarded-For is not read: clients can set its leading entries.
    """
    for name, value in scope.get("headers", ()):
        if name == b"x-real-ip" and value:
            return str(value.decode("latin-1"))
    client = scope.get("client")
    return str(client[0]) if client else None


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
