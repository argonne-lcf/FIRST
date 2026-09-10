"""Structured JSONL log events emitted at their source during a request.

Three events, correlated by one ``request_id`` (a UUID generated in the ASGI
middleware and stored in a contextvar):

* :class:`RequestLog`   — emitted in ``get_auth_user`` once auth succeeds.
* :class:`ResponseLog`  — emitted in the ASGI middleware after the response is sent.
* :class:`InferenceLog` — emitted in ``InferenceService`` per completed inference.

Each event's ``emit()`` writes one lean JSONL line (non-blocking, via the
QueueHandler). When a body is large (>= ``INLINE_BODY_LIMIT`` bytes) it is not
inlined: the full raw bytes are written to a date-partitioned file under
``storage_dir`` on a worker thread (fire-and-forget), and the JSONL line carries
``body=None`` + ``body_stored=True``.
"""

import asyncio
import uuid
from datetime import datetime, timezone
from logging import getLogger
from pathlib import Path
from typing import ClassVar, Literal

import anyio
from pydantic import BaseModel, Field

logger = getLogger(__name__)

# Bodies smaller than this are inlined in the JSONL line; larger ones are
# written to a separate on-disk file correlated by request_id.
INLINE_BODY_LIMIT = 2000


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# Keep a strong reference to in-flight file-write tasks so they aren't GC'd
# before completion
_background_tasks: set[asyncio.Task[None]] = set()


def _on_write_done(task: asyncio.Task[None]) -> None:
    _background_tasks.discard(task)
    if task.cancelled():
        return
    if exc := task.exception():
        logger.error("Structured-log body write failed", exc_info=exc)


async def _write_body_file(path: Path, raw: bytes) -> None:
    """Write ``raw`` to ``path`` on a worker thread.  Creates the date-partition
    directory on first write."""

    def _write() -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)

    await anyio.to_thread.run_sync(_write)


def _store_body(
    raw: bytes, subdir: str, request_id: str, timestamp: datetime, storage_dir: Path
) -> tuple[str | None, bool]:
    """Apply the inline-vs-file decision.

    Small bodies are decoded and returned inline. Large bodies are scheduled for
    a fire-and-forget threaded write to
    ``{storage_dir}/{subdir}/YYYY/MM/DD/{request_id}.json`` (UTC date from
    ``timestamp``) and returned as ``(None, True)``.
    """
    if len(raw) < INLINE_BODY_LIMIT:
        return raw.decode(errors="replace"), False

    path = storage_dir / subdir / timestamp.strftime("%Y/%m/%d") / f"{request_id}.json"
    task = asyncio.create_task(_write_body_file(path, raw))
    _background_tasks.add(task)
    task.add_done_callback(_on_write_done)
    return None, True


class _StructuredLog(BaseModel):
    """Base for the three structured events: carries the shared correlation id,
    timestamp, and lean-JSONL emit + large-body persistence logic."""

    # Subdirectory under storage_dir for large-body files ("large-requests" or
    # "large-responses"). Empty for events that never carry a body.
    _body_subdir: ClassVar[str] = ""

    # Unique per *emitted event* (never reused across retries or event types).
    # This is the delivery-dedup key: at-least-once redelivery of the same line
    # carries the same event_id and collapses downstream, while two genuinely
    # distinct events (e.g. two retry attempts) keep two ids and both survive.
    # Contrast request_id, the correlation key, which is deliberately shared.
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    stream: str
    request_id: str
    timestamp: datetime = Field(default_factory=_utc_now)
    body: str | None = None
    body_stored: bool = False

    def emit(
        self, raw_body: bytes | None = None, storage_dir: Path | None = None
    ) -> None:
        """Emit the lean JSONL line; persist a large body to disk if needed.

        ``raw_body`` is the captured body bytes for this event (or ``None`` when
        the event carries no body). ``storage_dir`` is required whenever a body
        may need off-lining (i.e. whenever ``raw_body`` is not None).
        """
        if raw_body is not None:
            assert storage_dir is not None, "storage_dir required to store a body"
            self.body, self.body_stored = _store_body(
                raw_body,
                self._body_subdir,
                self.request_id,
                self.timestamp,
                storage_dir,
            )
        logger.info(self.stream, extra=self.model_dump(mode="json"))


class RequestLog(_StructuredLog):
    """An authenticated HTTP request reached the service (emitted post-auth)."""

    _body_subdir: ClassVar[str] = "large-requests"

    stream: Literal["request"] = "request"

    user_id: str | None = None
    user_name: str | None = None
    username: str | None = None
    user_group_uuids: list[str] | None = None
    authorized_group_uuids: str | None = None
    idp_id: str | None = None
    idp_name: str | None = None
    auth_service: str | None = None

    method: str
    path: str
    origin_ip: str | None
    content_length: int | None


class ResponseLog(_StructuredLog):
    """An HTTP response was sent (emitted in the ASGI middleware)."""

    _body_subdir: ClassVar[str] = "large-responses"

    stream: Literal["response"] = "response"

    status_code: int
    duration_ms: float
    streaming: bool


InferenceOutcome = Literal[
    "success",  # backend returned a usable response
    "upstream_error",  # backend returned a retryable 5xx / connection failure
    "upstream_rejected",  # backend returned a non-retryable 4xx
    "admission_rejected",  # request never dispatched (admission control refused)
]


class InferenceLog(_StructuredLog):
    """One *attempt* to serve an inference against a backend (emitted in
    InferenceService). Emitted for every attempt — success, retry, or terminal
    failure — so a single request_id may carry several InferenceLogs. Carries
    the response *content* for success cases.

    A request's logical result is the roll-up of its attempts by request_id:
    ``success`` if any attempt succeeded, else the last attempt's outcome. Token
    usage is taken from the successful attempt (failed attempts report what they
    can, usually nothing)."""

    _body_subdir: ClassVar[str] = "large-responses"

    stream: Literal["inference"] = "inference"

    # Denormalized from the request's auth context
    user_id: str

    endpoint: str
    model: str
    deployment: str
    cluster: str
    backend_id: str
    backend_model_url: str

    # Per-attempt result. ``attempt`` is 1-based within this request_id;
    # ``error`` carries the failure summary for non-success outcomes.
    outcome: InferenceOutcome = "success"
    attempt: int = 1
    error: str | None = None
    latency_sec: float

    input_tokens: int | None
    output_tokens: int | None
    total_tokens: int | None
    cache_read_tokens: int | None
    cache_write_tokens: int | None
    reasoning_tokens: int | None
