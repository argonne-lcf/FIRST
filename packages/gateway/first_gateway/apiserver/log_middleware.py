import asyncio
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from logging import getLogger

from fastapi.requests import Request
from fastapi.responses import Response, StreamingResponse

from first_common.schema.structured_logs import (
    AccessLog,
)
from first_gateway.cache import should_throttle

from .context import RequestContext, _request_context

logger = getLogger(__name__)


def initialize_access_log(request: Request) -> AccessLog:
    """Return initial state of an AccessLog entry"""
    origin_ip = request.headers.get("X-Forwarded-For")
    if not origin_ip and request.client is not None:
        origin_ip = request.client.host

    # Remove duplicate if any
    if origin_ip:
        ip_list = [ip.strip() for ip in origin_ip.split(",")]
        origin_ip = ", ".join(set(ip_list))

    return AccessLog(
        id=str(uuid.uuid4()),
        timestamp_request=datetime.now(timezone.utc),
        api_route=request.url.path,
        origin_ip=origin_ip,
    )


async def write_logs(context: RequestContext, response: Response) -> None:
    context.access_log.emit(context.user, response)

    if context.request_log:
        if isinstance(response, StreamingResponse):
            body = "streaming_response_in_progress"
        elif isinstance(response.body, bytes):
            body = response.body.decode(errors="ignore")
        else:
            body = "unavailable"
        context.request_log.emit(body, response.status_code)

        if not isinstance(response, StreamingResponse):
            await context.request_log.emit_metrics()


_background_tasks: set[asyncio.Task[None]] = set()


def _on_done(task: asyncio.Task[None]) -> None:
    _background_tasks.discard(task)
    if task.cancelled():
        return
    if exc := task.exception():
        logger.error("Background log write failed", exc_info=exc)


async def log_request(request: Request, call_next) -> Response:

    token = _request_context.set(RequestContext(initialize_access_log(request)))

    try:
        response = await call_next(request)
        ctx_data = _request_context.get()
    finally:
        _request_context.reset(token)

    if await should_skip_logging(ctx_data, request, response):
        return response

    # Fire-and-forget logging pattern:
    task = asyncio.create_task(write_logs(ctx_data, response))
    _background_tasks.add(task)
    task.add_done_callback(_on_done)
    return response


async def should_skip_logging(
    ctx: RequestContext,
    request: Request,
    response: Response,
) -> bool:
    # Don't log internal streaming requests:
    if "api/streaming" in request.url.path:
        return True

    status_code = response.status_code
    fingerprint = (
        "<streaming>"
        if isinstance(response, StreamingResponse)
        else str(response.body[:128])
    )

    user = ctx.user.username if ctx.user else ctx.access_log.origin_ip

    # Debounce if it's the same user/error repeatedly:
    if status_code >= 400 and await should_throttle(user, fingerprint, status_code):
        return True

    # Internal errors de-dup'd at user/status level:
    if status_code >= 500 and await should_throttle(user, status_code):
        return True

    return False
