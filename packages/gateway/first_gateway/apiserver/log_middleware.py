import asyncio
import uuid
from datetime import datetime, timezone
from logging import getLogger
from pathlib import Path
from typing import Any

from fastapi.requests import Request
from fastapi.responses import Response, StreamingResponse
from redis.asyncio import Redis

from first_common.schema.structured_logs import (
    AccessLog,
)

from ..settings import ClientState
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


async def write_logs(
    context: RequestContext, response: Response, prompt_storage_dir: Path
) -> None:
    context.access_log.emit(context.user, response)

    if context.request_log:
        if isinstance(response, StreamingResponse):
            body = "streaming_response_in_progress"
        elif isinstance(response.body, bytes):
            body = response.body.decode(errors="ignore")
        else:
            body = "unavailable"
        context.request_log.emit(
            body, response.status_code, prompt_dir=prompt_storage_dir
        )

        if not isinstance(response, StreamingResponse):
            await context.request_log.emit_metrics()


_background_tasks: set[asyncio.Task[None]] = set()


def _on_done(task: asyncio.Task[None]) -> None:
    _background_tasks.discard(task)
    if task.cancelled():
        return
    if exc := task.exception():
        logger.error("Background log write failed", exc_info=exc)


async def log_request(request: Request, call_next: Any) -> Response:

    token = _request_context.set(RequestContext(initialize_access_log(request)))

    try:
        response: Response = await call_next(request)
        ctx_data = _request_context.get()
    finally:
        _request_context.reset(token)

    client_state: ClientState = request.app.state.client_state
    if await should_skip_logging(ctx_data, request, response, client_state.redis):
        return response

    # Fire-and-forget logging pattern:
    task = asyncio.create_task(
        write_logs(ctx_data, response, client_state.settings.prompt_storage_dir)
    )
    _background_tasks.add(task)
    task.add_done_callback(_on_done)
    return response


async def should_skip_logging(
    ctx: RequestContext,
    request: Request,
    response: Response,
    redis: Redis,
) -> bool:
    # Don't log internal streaming requests:
    if "api/streaming" in request.url.path:
        return True

    status_code = response.status_code
    user = ctx.user.username if ctx.user else ctx.access_log.origin_ip

    if status_code < 400:
        return False
    elif status_code >= 500:
        is_new_err = await redis.set(f"{user}{status_code}", "", nx=True, ex=30)
    else:
        body = getattr(response, "body", b"")
        fingerprint = (
            "<streaming>"
            if isinstance(response, StreamingResponse)
            else (str(body[:128]))
        )
        is_new_err = await redis.set(
            f"{user}{fingerprint}{status_code}", "", nx=True, ex=30
        )

    # De-duplicate logs when it's the same user/error repeatedly:
    return not is_new_err
