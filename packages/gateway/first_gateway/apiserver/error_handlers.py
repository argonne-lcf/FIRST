import logging
import uuid
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from first_common.errors import FirstError, TaskPending

logger = logging.getLogger(__name__)


def error_body(code: str, message: str, **extra: Any) -> dict[str, Any]:
    """
    Error envelope that validates as both an OpenAI error
    (`{"error": {"message", "code"}}`) and an Anthropic error
    (`{"type": "error", "error": {"type", "message"}}`).  SDKs that fail to
    parse an error body (e.g. the Vercel AI SDK used by OpenCode) fall back to
    the bare HTTP status text, hiding our message from the user.
    """
    return {
        "type": "error",
        "error": {"type": code, "code": code, "message": message, **extra},
    }


def register_error_handlers(app: FastAPI) -> None:
    """Install the shared FirstError/uncaught exception handlers."""

    @app.exception_handler(FirstError)
    def handle_app_error(_request: Request, exc: FirstError) -> JSONResponse:
        headers = dict(exc.headers)
        if exc.retry_after_sec is not None:
            headers["Retry-After"] = str(exc.retry_after_sec)

        body: dict[str, Any]
        if isinstance(exc, TaskPending):
            body = {"status": exc.code, "task_id": exc.task_id}
        else:
            body = error_body(exc.code, str(exc), info=exc.info)

        return JSONResponse(body, status_code=exc.status_code, headers=headers or None)

    @app.exception_handler(Exception)
    def handle_uncaught_error(request: Request, exc: Exception) -> JSONResponse:
        error_id = uuid.uuid4().hex
        logger.exception(
            f"Uncaught Exception in API View {request.url.path!r}",
            extra={"error_id": error_id},
            exc_info=exc,
        )

        return JSONResponse(
            error_body("internal_error", "Internal Server Error", error_id=error_id),
            status_code=500,
        )
