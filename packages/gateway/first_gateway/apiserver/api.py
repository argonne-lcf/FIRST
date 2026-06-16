import logging
import uuid
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from first_common.errors import FirstError, TaskPending
from first_gateway.settings import ClientState, Settings

from ..log_config import config_logging
from .log_middleware import log_request
from .routes import routers

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[ClientState, None]:
    """
    Initializes ClientState and makes it available on all request.state.
    """
    settings = Settings()
    config_logging(settings.log_level)
    async with settings.build_clients() as client_state:
        yield client_state


app = FastAPI(title="ALCF Inference Service", lifespan=lifespan)

app.middleware("http")(log_request)
app.include_router(routers.anon)
app.include_router(routers.auth)
app.include_router(routers.admin)


@app.exception_handler(FirstError)
def handle_app_error(_request: Request, exc: FirstError) -> JSONResponse:
    return JSONResponse(
        {"error": {"code": exc.code, "message": str(exc), "info": exc.info}},
        status_code=exc.status_code,
    )


@app.exception_handler(TaskPending)
def handle_pending(_request: Request, exc: TaskPending) -> JSONResponse:
    return JSONResponse(
        {"status": exc.code, "task_id": exc.task_id},
        status_code=exc.status_code,
        headers={"Retry-After": str(exc.retry_after)},
    )


@app.exception_handler(Exception)
def handle_uncaught_error(request: Request, exc: Exception) -> JSONResponse:
    error_id = uuid.uuid4().hex
    logger.exception(
        f"Uncaught Exception in API View {request.url.path!r}",
        extra={"error_id": error_id},
        exc_info=exc,
    )

    return JSONResponse(
        {
            "error": {
                "code": "internal_error",
                "message": "Internal Server Error",
                "error_id": error_id,
            }
        },
        status_code=500,
    )
