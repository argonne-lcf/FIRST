"""Small FastAPI server exposing /healthz, /metrics, and controller status.

Started as an asyncio task inside the ControllerManager so it lives
exactly as long as the manager process does.
"""

import logging
from time import monotonic
from typing import Any

import uvicorn
from fastapi import FastAPI, Response
from prometheus_client import make_asgi_app

from .worker import Worker

logger = logging.getLogger(__name__)

METRICS_HOST = "0.0.0.0"
METRICS_PORT = 9100

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
app.mount("/metrics", make_asgi_app())

_workers: list[Worker] = []


@app.get("/healthz")
async def healthz() -> Response:
    for w in _workers:
        status = w.check_heartbeat()
        if status.timed_out:
            return Response(content=f"worker {w.name} heartbeat stale", status_code=503)
    return Response(content="ok", status_code=200)


@app.get("/controllers")
async def controllers_list() -> list[dict[str, Any]]:
    now = monotonic()
    result: list[dict[str, Any]] = []
    for w in _workers:
        hb_status = w.check_heartbeat()
        result.append(
            {
                "name": w.name,
                "status": "running" if not hb_status.timed_out else "stale",
                "heartbeats": [
                    {"name": h.name, "since_last_s": round(now - h._last_beat, 2)}
                    for h in w._heartbeats
                ],
                "restart_count": int(_get_restart_count(w.name)),
            }
        )
    return result


def _get_restart_count(worker_name: str) -> float:
    from .worker import WORKER_RESTARTS

    try:
        return float(WORKER_RESTARTS.labels(worker_name)._value.get())
    except Exception:
        return 0


async def serve(workers: list[Worker]) -> None:
    _workers.clear()
    _workers.extend(workers)

    config = uvicorn.Config(
        app,
        host=METRICS_HOST,
        port=METRICS_PORT,
        log_level="warning",
    )
    server = uvicorn.Server(config)
    server.install_signal_handlers = lambda: None  # type: ignore[attr-defined]
    logger.info("Starting metrics server")
    await server.serve()
