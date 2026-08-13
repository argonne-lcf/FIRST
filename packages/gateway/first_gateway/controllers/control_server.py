"""Localhost-only control server run by the ControllerManager.

Serves operational endpoints (`/metrics`, `/healthz`, `/controllers`) alongside
the control plane (`/control/v1/*`) and Prometheus service discovery
(`/discovery/v1/prometheus`).  These routes are deliberately NOT mounted on the
user-facing apiserver: this server is host-published on 127.0.0.1 only and is
otherwise reachable solely within the compose network, so the control API is
unreachable from outside.  Globus admin authentication is enforced on
`/control/v1/*` as defense-in-depth.

Started as an asyncio task inside the ControllerManager.
"""

import logging
from time import monotonic
from typing import Any

import uvicorn
from fastapi import Depends, FastAPI, Response
from prometheus_client import make_asgi_app

from ..apiserver.dependencies import get_admin_user
from ..apiserver.error_handlers import register_error_handlers
from ..apiserver.routes import control, discovery
from ..settings import ClientState
from .worker import Worker

logger = logging.getLogger(__name__)

CONTROL_HOST = "0.0.0.0"
CONTROL_PORT = 9100

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
app.mount("/metrics", make_asgi_app())

_workers: list[Worker] = []


app.include_router(discovery.router)
app.include_router(control.admin_router, dependencies=[Depends(get_admin_user)])

register_error_handlers(app)


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


async def serve(workers: list[Worker], client_state: ClientState) -> None:
    _workers.clear()
    _workers.extend(workers)
    app.state.client_state = client_state

    config = uvicorn.Config(
        app,
        host=CONTROL_HOST,
        port=CONTROL_PORT,
        log_level="warning",
    )
    server = uvicorn.Server(config)
    server.install_signal_handlers = lambda: None  # type: ignore[attr-defined]
    logger.info("Starting control server")
    await server.serve()
