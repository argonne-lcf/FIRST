"""Localhost-only control server run by the ControllerManager: the admin API.

Serves operational endpoints (`/metrics`, `/healthz`, `/controllers`) alongside
the control plane (`/control/v1/*`), the admin catalog (`/catalog/v1/*`: every
resource in full), Prometheus service discovery (`/discovery/v1/prometheus`)
and the admin console (`/admin-console/`).  These routes are deliberately NOT
mounted on the user-facing apiserver, which serves only the public catalog
views: this server is host-published on 127.0.0.1 only and is otherwise
reachable solely within the compose network, so the admin API is unreachable
from outside.  Globus admin authentication is enforced on `/control/v1/*`,
`/catalog/v1/*` and `/whoami` as defense-in-depth.

Being unreachable from outside, it publishes its full OpenAPI schema
(`/openapi.json`, `/docs`); the admin console SDK is generated from it.

Started as an asyncio task inside the ControllerManager.
"""

import logging
from pathlib import Path
from time import monotonic
from typing import Any

import uvicorn
from fastapi import Depends, FastAPI, Response
from prometheus_client import make_asgi_app
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.staticfiles import StaticFiles
from starlette.types import Scope

from first_common.schema.auth import UserAuthEvent

from ..apiserver.dependencies import AdminUser, get_admin_user
from ..apiserver.error_handlers import register_error_handlers
from ..services.pilot_control import PilotControlClient
from ..settings import ClientState
from .routes import catalog, control, discovery
from .worker import Worker

logger = logging.getLogger(__name__)

CONTROL_HOST = "0.0.0.0"
CONTROL_PORT = 9100

ADMIN_CONSOLE_PATH = "/admin-console"

app = FastAPI(title="ALCF Inference Service: Admin API")
app.mount("/metrics", make_asgi_app())

_workers: list[Worker] = []


app.include_router(discovery.router)
app.include_router(control.admin_router, dependencies=[Depends(get_admin_user)])
app.include_router(catalog.router, dependencies=[Depends(get_admin_user)])

register_error_handlers(app)


@app.get("/whoami", response_model=UserAuthEvent)
async def whoami(user: AdminUser) -> UserAuthEvent:
    """Return the authenticated admin's identity."""
    return user


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


class _SinglePageApp(StaticFiles):
    """
    Serves a built SPA: a path that is not a file falls back to index.html, so
    deep links and the OAuth landing at /admin-console/callback?code=... work
    on hard loads.  Only the content-hashed assets may be cached.
    """

    async def get_response(self, path: str, scope: Scope) -> Response:
        try:
            response = await super().get_response(path, scope)
        except StarletteHTTPException as exc:
            if exc.status_code != 404:
                raise
            response = await super().get_response("index.html", scope)
        if not path.startswith("assets/"):
            response.headers["Cache-Control"] = "no-store"
        return response


def mount_admin_console(directory: Path | None) -> bool:
    """Serve the built admin console from `directory`, if it holds a build."""
    if any(getattr(r, "path", None) == ADMIN_CONSOLE_PATH for r in app.routes):
        return True
    if directory is None or not (directory / "index.html").is_file():
        return False
    app.mount(ADMIN_CONSOLE_PATH, _SinglePageApp(directory=directory, html=True))
    return True


async def serve(workers: list[Worker], client_state: ClientState) -> None:
    _workers.clear()
    _workers.extend(workers)
    app.state.client_state = client_state
    app.state.pilot_control_client = PilotControlClient(client_state)

    console_dir = client_state.settings.admin_console_dir
    if mount_admin_console(console_dir):
        logger.info("Serving the admin console from %s", console_dir)
    elif console_dir is not None:
        logger.warning("No admin console build (index.html) in %s", console_dir)

    config = uvicorn.Config(
        app,
        host=CONTROL_HOST,
        port=CONTROL_PORT,
        log_level="warning",
    )
    server = uvicorn.Server(config)
    server.install_signal_handlers = lambda: None  # type: ignore[attr-defined]
    logger.info("Starting control server")
    try:
        await server.serve()
    finally:
        await app.state.pilot_control_client.aclose()
