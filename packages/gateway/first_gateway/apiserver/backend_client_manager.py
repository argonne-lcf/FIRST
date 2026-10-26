import asyncio
import logging
import os
import socket
from collections.abc import Coroutine
from ssl import SSLContext

import httpx

from first_common.schema.types import SecretRef

from ..database.redis.router_config import BackendConfig, DeploymentConfig, RouterConfig
from ..services.pilot_control import pilot_ssl_context
from ..settings import Settings

logger = logging.getLogger(__name__)

_background_tasks: set[asyncio.Task[None]] = set()


def _on_done(task: asyncio.Task[None]) -> None:
    _background_tasks.discard(task)
    if task.cancelled():
        return
    if exc := task.exception():
        logger.error("Background task failed", exc_info=exc)


def _run_background(coro: Coroutine[None, None, None]) -> None:
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_on_done)


# To accomodate different OS platform (MacOS/Linux)
TCP_KEEPIDLE_OR_KEEPALIVE: int = getattr(socket, "TCP_KEEPALIVE", None) or getattr(
    socket, "TCP_KEEPIDLE"
)

SOCKET_OPTS = [
    (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
    (socket.IPPROTO_TCP, TCP_KEEPIDLE_OR_KEEPALIVE, 60),
    (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 15),
    (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 4),
]

# Long prefills can be silent before the first SSE chunk. This is a per-read
# inactivity limit, not an end-to-end request deadline.
STREAM_TIMEOUT = httpx.Timeout(connect=5.0, read=900.0, write=60.0, pool=5.0)
UNARY_TIMEOUT = httpx.Timeout(connect=5.0, read=900.0, write=60.0, pool=5.0)

KEEPALIVE_EXPIRY = 30.0

# How long a retired client stays open for requests that already hold it
CLOSE_GRACE_SEC = 60


class BackendClientManager:
    """Maintains one httpx.AsyncClient per healthy deployment backend.

    Clients are created on the first RouterConfig swap that introduces a
    backend, and closed when the backend disappears from the config. Reusing
    clients across requests amortizes mTLS handshake and connection-pool costs.
    """

    def __init__(self, settings: Settings) -> None:
        self._clients: dict[str, httpx.AsyncClient] = {}
        self._configs: dict[str, tuple[BackendConfig, DeploymentConfig]] = {}
        self._ctx: SSLContext = pilot_ssl_context(settings)

    def get(self, backend_id: str) -> httpx.AsyncClient | None:
        return self._clients.get(backend_id)

    async def on_config_swap(self, cfg: RouterConfig) -> None:
        incoming: dict[str, tuple[BackendConfig, DeploymentConfig]] = {
            backend.id: (backend, deployment)
            for model in cfg.models
            for deployment in model.deployments
            for backend in deployment.backends
        }

        # Backends that should be added or updated. One bad backend (e.g. an
        # unresolvable api_key) must not block the others; a failed update
        # keeps serving with the previous client.
        failed: list[str] = []
        client: httpx.AsyncClient | None

        for backend_id, (backend, deployment) in incoming.items():
            recreate = self._should_recreate(backend_id, backend, deployment)
            if backend_id in self._clients and not recreate:
                continue
            try:
                client = self._create_client(backend, deployment)
            except Exception:
                logger.exception(f"Failed to create client for backend {backend_id}")
                failed.append(backend_id)
                continue
            if recreate:
                logger.info(f"Swap client for {backend_id}: config updated")
                _run_background(self._drain_client(self._clients[backend_id]))
            self._clients[backend_id] = client
            self._configs[backend_id] = (backend, deployment)

        # Backends that should be removed
        for backend_id in set(self._clients) - incoming.keys():
            logger.info(f"Removing backend {backend_id}")
            client = self._clients.pop(backend_id, None)
            self._configs.pop(backend_id, None)
            if client:
                _run_background(self._drain_client(client))

        if failed:
            raise RuntimeError(f"Failed to create clients for backends: {failed}")

    def _should_recreate(
        self, backend_id: str, backend: BackendConfig, deployment: DeploymentConfig
    ) -> bool:
        """Return True if backend in self._clients and in need of update."""
        if backend_id in self._clients:
            current_backend, current_deployment = self._configs[backend_id]
            if (
                current_backend.api_key != backend.api_key
                or current_backend.mtls != backend.mtls
                or current_backend.model_url != backend.model_url
                or current_deployment.kind != deployment.kind
                or current_deployment.router_params.max_backend_concurrency
                != deployment.router_params.max_backend_concurrency
            ):
                return True
        return False

    async def _drain_client(self, client: httpx.AsyncClient) -> None:
        await asyncio.sleep(CLOSE_GRACE_SEC)
        await client.aclose()

    async def close_all(self) -> None:
        for client in self._clients.values():
            await client.aclose()
        self._clients.clear()
        self._configs.clear()

    def _create_client(
        self, backend: BackendConfig, deployment: DeploymentConfig
    ) -> httpx.AsyncClient:
        headers = {"Accept-Encoding": "identity"}
        if backend.api_key:
            resolved = SecretRef(backend.api_key).resolve().get_secret_value()
            headers["Authorization"] = f"Bearer {resolved}"

        max_concurrency = deployment.router_params.max_backend_concurrency
        limits = httpx.Limits(
            max_connections=max_concurrency,
            max_keepalive_connections=max_concurrency,
            keepalive_expiry=KEEPALIVE_EXPIRY,
        )

        verify: SSLContext | bool = True
        if deployment.kind == "pilot":
            verify = self._ctx
        elif backend.mtls:
            verify = backend.mtls.ssl_context()
        transport = httpx.AsyncHTTPTransport(
            verify=verify,
            retries=2,
            limits=limits,
            http2=False,
            proxy=os.environ.get("ALL_PROXY") or os.environ.get("HTTPS_PROXY"),
            socket_options=SOCKET_OPTS,
        )

        return httpx.AsyncClient(
            base_url=backend.model_url,
            transport=transport,
            timeout=STREAM_TIMEOUT,
            headers=headers,
        )

    @property
    def clients(self) -> dict[str, httpx.AsyncClient]:
        return self._clients
