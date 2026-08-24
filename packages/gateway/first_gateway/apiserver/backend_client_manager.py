import asyncio
import logging
import socket
import tempfile
from pathlib import Path
from ssl import SSLContext, create_default_context
from uuid import uuid4

import httpx

from ..database.redis.router_config import BackendConfig, DeploymentConfig, RouterConfig
from ..services.certmanager import generate_client_cert
from ..settings import Settings

logger = logging.getLogger(__name__)

# To accomodate different OS platform (MacOS/Linux)
if hasattr(socket, "TCP_KEEPALIVE"):
    TCP_KEEPIDLE_OR_KEEPALIVE = socket.TCP_KEEPALIVE
else:
    TCP_KEEPIDLE_OR_KEEPALIVE = socket.TCP_KEEPIDLE

SOCKET_OPTS = [
    (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
    (socket.IPPROTO_TCP, TCP_KEEPIDLE_OR_KEEPALIVE, 60),
    (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 15),
    (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 4),
]

STREAM_TIMEOUT = httpx.Timeout(connect=5.0, read=120.0, write=60.0, pool=5.0)
UNARY_TIMEOUT = httpx.Timeout(connect=5.0, read=900.0, write=60.0, pool=5.0)

KEEPALIVE_EXPIRY = 30.0


class BackendClientManager:
    """Maintains one httpx.AsyncClient per healthy deployment backend.

    Clients are created on the first RouterConfig swap that introduces a
    backend, and closed when the backend disappears from the config. Reusing
    clients across requests amortizes mTLS handshake and connection-pool costs.
    """

    def __init__(self, settings: Settings) -> None:
        self._clients: dict[str, httpx.AsyncClient] = {}
        self._configs: dict[str, tuple[BackendConfig, DeploymentConfig]] = {}
        self._ctx: SSLContext = self._get_ssl_context(settings)

    def _get_ssl_context(self, settings: Settings) -> SSLContext:
        ctx = create_default_context(cadata=settings.pilot_ca_crt)
        ctx.check_hostname = False
        client_crt_pem, client_key_pem = generate_client_cert(
            cn=str(uuid4()),
            ca_cert_pem=settings.pilot_ca_crt,
            ca_key_pem=settings.pilot_ca_key.get_secret_value(),
        )
        with tempfile.TemporaryDirectory(delete=True) as tmpdir:
            crt_path = Path(tmpdir) / "client.crt"
            key_path = Path(tmpdir) / "client.key"
            crt_path.write_text(client_crt_pem)
            key_path.write_text(client_key_pem)
            ctx.load_cert_chain(crt_path, key_path)

        return ctx

    def get(self, backend_id: str) -> httpx.AsyncClient | None:
        return self._clients.get(backend_id)

    async def on_config_swap(self, cfg: RouterConfig) -> None:
        incoming: dict[str, tuple[BackendConfig, DeploymentConfig]] = {
            backend.id: (backend, deployment)
            for model in cfg.models
            for deployment in model.deployments
            for backend in deployment.backends
        }

        # Backends that should be added or updated
        for backend_id, (backend, deployment) in incoming.items():
            if self._should_recreate(backend_id, backend, deployment):
                logger.info(f"Closing client for {backend_id}: configuration updated")
                await self._close_client(backend_id, sleep=0)
            if backend_id not in self._clients:
                self._clients[backend_id] = self._create_client(backend, deployment)
                self._configs[backend_id] = (backend, deployment)

        # Backends that should be removed
        for backend_id in set(self._clients) - incoming.keys():
            await self._close_client(backend_id, sleep=60)

    def _should_recreate(
        self, backend_id: str, backend: BackendConfig, deployment: DeploymentConfig
    ) -> bool:
        """Return True if backend in self._clients and in need of update."""
        if backend_id in self._clients:
            current_backend, current_deployment = self._configs[backend_id]
            if (
                current_backend.api_key != backend.api_key
                or current_backend.model_url != backend.model_url
                or current_deployment.kind != deployment.kind
                or current_deployment.router_params.max_backend_concurrency
                != deployment.router_params.max_backend_concurrency
            ):
                return True
        return False

    async def _close_client(self, backend_id: str, sleep: int = 0) -> None:
        await asyncio.sleep(sleep)
        client = self._clients.pop(backend_id)
        self._configs.pop(backend_id)
        if client:
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
            headers["Authorization"] = f"Bearer {backend.api_key}"

        max_concurrency = deployment.router_params.max_backend_concurrency
        limits = httpx.Limits(
            max_connections=max_concurrency,
            max_keepalive_connections=max_concurrency,
            keepalive_expiry=KEEPALIVE_EXPIRY,
        )

        verify = self._ctx if deployment.kind == "pilot" else True
        transport = httpx.AsyncHTTPTransport(
            verify=verify,
            retries=2,
            limits=limits,
            http2=False,
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
