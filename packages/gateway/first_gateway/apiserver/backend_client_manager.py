import asyncio
import ipaddress
import logging
import os
import socket
import urllib.request
from collections.abc import Coroutine
from ssl import SSLContext
from typing import Any

import aiohttp
import aiohttp_socks
from yarl import URL

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


def _in_no_proxy_network(host: str) -> bool:
    """True if `host` is an IP address inside a CIDR entry of NO_PROXY.

    urllib's NO_PROXY matching is suffix-only, so it cannot exempt a subnet of
    compute nodes addressed by IP (e.g. ``10.124.0.0/16``).
    """
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        return False
    no_proxy = os.environ.get("NO_PROXY") or os.environ.get("no_proxy") or ""
    for entry in no_proxy.split(","):
        if "/" not in entry:
            continue
        try:
            if addr in ipaddress.ip_network(entry.strip(), strict=False):
                return True
        except ValueError:
            continue
    return False


def _egress_proxy(url: str) -> str | None:
    """The env-configured egress proxy for `url`, or None if NO_PROXY exempts it.

    The backend connector is given its proxy explicitly, so NO_PROXY is applied
    here.  It may also list CIDR networks, matched against IP-address hosts.
    """
    proxy = os.environ.get("ALL_PROXY") or os.environ.get("HTTPS_PROXY")
    if not proxy:
        return None
    host = URL(url).host or ""
    if urllib.request.proxy_bypass(host) or _in_no_proxy_network(host):
        return None
    return proxy


# To accomodate different OS platform (MacOS/Linux)
TCP_KEEPIDLE_OR_KEEPALIVE: int = getattr(socket, "TCP_KEEPALIVE", None) or getattr(
    socket, "TCP_KEEPIDLE"
)

# aiohttp has no write timeout: its read timer starts once the request body is
# written.  On Linux, TCP_USER_TIMEOUT bounds how long sent data may stay
# unacknowledged, so a peer that stops reading a large prompt fails the request
# after this long instead of after the kernel's retransmission limit (~15 min).
# A live peer acknowledges promptly, including keepalive probes during a silent
# prefill, so it never trips.  (macOS has no equivalent; tests run there.)
WRITE_STALL_TIMEOUT_MS = 60_000

SOCKET_OPTS = [
    (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
    (socket.IPPROTO_TCP, TCP_KEEPIDLE_OR_KEEPALIVE, 60),
    (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 15),
    (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 4),
]
if hasattr(socket, "TCP_USER_TIMEOUT"):
    SOCKET_OPTS.append(
        (socket.IPPROTO_TCP, socket.TCP_USER_TIMEOUT, WRITE_STALL_TIMEOUT_MS)
    )


def _keepalive_socket(addr_info: Any) -> socket.socket:
    """Socket factory for direct backend connections: TCP keepalive on, and a
    bound on unacknowledged writes where the OS supports it."""
    family, type_, proto, _, _ = addr_info
    sock = socket.socket(family=family, type=type_, proto=proto)
    for level, option, value in SOCKET_OPTS:
        sock.setsockopt(level, option, value)
    return sock


# `sock_read` is a per-read inactivity limit, not an end-to-end deadline: long
# prefills can be silent before the first SSE chunk, and a unary completion
# sends nothing until it is done.  `connect` also covers the wait for a free
# connection and the TLS handshake.
BACKEND_TIMEOUT = aiohttp.ClientTimeout(
    total=None, connect=10.0, sock_connect=5.0, sock_read=900.0
)

KEEPALIVE_EXPIRY = 30.0

# Extra attempts when no connection to the backend could be established (the
# request was never sent, so repeating it is safe), and the pause before each.
CONNECT_RETRIES = 2
CONNECT_RETRY_DELAY_SEC = 0.5

# How long a retired client stays open for requests that already hold it
CLOSE_GRACE_SEC = 60

# What a backend call can raise: connection, protocol and timeout failures,
# including those of the egress proxy.  HTTP error statuses are not raised.
BACKEND_ERRORS: tuple[type[Exception], ...] = (
    aiohttp.ClientError,
    asyncio.TimeoutError,
    OSError,
    aiohttp_socks.ProxyError,
    aiohttp_socks.ProxyConnectionError,
    aiohttp_socks.ProxyTimeoutError,
)

_CONNECT_ERRORS: tuple[type[Exception], ...] = (
    aiohttp.ClientConnectorError,
    aiohttp.ConnectionTimeoutError,
    aiohttp_socks.ProxyError,
    aiohttp_socks.ProxyConnectionError,
    aiohttp_socks.ProxyTimeoutError,
)


def is_read_timeout(exc: BaseException) -> bool:
    """True if the backend accepted the request and then went silent."""
    return isinstance(exc, aiohttp.SocketTimeoutError)


def _connector(
    limit: int, ssl: SSLContext | bool, proxy: str | None
) -> aiohttp.TCPConnector:
    if proxy is None:
        return aiohttp.TCPConnector(
            limit=limit,
            limit_per_host=0,
            keepalive_timeout=KEEPALIVE_EXPIRY,
            ssl=ssl,
            socket_factory=_keepalive_socket,
        )
    # python-socks has no "socks5h" scheme: it is socks5 with the name resolved
    # by the proxy.
    remote_dns = proxy.startswith("socks5h://")
    return aiohttp_socks.ProxyConnector.from_url(
        proxy.replace("socks5h://", "socks5://", 1),
        rdns=True if remote_dns else None,
        limit=limit,
        limit_per_host=0,
        keepalive_timeout=KEEPALIVE_EXPIRY,
        ssl=ssl,
    )


class BackendClient:
    """HTTP client for one backend: a connection pool and its fixed headers.

    Uses aiohttp, whose pool takes and returns a connection in constant time
    and reads the response with little per-chunk overhead.  (httpx/httpcore
    rescanned every connection on each acquire and release, which dominated
    gateway CPU at a few hundred requests in flight.)
    """

    def __init__(
        self,
        base_url: str,
        *,
        headers: dict[str, str] | None = None,
        max_connections: int = 100,
        ssl: SSLContext | bool = True,
        proxy: str | None = None,
        timeout: aiohttp.ClientTimeout = BACKEND_TIMEOUT,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._session = aiohttp.ClientSession(
            connector=_connector(max_connections, ssl, proxy),
            headers={"Accept-Encoding": "identity", **(headers or {})},
            timeout=timeout,
        )

    @property
    def is_closed(self) -> bool:
        return self._session.closed

    async def post(
        self, path: str, body: bytes, *, headers: dict[str, str] | None = None
    ) -> aiohttp.ClientResponse:
        """POST a JSON body to ``base_url + path``; returns at the response
        headers.  The caller reads the body and then releases or closes the
        response.  Raises one of BACKEND_ERRORS.
        """
        url = self.base_url + path
        request_headers = {"Content-Type": "application/json", **(headers or {})}
        for attempt in range(CONNECT_RETRIES + 1):
            try:
                return await self._session.post(url, data=body, headers=request_headers)
            except _CONNECT_ERRORS:
                if attempt == CONNECT_RETRIES:
                    raise
                await asyncio.sleep(CONNECT_RETRY_DELAY_SEC * attempt)
        raise AssertionError("unreachable")

    async def aclose(self) -> None:
        await self._session.close()


class BackendClientManager:
    """Maintains one BackendClient per healthy deployment backend.

    Clients are created on the first RouterConfig swap that introduces a
    backend, and closed when the backend disappears from the config. Reusing
    clients across requests amortizes mTLS handshake and connection-pool costs.
    """

    def __init__(self, settings: Settings) -> None:
        self._clients: dict[str, BackendClient] = {}
        self._configs: dict[str, tuple[BackendConfig, DeploymentConfig]] = {}
        self._settings = settings

    def get(self, backend_id: str) -> BackendClient | None:
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
        client: BackendClient | None

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
                or current_deployment.cluster_name != deployment.cluster_name
                or current_deployment.router_params.max_backend_concurrency
                != deployment.router_params.max_backend_concurrency
            ):
                return True
        return False

    async def _drain_client(self, client: BackendClient) -> None:
        await asyncio.sleep(CLOSE_GRACE_SEC)
        await client.aclose()

    async def close_all(self) -> None:
        for client in self._clients.values():
            await client.aclose()
        self._clients.clear()
        self._configs.clear()

    def _create_client(
        self, backend: BackendConfig, deployment: DeploymentConfig
    ) -> BackendClient:
        headers: dict[str, str] = {}
        if backend.api_key:
            resolved = SecretRef(backend.api_key).resolve().get_secret_value()
            headers["Authorization"] = f"Bearer {resolved}"

        verify: SSLContext | bool = True
        if deployment.kind == "pilot":
            verify = pilot_ssl_context(self._settings, deployment.cluster_name)
        elif backend.mtls:
            verify = backend.mtls.ssl_context()

        return BackendClient(
            backend.model_url,
            headers=headers,
            max_connections=deployment.router_params.max_backend_concurrency,
            ssl=verify,
            proxy=_egress_proxy(backend.model_url),
        )

    @property
    def clients(self) -> dict[str, BackendClient]:
        return self._clients
