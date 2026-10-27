"""
Client helpers for the pilot manager control API (mTLS).

The gateway reaches each running pilot job's manager over mutual TLS to place,
inspect, and stop replicas. This module centralizes the mTLS client
construction and request wrappers.
"""

import asyncio
import logging
import socket
import ssl
from pathlib import Path

import httpx

from first_common.schema.pilot import (
    PilotJobStatus,
    ReplicaStartRequest,
    pilot_server_name,
)

from ..settings import ClientState, Settings

logger = logging.getLogger(__name__)

# fail fast on connect, but allow start-replica room to do its synchronous
# on-node work before responding.
DEFAULT_TIMEOUT = httpx.Timeout(connect=5.0, read=30.0, write=10.0, pool=5.0)
# Monitor join (15s), a configured pre-stop hook (maximum 80s), hook cleanup
# (2s), model TERM/KILL fallback (13s), post-stop verification (maximum 50s),
# and its cleanup (2s) total 162s, leaving 18s inside this read deadline.
STOP_TIMEOUT = httpx.Timeout(connect=5.0, read=180.0, write=10.0, pool=5.0)
STATUS_TIMEOUT = httpx.Timeout(connect=5.0, read=5.0, write=3.0, pool=5.0)

# Short retry to ride out ephemeral hiccups (a dropped connection, a manager
# still binding its port, a momentary 503). A handful of quick attempts only;
# anything that persists past them bubbles up to the reconcile cooldown pathway.
_RETRY_ATTEMPTS = 3
_RETRY_BACKOFF = 0.25  # seconds; scaled by attempt number
_RETRYABLE_STATUS = frozenset({502, 503, 504})

# The drainer must stay alive across all existing stop retries: each attempt
# allows connect/read/write/pool = 5+180+10+5s, then retry backoff, with another
# 30s for reconcile bookkeeping. Other controllers keep their usual watchdog.
STOP_HEARTBEAT_TIMEOUT = (
    _RETRY_ATTEMPTS * 200.0 + _RETRY_BACKOFF * sum(range(_RETRY_ATTEMPTS)) + 30.0
)


class _PilotServerContext(ssl.SSLContext):
    """Verifies the peer as `server_name`, whatever hostname the caller passes."""

    server_name: str

    def wrap_socket(
        self,
        sock: socket.socket,
        server_side: bool = False,
        do_handshake_on_connect: bool = True,
        suppress_ragged_eofs: bool = True,
        server_hostname: str | bytes | None = None,  # noqa: ARG002 (pinned)
        session: ssl.SSLSession | None = None,
    ) -> ssl.SSLSocket:
        return super().wrap_socket(
            sock=sock,
            server_side=server_side,
            do_handshake_on_connect=do_handshake_on_connect,
            suppress_ragged_eofs=suppress_ragged_eofs,
            server_hostname=self.server_name,
            session=session,
        )

    def wrap_bio(
        self,
        incoming: ssl.MemoryBIO,
        outgoing: ssl.MemoryBIO,
        server_side: bool = False,
        server_hostname: str | bytes | None = None,  # noqa: ARG002 (pinned)
        session: ssl.SSLSession | None = None,
    ) -> ssl.SSLObject:
        return super().wrap_bio(
            incoming=incoming,
            outgoing=outgoing,
            server_side=server_side,
            server_hostname=self.server_name,
            session=session,
        )


# Keyed by cluster and the files loaded, since Settings is not hashable.
_contexts: dict[tuple[str, Path, Path, Path], _PilotServerContext] = {}


def pilot_ssl_context(settings: Settings, cluster_name: str) -> _PilotServerContext:
    """
    mTLS context for calling pilots on `cluster_name`: trusts the pilot CA,
    presents this service's mounted client cert (whose CN is its role on the
    pilot), and accepts only a server cert naming that cluster.

    Pilots are dialed by IP, but the peer is verified by its per-cluster name
    (pilot_server_name), not its address: every handshake on this context
    checks that name, whatever host the caller connected to.
    """
    key = (
        cluster_name,
        settings.pilot_ca_crt_file,
        settings.pilot_client_crt_file,
        settings.pilot_client_key_file,
    )
    if (ctx := _contexts.get(key)) is None:
        # PROTOCOL_TLS_CLIENT: check_hostname on, CERT_REQUIRED
        ctx = _PilotServerContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.server_name = pilot_server_name(cluster_name)
        ctx.load_verify_locations(cafile=settings.pilot_ca_crt_file)
        ctx.load_cert_chain(
            settings.pilot_client_crt_file, settings.pilot_client_key_file
        )
        _contexts[key] = ctx
    return ctx


class PilotControlClient:
    def __init__(
        self,
        client_state: ClientState,
        *,
        timeout: httpx.Timeout | float = DEFAULT_TIMEOUT,
    ) -> None:
        """
        Authenticates to pilots over mTLS, with one httpx client per cluster
        (each verifies the pilot as that cluster's server name).
        """
        self._settings = client_state.settings
        self._timeout = timeout
        self._clients: dict[str, httpx.AsyncClient] = {}

    def _client_for(self, cluster_name: str, /) -> httpx.AsyncClient:
        if (client := self._clients.get(cluster_name)) is None:
            ctx = pilot_ssl_context(self._settings, cluster_name)
            client = httpx.AsyncClient(verify=ctx, timeout=self._timeout)
            self._clients[cluster_name] = client
        return client

    async def aclose(self) -> None:
        clients, self._clients = list(self._clients.values()), {}
        for client in clients:
            await client.aclose()

    async def _request(
        self,
        method: str,
        url: str,
        *,
        cluster_name: str,
        attempts: int = _RETRY_ATTEMPTS,
        **kwargs: object,
    ) -> httpx.Response:
        """
        Issue an mTLS request, retrying a few times on transport errors and
        transient 5xx gateway statuses. Non-retryable responses (2xx, 4xx, and a
        final-attempt 5xx) are returned as-is for the caller to interpret; a
        transport error that never resolves is re-raised after the last attempt.

        Only the first failed attempt is logged, at DEBUG: the caller knows the
        context (maintenance, its own retry count) and picks the log level for
        the final outcome.
        """
        client = self._client_for(cluster_name)
        last_exc: httpx.TransportError | None = None
        for attempt in range(attempts):
            if attempt:
                await asyncio.sleep(_RETRY_BACKOFF * attempt)
            try:
                resp = await client.request(method, url, **kwargs)  # type: ignore[arg-type]
            except httpx.TransportError as exc:
                last_exc = exc
                if attempt == 0:
                    logger.debug(
                        "pilot control %s %s attempt 1/%d failed: %r",
                        method,
                        url,
                        attempts,
                        exc,
                    )
                continue
            if resp.status_code in _RETRYABLE_STATUS and attempt < attempts - 1:
                if attempt == 0:
                    logger.debug(
                        "pilot control %s %s attempt 1/%d returned %d; retrying",
                        method,
                        url,
                        attempts,
                        resp.status_code,
                    )
                continue
            return resp

        assert last_exc is not None
        raise last_exc

    async def get_status(
        self, manager_url: str, *, cluster_name: str
    ) -> PilotJobStatus:
        # One attempt: callers re-poll /status on their own schedule and back
        # off per job, so retries here only stretch a pass over dead managers.
        resp = await self._request(
            "GET",
            f"{manager_url}/status",
            cluster_name=cluster_name,
            attempts=1,
            timeout=STATUS_TIMEOUT,
        )
        resp.raise_for_status()
        return PilotJobStatus.model_validate(resp.json())

    async def start_replica(
        self,
        manager_url: str,
        request: ReplicaStartRequest,
        *,
        cluster_name: str,
    ) -> httpx.Response:
        """
        POST /start-replica. Returns the raw response so the caller can distinguish
        outcomes (e.g. treating 409 CONFLICT as "already placed").
        """
        return await self._request(
            "POST",
            f"{manager_url}/start-replica",
            cluster_name=cluster_name,
            json=request.model_dump(mode="json"),
        )

    async def stop_replica(
        self,
        manager_url: str,
        replica_name: str,
        *,
        cluster_name: str,
    ) -> httpx.Response:
        """POST /stop-replica/{name}. Returns the raw response (404 is tolerable)."""
        return await self._request(
            "POST",
            f"{manager_url}/stop-replica/{replica_name}",
            cluster_name=cluster_name,
            timeout=STOP_TIMEOUT,
        )

    async def get_logs(
        self, manager_url: str, replica_name: str, *, cluster_name: str
    ) -> str:
        resp = await self._request(
            "GET", f"{manager_url}/logs/{replica_name}", cluster_name=cluster_name
        )
        resp.raise_for_status()
        return str(resp.json())
