"""
Client helpers for the pilot manager control API (mTLS).

The gateway reaches each running pilot job's manager over mutual TLS to place,
inspect, and stop replicas. This module centralizes the mTLS client
construction and request wrappers.
"""

import asyncio
import logging
import ssl
import tempfile
from pathlib import Path

import httpx

from first_common.schema.pilot import PilotJobStatus, ReplicaStartRequest

from ..settings import ClientState
from .certmanager import generate_client_cert

logger = logging.getLogger(__name__)

# fail fast on connect, but allow start-replica room to do its synchronous
# on-node work before responding.
DEFAULT_TIMEOUT = httpx.Timeout(connect=5.0, read=30.0, write=10.0, pool=5.0)
# A configured pre-stop hook (maximum 25s), hook cleanup (2s), the pilot's
# TERM/KILL process-group fallback (13s), a post-stop verifier (maximum 50s),
# its cleanup (2s), and monitor join fit inside this bounded read deadline.
STOP_TIMEOUT = httpx.Timeout(connect=5.0, read=120.0, write=10.0, pool=5.0)
STATUS_TIMEOUT = httpx.Timeout(connect=5.0, read=5.0, write=3.0, pool=5.0)

# Short retry to ride out ephemeral hiccups (a dropped connection, a manager
# still binding its port, a momentary 503). A handful of quick attempts only;
# anything that persists past them bubbles up to the reconcile cooldown pathway.
_RETRY_ATTEMPTS = 3
_RETRY_BACKOFF = 0.25  # seconds; scaled by attempt number
_RETRYABLE_STATUS = frozenset({502, 503, 504})


class PilotControlClient:
    def __init__(
        self,
        client_state: ClientState,
        *,
        cn: str,
        timeout: httpx.Timeout | float = DEFAULT_TIMEOUT,
    ) -> None:
        """
        Build an httpx client configured with an mTLS client cert signed by the
        pilot CA. ``cn`` is the common name embedded in the client cert (used for
        logging/audit on the pilot side).
        """
        settings = client_state.settings

        ctx = ssl.create_default_context(cadata=settings.pilot_ca_crt)
        # Pilot server certs use the job name as CN and are reached by IP, so
        # hostname verification is intentionally relaxed (chain is still verified).
        ctx.check_hostname = False

        client_crt_pem, client_key_pem = generate_client_cert(
            cn=cn,
            ca_cert_pem=settings.pilot_ca_crt,
            ca_key_pem=settings.pilot_ca_key.get_secret_value(),
        )

        with tempfile.TemporaryDirectory(delete=True) as tmpdir:
            crt_path = Path(tmpdir) / "client.crt"
            key_path = Path(tmpdir) / "client.key"
            crt_path.write_text(client_crt_pem)
            key_path.write_text(client_key_pem)
            ctx.load_cert_chain(crt_path, key_path)

        self._client = httpx.AsyncClient(verify=ctx, timeout=timeout)

    async def _request(self, method: str, url: str, **kwargs: object) -> httpx.Response:
        """
        Issue an mTLS request, retrying a few times on transport errors and
        transient 5xx gateway statuses. Non-retryable responses (2xx, 4xx, and a
        final-attempt 5xx) are returned as-is for the caller to interpret; a
        transport error that never resolves is re-raised after the last attempt.
        """
        last_exc: httpx.TransportError | None = None
        for attempt in range(_RETRY_ATTEMPTS):
            if attempt:
                await asyncio.sleep(_RETRY_BACKOFF * attempt)
            try:
                resp = await self._client.request(method, url, **kwargs)  # type: ignore[arg-type]
            except httpx.TransportError as exc:
                last_exc = exc
                logger.warning(
                    "pilot control %s %s attempt %d/%d failed: %r",
                    method,
                    url,
                    attempt + 1,
                    _RETRY_ATTEMPTS,
                    exc,
                )
                continue
            if resp.status_code in _RETRYABLE_STATUS and attempt < _RETRY_ATTEMPTS - 1:
                logger.warning(
                    "pilot control %s %s attempt %d/%d returned %d; retrying",
                    method,
                    url,
                    attempt + 1,
                    _RETRY_ATTEMPTS,
                    resp.status_code,
                )
                continue
            return resp

        assert last_exc is not None
        raise last_exc

    async def get_status(self, manager_url: str) -> PilotJobStatus:
        resp = await self._request(
            "GET", f"{manager_url}/status", timeout=STATUS_TIMEOUT
        )
        resp.raise_for_status()
        return PilotJobStatus.model_validate(resp.json())

    async def start_replica(
        self,
        manager_url: str,
        request: ReplicaStartRequest,
    ) -> httpx.Response:
        """
        POST /start-replica. Returns the raw response so the caller can distinguish
        outcomes (e.g. treating 409 CONFLICT as "already placed").
        """
        return await self._request(
            "POST",
            f"{manager_url}/start-replica",
            json=request.model_dump(mode="json"),
        )

    async def stop_replica(
        self,
        manager_url: str,
        replica_name: str,
    ) -> httpx.Response:
        """POST /stop-replica/{name}. Returns the raw response (404 is tolerable)."""
        return await self._request(
            "POST",
            f"{manager_url}/stop-replica/{replica_name}",
            timeout=STOP_TIMEOUT,
        )

    async def get_logs(self, manager_url: str, replica_name: str) -> str:
        resp = await self._request("GET", f"{manager_url}/logs/{replica_name}")
        resp.raise_for_status()
        return str(resp.json())
