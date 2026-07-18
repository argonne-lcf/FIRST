"""
Client helpers for the pilot manager control API (mTLS).

The gateway reaches each running pilot job's manager over mutual TLS to place,
inspect, and stop replicas. This module centralizes the mTLS client
construction and request wrappers.
"""

import ssl
import tempfile
from pathlib import Path

import httpx

from first_common.schema.pilot import PilotJobStatus, ReplicaStartRequest

from ..settings import ClientState
from .certmanager import generate_client_cert


class PilotControlClient:
    def __init__(
        self,
        client_state: ClientState,
        *,
        cn: str,
        timeout: float = 10.0,
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

    async def get_status(self, manager_url: str) -> PilotJobStatus:
        resp = await self._client.get(f"{manager_url}/status")
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
        return await self._client.post(
            f"{manager_url}/start-replica",
            json=request.model_dump(mode="json"),
        )

    async def stop_replica(
        self,
        manager_url: str,
        replica_name: str,
    ) -> httpx.Response:
        """POST /stop-replica/{name}. Returns the raw response (404 is tolerable)."""
        return await self._client.post(f"{manager_url}/stop-replica/{replica_name}")
