import ssl
import tempfile
from pathlib import Path

import httpx

from ..database.redis.router_config import BackendConfig, DeploymentConfig, RouterConfig
from ..services.certmanager import generate_client_cert
from ..settings import Settings


class BackendClientManager:
    """Maintains one httpx.AsyncClient per healthy deployment backend.

    Clients are created on the first RouterConfig swap that introduces a
    backend, and closed when the backend disappears from the config. Reusing
    clients across requests amortizes mTLS handshake and connection-pool costs.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._clients: dict[str, httpx.AsyncClient] = {}
        self._configs: dict[str, tuple[BackendConfig, DeploymentConfig]] = {}

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
            if backend_id in self._clients and (
                backend,
                deployment,
            ) != self._configs.get(backend_id):
                # Remove existing clients that require an update
                await self._close_backend(backend_id)
            if backend_id not in self._clients:
                self._clients[backend_id] = self._create_client(backend, deployment)
                self._configs[backend_id] = (backend, deployment)

        # Backends that should be removed
        for backend_id in set(self._clients) - incoming.keys():
            await self._close_backend(backend_id)

    async def _close_backend(self, backend_id: str) -> None:
        client = self._clients.pop(backend_id)
        self._configs.pop(backend_id)
        await client.aclose()

    async def close_all(self) -> None:
        for client in list(self._clients.values()):
            await client.aclose()
        self._clients.clear()
        self._configs.clear()

    def _create_client(
        self, backend: BackendConfig, deployment: DeploymentConfig
    ) -> httpx.AsyncClient:
        headers = {}
        if backend.api_key:
            headers["Authorization"] = f"Bearer {backend.api_key}"

        if deployment.kind == "pilot":
            settings = self._settings
            ctx = ssl.create_default_context(cadata=settings.pilot_ca_crt)
            ctx.check_hostname = False
            client_crt_pem, client_key_pem = generate_client_cert(
                cn=backend.id,
                ca_cert_pem=settings.pilot_ca_crt,
                ca_key_pem=settings.pilot_ca_key.get_secret_value(),
            )
            with tempfile.TemporaryDirectory(delete=True) as tmpdir:
                crt_path = Path(tmpdir) / "client.crt"
                key_path = Path(tmpdir) / "client.key"
                crt_path.write_text(client_crt_pem)
                key_path.write_text(client_key_pem)
                ctx.load_cert_chain(crt_path, key_path)
            return httpx.AsyncClient(
                base_url=backend.model_url,
                headers=headers,
                verify=ctx,
            )

        return httpx.AsyncClient(
            base_url=backend.model_url,
            headers=headers,
        )

    @property
    def clients(self) -> dict[str, httpx.AsyncClient]:
        return self._clients
