import httpx

from ..database.redis.router_config import RouterConfig


class BackendClientManager:
    """Maintains one httpx.AsyncClient per healthy deployment backend.

    Clients are created on the first RouterConfig swap that introduces a
    backend, and closed when the backend disappears from the config. Reusing
    clients across requests amortizes mTLS handshake and connection-pool costs.
    """

    def __init__(self) -> None:
        self._clients: dict[str, httpx.AsyncClient] = {}

    def get(self, backend_id: str) -> httpx.AsyncClient | None:
        return self._clients.get(backend_id)

    async def on_config_swap(self, cfg: RouterConfig) -> None:
        cfg_backend_ids = {
            backend.id
            for model in cfg.models
            for deployment in model.deployments
            for backend in deployment.backends
        }

        current_ids = set(self._clients)

        # New backends
        for backend_id in cfg_backend_ids - current_ids:
            self._clients[backend_id] = httpx.AsyncClient()

        # Deprecated backends
        for backend_id in current_ids - cfg_backend_ids:
            client = self._clients.pop(backend_id)
            await client.aclose()

    async def close_all(self) -> None:
        for client in list(self._clients.values()):
            await client.aclose()
        self._clients.clear()

    @property
    def clients(self) -> dict[str, httpx.AsyncClient]:
        return self._clients
