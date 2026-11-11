import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI

from first_gateway.settings import Settings

from ..database.redis.admission import AdmissionController
from ..log_config import config_logging, drain_logs
from ..services.pilot_control import PilotControlClient
from .backend_client_manager import BackendClientManager
from .error_handlers import register_error_handlers
from .log_middleware import ResponseLogMiddleware
from .router_config_manager import RouterConfigManager
from .routes import routers

logger = logging.getLogger(__name__)

# Host-run (outside containers) dotenv layering for the gateway.
# In Compose, the service's `env_file` alone decides what it gets.
GATEWAY_ENV_FILES = (".env.default", ".env.local", ".env.secret.gateway")


def assert_gateway_secret_scope(settings: Settings) -> None:
    """
    Refuse to run the internet-facing gateway with a credential that can
    submit work to a cluster.
    """
    present = []
    if settings.globus.compute_client_secret is not None:
        present.append("FIRST_GLOBUS__COMPUTE_CLIENT_SECRET")
    if settings.keycloak_clients:
        present.append(
            f"FIRST_KEYCLOAK_CLIENTS ({', '.join(settings.keycloak_clients)})"
        )
    if present:
        raise RuntimeError(
            "Refusing to start the gateway: it must not hold cluster credentials, "
            f"but these are configured: {'; '.join(present)}. They belong to "
            "controller-manager only (.env.secret.controller); see "
            "docs/deployment/docker.md."
        )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Initializes ClientState and stashes it on app.state so request handlers
    can reach it via request.app.state.client_state.
    """
    settings = Settings(_env_file=GATEWAY_ENV_FILES)
    assert_gateway_secret_scope(settings)
    config_logging(settings.log_level, settings.log_levels)

    settings.prompt_storage_dir.mkdir(parents=True, exist_ok=True)
    if not os.access(settings.prompt_storage_dir, os.W_OK | os.X_OK):
        raise PermissionError(
            f"prompt_storage_dir {settings.prompt_storage_dir.resolve()} is not writable"
        )

    async with settings.build_clients() as client_state:
        app.state.client_state = client_state
        backend_client_manager = BackendClientManager(client_state.settings)
        router_config_manager = RouterConfigManager(client_state.redis)
        router_config_manager.add_swap_callback(backend_client_manager.on_config_swap)
        await router_config_manager.start()
        admission_controller = AdmissionController(client=client_state.redis)
        await admission_controller.start()
        app.state.router_config_manager = router_config_manager
        app.state.backend_client_manager = backend_client_manager
        app.state.admission_controller = admission_controller
        app.state.pilot_control_client = PilotControlClient(client_state)
        try:
            yield
        finally:
            await admission_controller.stop()
            await router_config_manager.stop()
            await backend_client_manager.close_all()
            await app.state.pilot_control_client.aclose()
            drain_logs()


app = FastAPI(title="ALCF Inference Service", lifespan=lifespan)

app.add_middleware(ResponseLogMiddleware)
app.include_router(routers.anon)
app.include_router(routers.auth)
app.include_router(routers.admin)

register_error_handlers(app)
