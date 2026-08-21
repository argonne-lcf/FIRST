import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI

from first_gateway.settings import Settings

from ..log_config import config_logging
from .backend_client_manager import BackendClientManager
from .error_handlers import register_error_handlers
from .log_middleware import log_request
from .router_config_manager import RouterConfigManager
from .routes import routers

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Initializes ClientState and stashes it on app.state so request handlers
    can reach it via request.app.state.client_state.
    """
    settings = Settings()
    config_logging(settings.log_level)
    async with settings.build_clients() as client_state:
        app.state.client_state = client_state
        backend_client_manager = BackendClientManager()
        router_config_manager = RouterConfigManager(client_state.redis)
        router_config_manager.add_swap_callback(backend_client_manager.on_config_swap)
        await router_config_manager.start()
        app.state.router_config_manager = router_config_manager
        app.state.backend_client_manager = backend_client_manager
        try:
            yield
        finally:
            await router_config_manager.stop()
            await backend_client_manager.close_all()


app = FastAPI(title="ALCF Inference Service", lifespan=lifespan)

app.middleware("http")(log_request)
app.include_router(routers.anon)
app.include_router(routers.auth)
app.include_router(routers.admin)

register_error_handlers(app)
