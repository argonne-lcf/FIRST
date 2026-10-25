from typing import cast

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from first_common.schema.auth import UserAuthEvent

from ..dependencies import AuthUser, get_admin_user, get_auth_user
from ..router_config_manager import RouterConfigManager
from . import catalog, inference, staging, v1_compat

# Allows public access:
anon = APIRouter()

# Requires authentication:
auth = APIRouter(dependencies=[Depends(get_auth_user)])

# Requires authentication and admin group membership:
admin = APIRouter(dependencies=[Depends(get_admin_user)])


@anon.get("/health")
async def health(request: Request) -> JSONResponse:
    """Health probe: 503 with a short public-safe reason when degraded."""
    manager = cast(RouterConfigManager, request.app.state.router_config_manager)
    if problem := manager.health_problem:
        return JSONResponse({"status": "degraded", "detail": problem}, status_code=503)
    return JSONResponse({"status": "ok"})


@auth.get("/whoami", response_model=UserAuthEvent)
async def whoami(user: AuthUser) -> UserAuthEvent:
    """Return the authenticated caller's identity."""
    return user


admin.include_router(catalog.admin_router)
auth.include_router(catalog.user_router)
auth.include_router(inference.router)
auth.include_router(staging.router)

# Temporary V1 (/resource_server) compatibility shim.
anon.include_router(v1_compat.anon_router)
auth.include_router(v1_compat.router)
