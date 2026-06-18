from fastapi import APIRouter, Depends

from first_common.schema.auth import UserAuthEvent

from ..dependencies import AuthUser, get_admin_user, get_auth_user
from . import resources

# Allows public access:
anon = APIRouter()

# Requires authentication:
auth = APIRouter(dependencies=[Depends(get_auth_user)])

# Requires authentication and admin group membership:
admin = APIRouter(dependencies=[Depends(get_admin_user)])


@anon.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe"""
    return {"status": "ok"}


@auth.get("/whoami", response_model=UserAuthEvent)
async def whoami(user: AuthUser) -> UserAuthEvent:
    """Return the authenticated caller's identity."""
    return user


admin.include_router(resources.admin_router)
auth.include_router(resources.user_router)
