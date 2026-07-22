from typing import Annotated, AsyncGenerator, cast

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import (
    AsyncSession,
)

from first_common.schema.auth import UserAuthEvent
from first_common.schema.resources.spec import AccessGroupSpec

from ..database.redis.pubsub import RedisPubSub as _RedisPubSub
from ..database.redis.repo import RedisRepo as _RedisRepo
from ..database.redis.router_config import RouterConfig as _RouterConfig
from ..settings import ClientState
from .auth import GlobusAuthService, enforce_permission
from .router_config_manager import RouterConfigManager


async def get_state(request: Request) -> ClientState:
    return cast(ClientState, request.app.state.client_state)


async def get_router_config(request: Request) -> _RouterConfig:
    """Return the current hot-swapped RouterConfig snapshot.

    The reference is captured once per request; a swap mid-request rebinds the
    manager's attribute but leaves this instance intact for the caller.
    """
    manager = cast(RouterConfigManager, request.app.state.router_config_manager)
    return manager.current


AppState = Annotated[ClientState, Depends(get_state)]


async def get_session(state: AppState) -> AsyncGenerator[AsyncSession, None]:
    """
    Yields a "commit-as-you-go" AsyncSession.  Use sess.begin() or sess.commit()
    to manage transactions explicitly.
    """
    async with state.db_sessionmaker() as sess:
        yield sess


async def get_redis_repo(state: AppState) -> _RedisRepo:
    return state.redis_repo


async def get_redis_pubsub(state: AppState) -> _RedisPubSub:
    return state.redis_pubsub


async def get_auth_user(
    state: AppState,
    token: HTTPAuthorizationCredentials = Depends(HTTPBearer()),
) -> UserAuthEvent:
    """
    Returns UserAuthEvent if and only if the user is authenticated. Raises Unauthorized otherwise.
    """
    auth_svc = GlobusAuthService(state)
    user = await auth_svc.validate_access_token(token)
    return user


async def get_admin_user(
    state: AppState, user: UserAuthEvent = Depends(get_auth_user)
) -> UserAuthEvent:
    """
    Returns UserAuthEvent if and only if the user is authenticated and is a
    member of `settings.globus.admin_group`.  Raises AccessDenied otherwise.
    """
    settings = state.settings
    enforce_permission(
        user, AccessGroupSpec(allowed_groups=[settings.globus.admin_group])
    )
    return user


async def is_user_admin(
    state: AppState, user: UserAuthEvent = Depends(get_auth_user)
) -> bool:
    """Returns True if the user belongs to the admin group"""
    admin_group = state.settings.globus.admin_group
    return admin_group in user.user_group_uuids


BearerCredentials = Annotated[HTTPAuthorizationCredentials, Depends(HTTPBearer())]
DbSession = Annotated[AsyncSession, Depends(get_session)]
RedisRepo = Annotated[_RedisRepo, Depends(get_redis_repo)]
RedisPubSub = Annotated[_RedisPubSub, Depends(get_redis_pubsub)]
RouterConfigDep = Annotated[_RouterConfig, Depends(get_router_config)]
AuthUser = Annotated[UserAuthEvent, Depends(get_auth_user)]
AdminUser = Annotated[UserAuthEvent, Depends(get_admin_user)]
IsUserAdmin = Annotated[bool, Depends(is_user_admin)]
