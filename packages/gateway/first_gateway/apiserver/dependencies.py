import uuid
from typing import Annotated, AsyncGenerator, cast

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from redis.asyncio import Redis as _AsyncRedis
from sqlalchemy.ext.asyncio import (
    AsyncSession,
)

from first_common.schema.auth import UserAuthEvent
from first_common.schema.resources.spec import AccessGroupSpec

from ..database.redis.admission import AdmissionController as _AdmissionController
from ..database.redis.pubsub import RedisPubSub as _RedisPubSub
from ..database.redis.repo import RedisRepo as _RedisRepo
from ..database.redis.router_config import RouterConfig as _RouterConfig
from ..settings import ClientState
from .auth import GlobusAuthService, enforce_permission
from .backend_client_manager import BackendClientManager as _BackendClientManager
from .router_config_manager import RouterConfigManager


async def get_state(request: Request) -> ClientState:
    return cast(ClientState, request.app.state.client_state)


AppState = Annotated[ClientState, Depends(get_state)]


async def get_router_config(request: Request) -> _RouterConfig:
    """Return the current hot-swapped RouterConfig snapshot.

    The reference is captured once per request; a swap mid-request rebinds the
    manager's attribute but leaves this instance intact for the caller.
    """
    manager = cast(RouterConfigManager, request.app.state.router_config_manager)
    return manager.current


RouterConfigDep = Annotated[_RouterConfig, Depends(get_router_config)]


async def get_session(state: AppState) -> AsyncGenerator[AsyncSession, None]:
    """
    Yields a "commit-as-you-go" AsyncSession.  Use sess.begin() or sess.commit()
    to manage transactions explicitly.
    """
    async with state.db_sessionmaker() as sess:
        yield sess


DbSession = Annotated[AsyncSession, Depends(get_session)]


async def get_redis(state: AppState) -> _AsyncRedis:
    return state.redis


RedisDep = Annotated[_AsyncRedis, Depends(get_redis)]


async def get_redis_repo(state: AppState) -> _RedisRepo:
    return state.redis_repo


RedisRepo = Annotated[_RedisRepo, Depends(get_redis_repo)]


async def get_redis_pubsub(state: AppState) -> _RedisPubSub:
    return state.redis_pubsub


RedisPubSub = Annotated[_RedisPubSub, Depends(get_redis_pubsub)]


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


BearerCredentials = Annotated[HTTPAuthorizationCredentials, Depends(HTTPBearer())]
AuthUser = Annotated[UserAuthEvent, Depends(get_auth_user)]


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


AdminUser = Annotated[UserAuthEvent, Depends(get_admin_user)]


async def is_user_admin(
    state: AppState, user: UserAuthEvent = Depends(get_auth_user)
) -> bool:
    """Returns True if the user belongs to the admin group"""
    admin_group = state.settings.globus.admin_group
    return admin_group in user.user_group_uuids


IsUserAdmin = Annotated[bool, Depends(is_user_admin)]


async def get_backend_client_manager(request: Request) -> _BackendClientManager:
    return cast(_BackendClientManager, request.app.state.backend_client_manager)


BackendClientManagerDep = Annotated[
    _BackendClientManager, Depends(get_backend_client_manager)
]


async def get_admission_controller(request: Request) -> _AdmissionController:
    return cast(_AdmissionController, request.app.state.admission_controller)


AdmissionControllerDep = Annotated[
    _AdmissionController, Depends(get_admission_controller)
]


def get_request_id() -> str:
    return str(uuid.uuid4())


RequestId = Annotated[str, Depends(get_request_id)]
