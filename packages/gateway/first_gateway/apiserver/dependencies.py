import uuid
from typing import Annotated, AsyncGenerator, cast

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import (
    AsyncSession,
)

from first_common.schema.auth import UserAuthEvent
from first_common.schema.resources.spec import AccessGroupSpec
from first_common.schema.structured_logs import RequestLog

from ..database.redis.pubsub import RedisPubSub as _RedisPubSub
from ..database.redis.repo import RedisRepo as _RedisRepo
from ..database.redis.router_config import RouterConfig as _RouterConfig
from ..settings import ClientState
from .auth import GlobusAuthService, enforce_permission
from .context import get_request_id
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


async def get_redis_repo(state: AppState) -> _RedisRepo:
    return state.redis_repo


RedisRepo = Annotated[_RedisRepo, Depends(get_redis_repo)]


async def get_redis_pubsub(state: AppState) -> _RedisPubSub:
    return state.redis_pubsub


RedisPubSub = Annotated[_RedisPubSub, Depends(get_redis_pubsub)]


# Route prefixes of the inference routers (routes/inference.py, v1_compat.py).
# Only these requests have their bodies logged and spilled to disk when large.
_INFERENCE_PATH_PREFIXES = ("/federated/", "/deployments/", "/resource_server/")


async def get_auth_user(
    request: Request,
    state: AppState,
    token: HTTPAuthorizationCredentials = Depends(HTTPBearer()),
) -> UserAuthEvent:
    """
    Returns UserAuthEvent if and only if the user is authenticated. Raises Unauthorized otherwise.
    """
    if request.url.path.startswith(_INFERENCE_PATH_PREFIXES):
        raw_body = await request.body()  #  Cached on request._body
        storage_dir = state.settings.prompt_storage_dir
    else:
        raw_body, storage_dir = None, None
    # The fronting NGINX sets X-Real-IP to the client address it resolved from
    # trusted proxies only. X-Forwarded-For is not read: clients can set its
    # leading entries.
    origin_ip = request.headers.get("X-Real-IP")
    if not origin_ip and request.client is not None:
        origin_ip = request.client.host
    content_length = request.headers.get("Content-Length")

    auth_svc = GlobusAuthService(state)

    try:
        user = await auth_svc.validate_access_token(token)
    except Exception:
        RequestLog(
            request_id=get_request_id() or str(uuid.uuid4()),
            method=request.method,
            path=request.url.path,
            origin_ip=origin_ip,
            content_length=int(content_length) if content_length else None,
        ).emit()
        raise

    RequestLog(
        request_id=get_request_id() or str(uuid.uuid4()),
        user_id=user.id,
        user_name=user.name,
        username=user.username,
        user_group_uuids=user.user_group_uuids,
        authorized_group_uuids=user.authorized_group_uuids,
        idp_id=user.idp_id,
        idp_name=user.idp_name,
        auth_service=user.auth_service,
        method=request.method,
        path=request.url.path,
        origin_ip=origin_ip,
        content_length=int(content_length) if content_length else None,
    ).emit(raw_body=raw_body, storage_dir=storage_dir)

    return user


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
        user,
        AccessGroupSpec(
            allowed_groups=[settings.globus.admin_group], allowed_domains=[]
        ),
    )
    return user


AdminUser = Annotated[UserAuthEvent, Depends(get_admin_user)]
