from typing import Annotated, AsyncGenerator

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from httpx import AsyncClient
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
)

from first_common.schema.auth import UserAuthEvent

from ..settings import ClientState
from ..settings import Settings as _Settings
from .auth import check_permission, validate_access_token


async def get_settings(request: Request[ClientState]) -> _Settings:
    return request.state["settings"]


async def get_httpx_client(request: Request[ClientState]) -> AsyncClient:
    return request.state["httpx_client"]


async def get_redis(request: Request[ClientState]) -> Redis:
    return request.state["redis"]


async def get_db_engine(request: Request[ClientState]) -> AsyncEngine:
    return request.state["db_engine"]


async def get_sessionmaker(
    request: Request[ClientState],
) -> async_sessionmaker[AsyncSession]:
    return request.state["db_sessionmaker"]


async def get_session(
    sessionmaker: async_sessionmaker[AsyncSession] = Depends(get_sessionmaker),
) -> AsyncGenerator[AsyncSession, None]:
    """
    Yields a "commit-as-you-go" AsyncSession.  Use sess.begin() or sess.commit()
    to manage transactions explicitly.
    """
    async with sessionmaker() as sess:
        yield sess


async def get_auth_user(
    request: Request[ClientState],
    token: HTTPAuthorizationCredentials = Depends(HTTPBearer()),
) -> UserAuthEvent:
    """
    Returns UserAuthEvent if and only if the user is authenticated. Raises Unauthorized otherwise.
    """
    user = await validate_access_token(token, request.state["redis"])
    return user


def get_admin_user(
    request: Request[ClientState], user: UserAuthEvent = Depends(get_auth_user)
) -> UserAuthEvent:
    """
    Returns UserAuthEvent if and only if the user is authenticated and is a
    member of `settings.globus.admin_group`.  Raises Unauthorized otherwise.
    """
    settings = request.state["settings"]
    check_permission(
        user, allowed_globus_groups=[settings.globus.admin_group], allowed_domains=None
    )
    return user


BearerCredentials = Annotated[HTTPAuthorizationCredentials, Depends(HTTPBearer())]
Settings = Annotated[_Settings, Depends(get_settings)]
HttpxClient = Annotated[AsyncClient, Depends(get_httpx_client)]
AsyncRedis = Annotated[Redis, Depends(get_redis)]
DbEngine = Annotated[AsyncEngine, Depends(get_db_engine)]
DbSession = Annotated[AsyncSession, Depends(get_session)]
AuthUser = Annotated[UserAuthEvent, Depends(get_auth_user)]
AdminUser = Annotated[UserAuthEvent, Depends(get_admin_user)]
