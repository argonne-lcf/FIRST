from typing import Annotated, AsyncGenerator, cast

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import (
    AsyncSession,
)

from first_common.schema.auth import UserAuthEvent

from ..settings import ClientState
from .auth import GlobusAuthService, check_permission


async def get_state(request: Request) -> ClientState:
    return cast(ClientState, request.state)


State = Annotated[ClientState, Depends(get_state)]


async def get_session(state: State) -> AsyncGenerator[AsyncSession, None]:
    """
    Yields a "commit-as-you-go" AsyncSession.  Use sess.begin() or sess.commit()
    to manage transactions explicitly.
    """
    async with state["db_sessionmaker"]() as sess:
        yield sess


async def get_auth_user(
    state: State,
    token: HTTPAuthorizationCredentials = Depends(HTTPBearer()),
) -> UserAuthEvent:
    """
    Returns UserAuthEvent if and only if the user is authenticated. Raises Unauthorized otherwise.
    """
    auth_svc = GlobusAuthService(state)
    user = await auth_svc.validate_access_token(token)
    return user


async def get_admin_user(
    state: State, user: UserAuthEvent = Depends(get_auth_user)
) -> UserAuthEvent:
    """
    Returns UserAuthEvent if and only if the user is authenticated and is a
    member of `settings.globus.admin_group`.  Raises Unauthorized otherwise.
    """
    settings = state["settings"]
    check_permission(
        user, allowed_globus_groups=[settings.globus.admin_group], allowed_domains=None
    )
    return user


BearerCredentials = Annotated[HTTPAuthorizationCredentials, Depends(HTTPBearer())]
DbSession = Annotated[AsyncSession, Depends(get_session)]
AuthUser = Annotated[UserAuthEvent, Depends(get_auth_user)]
AdminUser = Annotated[UserAuthEvent, Depends(get_admin_user)]
