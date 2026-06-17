import time
from types import SimpleNamespace
from typing import Any, AsyncGenerator

import globus_sdk
import httpx
import pytest
from asgi_lifespan import LifespanManager
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from first_gateway.apiserver.api import app

ADMIN_GROUP = "1e943dcd-32ae-11f1-b41c-0e1cdb0e3035"
POLICY = "f7b3f89c-d8d2-453d-9fc7-3576bc27c421"
_IDP = "11111111-1111-1111-1111-111111111111"

# Bearer tokens the tests present; each maps to a fake Globus identity below.
ADMIN_TOKEN = "admin-token"
USER_TOKEN = "user-token"
INVALID_TOKEN = "invalid-token"

_USERS: dict[str, dict[str, Any]] = {
    ADMIN_TOKEN: {
        "sub": "aaaaaaaa-0000-0000-0000-000000000001",
        "username": "admin@anl.gov",
        "name": "Admin User",
        "groups": [ADMIN_GROUP],
    },
    USER_TOKEN: {
        "sub": "bbbbbbbb-0000-0000-0000-000000000002",
        "username": "user@anl.gov",
        "name": "Regular User",
        "groups": [],
    },
}


def auth_header(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _introspection(user: dict[str, Any]) -> dict[str, Any]:
    """A valid, active Globus introspection payload for ``user``."""
    now = int(time.time())
    return {
        "active": True,
        "scope": "openid profile email",
        "client_id": "cccccccc-0000-0000-0000-000000000003",
        "sub": user["sub"],
        "username": user["username"],
        "aud": [],
        "iss": "https://auth.globus.org/",
        "exp": now + 3600,
        "iat": now,
        "nbf": now,
        "name": user["name"],
        "email": None,
        "identity_set_detail": [
            {
                "id": user["sub"],
                "sub": user["sub"],
                "username": user["username"],
                "name": user["name"],
                "identity_provider": _IDP,
                "identity_provider_display_name": "Argonne National Laboratory",
            }
        ],
        "session_info": {
            "session_id": "dddddddd-0000-0000-0000-000000000004",
            "authentications": {user["sub"]: {"idp": _IDP}},
        },
        "policy_evaluations": {POLICY: {"evaluation": True}},
    }


@pytest.fixture
def mock_globus(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace Globus Auth network calls with token-keyed fakes."""

    def fake_post(self: Any, path: str = "", *args: Any, **kwargs: Any) -> Any:
        token = (kwargs.get("data") or {}).get("token", "")
        user = _USERS.get(token)
        if user is None:
            return SimpleNamespace(data={"active": False})
        return SimpleNamespace(data=_introspection(user))

    def fake_dependent_tokens(self: Any, token: str, *args: Any, **kwargs: Any) -> Any:
        # Echo the bearer token through as the groups.api access token so that
        # get_my_groups (below) can recover which user is calling.
        return SimpleNamespace(
            by_resource_server={"groups.api.globus.org": {"access_token": token}}
        )

    def fake_get_my_groups(
        self: Any, *args: Any, **kwargs: Any
    ) -> list[dict[str, str]]:
        token = self.authorizer.access_token
        user = _USERS.get(token, {})
        return [{"id": gid} for gid in user.get("groups", [])]

    monkeypatch.setattr(globus_sdk.ConfidentialAppAuthClient, "post", fake_post)
    monkeypatch.setattr(
        globus_sdk.ConfidentialAppAuthClient,
        "oauth2_get_dependent_tokens",
        fake_dependent_tokens,
    )
    monkeypatch.setattr(globus_sdk.GroupsClient, "get_my_groups", fake_get_my_groups)


@pytest.fixture
async def client(
    db: async_sessionmaker[AsyncSession],
    mock_globus: None,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """
    An httpx client bound to the FastAPI app, with its lifespan run.

    Relies on db and mock_globus fixtures to patch postgres, redis, and globus auth.
    """

    # See https://fastapi.tiangolo.com/advanced/async-tests/
    async with LifespanManager(app) as manager:
        transport = httpx.ASGITransport(app=manager.app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as http_client:
            yield http_client
