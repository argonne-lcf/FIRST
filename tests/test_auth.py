"""
Auth-gating tests for the public, authenticated, and admin API routes.

Globus is mocked at the network boundary (see ``mock_globus``), so these tests
exercise the auth logic end to end.
"""

import httpx
import pytest

from .fixtures.auth import ADMIN_TOKEN, INVALID_TOKEN, USER_TOKEN, auth_header


async def test_health_is_public(client: httpx.AsyncClient) -> None:
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


@pytest.mark.parametrize("token", [ADMIN_TOKEN, USER_TOKEN])
async def test_health_open_to_authenticated_users_too(
    client: httpx.AsyncClient, token: str
) -> None:
    resp = await client.get("/health", headers=auth_header(token))
    assert resp.status_code == 200


async def test_whoami_rejects_missing_credentials(client: httpx.AsyncClient) -> None:
    resp = await client.get("/whoami")
    assert resp.status_code == 401


async def test_whoami_rejects_invalid_token(client: httpx.AsyncClient) -> None:
    resp = await client.get("/whoami", headers=auth_header(INVALID_TOKEN))
    assert resp.status_code == 401


@pytest.mark.parametrize(
    ("token", "username"),
    [(ADMIN_TOKEN, "admin@anl.gov"), (USER_TOKEN, "user@anl.gov")],
)
async def test_whoami_returns_identity_for_authenticated_users(
    client: httpx.AsyncClient, token: str, username: str
) -> None:
    resp = await client.get("/whoami", headers=auth_header(token))
    assert resp.status_code == 200
    assert resp.json()["username"] == username


async def test_clusters_rejects_missing_credentials(
    client: httpx.AsyncClient,
) -> None:
    resp = await client.get("/clusters")
    assert resp.status_code == 401


async def test_plan_forbidden_for_non_admin(client: httpx.AsyncClient) -> None:
    # Authenticated, but not in the admin group -> 403 from check_permission.
    resp = await client.post("/plan", headers=auth_header(USER_TOKEN))
    assert resp.status_code == 403

    resp = await client.post("/apply", headers=auth_header(USER_TOKEN))
    assert resp.status_code == 403


async def test_clusters_allowed_for_admin(client: httpx.AsyncClient) -> None:
    resp = await client.get("/clusters", headers=auth_header(ADMIN_TOKEN))
    assert resp.status_code == 200
    assert resp.json() == []
