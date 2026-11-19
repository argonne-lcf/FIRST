"""
Auth-gating tests for the public, authenticated, and admin API routes.

Globus is mocked at the network boundary (see ``mock_globus``), so these tests
exercise the auth logic end to end.
"""

import asyncio
from typing import Any

import httpx
import pytest

from first_gateway.apiserver.api import app

from .fixtures.auth import ADMIN_TOKEN, INVALID_TOKEN, USER_TOKEN, auth_header


async def test_health_is_public(client: httpx.AsyncClient) -> None:
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


async def test_health_reports_router_config_failure(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager = app.state.router_config_manager
    monkeypatch.setattr(manager, "_apply_failed", True)
    resp = await client.get("/health")
    assert resp.status_code == 503
    assert resp.json() == {
        "status": "degraded",
        "detail": "RouterConfig failed to apply",
    }


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
    resp = await client.get("/catalog/v1/clusters")
    assert resp.status_code == 401


async def test_plan_forbidden_for_non_admin(
    control_client: httpx.AsyncClient,
) -> None:
    # Authenticated, but not in the admin group -> 403 from check_permission.
    resp = await control_client.post(
        "/control/v1/plan", headers=auth_header(USER_TOKEN)
    )
    assert resp.status_code == 403

    resp = await control_client.post(
        "/control/v1/apply", headers=auth_header(USER_TOKEN)
    )
    assert resp.status_code == 403


async def test_admin_catalog_forbidden_for_non_admin(
    control_client: httpx.AsyncClient,
) -> None:
    for path in ("/catalog/v1/clusters", "/catalog/v1/system-health", "/whoami"):
        assert (await control_client.get(path)).status_code == 401
        resp = await control_client.get(path, headers=auth_header(USER_TOKEN))
        assert resp.status_code == 403, path


async def test_admin_whoami_on_control_server(
    control_client: httpx.AsyncClient,
) -> None:
    resp = await control_client.get("/whoami", headers=auth_header(ADMIN_TOKEN))
    assert resp.status_code == 200
    assert resp.json()["username"] == "admin@anl.gov"


@pytest.mark.parametrize("token", [ADMIN_TOKEN, USER_TOKEN])
async def test_clusters_allowed_for_any_user(
    client: httpx.AsyncClient, token: str
) -> None:
    resp = await client.get("/catalog/v1/clusters", headers=auth_header(token))
    assert resp.status_code == 200
    assert resp.json() == []


async def test_failed_introspection_does_not_overwrite_valid_cache(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Two requests with the same token miss the cache together.  The first one's
    Globus Groups call fails slowly; the second succeeds and caches a valid
    result first.  The late failure must not replace that entry with a 60 s
    error, or the user would get 401s until it expires.
    """
    import threading

    import globus_sdk

    real_get_my_groups = globus_sdk.GroupsClient.get_my_groups
    entered, release = threading.Event(), threading.Event()
    calls = 0
    lock = threading.Lock()

    def flaky_get_my_groups(self: Any, *args: Any, **kwargs: Any) -> Any:
        nonlocal calls
        with lock:
            calls += 1
            first = calls == 1
        if first:
            entered.set()
            release.wait(timeout=10)
            raise globus_sdk.GlobusConnectionError(
                "ConnectionError on request", ConnectionError()
            )
        return real_get_my_groups(self, *args, **kwargs)

    monkeypatch.setattr(globus_sdk.GroupsClient, "get_my_groups", flaky_get_my_groups)

    slow = asyncio.create_task(client.get("/whoami", headers=auth_header(USER_TOKEN)))
    assert await asyncio.to_thread(entered.wait, 10)

    # The second request succeeds and caches a valid result.
    assert (
        await client.get("/whoami", headers=auth_header(USER_TOKEN))
    ).status_code == 200

    # The first request now fails, and fails closed.
    release.set()
    assert (await slow).status_code == 401

    # The valid cache entry survived the late failure.
    resp = await client.get("/whoami", headers=auth_header(USER_TOKEN))
    assert resp.status_code == 200
    assert calls == 2
