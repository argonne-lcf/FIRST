"""
Tests for the Globus data-staging service and its V2 route (/data/v1/staging).
The Globus TransferClient is mocked.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest
from globus_sdk import TransferClient

from first_gateway.services import staging

from .fixtures.auth import USER_TOKEN, auth_header

# --- staging area (mocked Globus TransferClient) -----------------------------


class _FakeTransferError(Exception):
    pass


class _FakeTransferClient:
    """Minimal stand-in for globus_sdk.TransferClient's staging surface."""

    error_class = _FakeTransferError

    def __init__(self, *, existing_dir: bool, existing_rule: bool) -> None:
        self.existing_dir = existing_dir
        self.existing_rule = existing_rule
        self.mkdir_calls: list[tuple[str, str]] = []
        self.added_rules: list[dict[str, Any]] = []

    def operation_mkdir(self, collection_id: str, path: str) -> None:
        self.mkdir_calls.append((collection_id, path))
        if self.existing_dir:
            raise _FakeTransferError("Path already exists")

    def endpoint_acl_list(self, collection_id: str) -> list[dict[str, str]]:
        if self.existing_rule:
            return [
                {
                    "id": "rule-existing",
                    "principal": "user-123",
                    "path": "/user-staging/user-123/",
                }
            ]
        return []

    def add_endpoint_acl_rule(
        self, collection_id: str, rule: dict[str, Any]
    ) -> dict[str, str]:
        self.added_rules.append(rule)
        return {"access_id": "rule-new"}


def test_prep_staging_creates_dir_and_rule() -> None:
    tc = _FakeTransferClient(existing_dir=False, existing_rule=False)
    result = staging.prep_globus_staging_area(tc, "user-123", "coll-1")  # type: ignore[arg-type]

    assert result.path == "/user-staging/user-123/"
    assert result.acl_rule_id == "rule-new"
    assert result.collection_id == "coll-1"
    assert result.principal == "user-123"
    assert tc.mkdir_calls == [("coll-1", "/user-staging/user-123/")]
    assert tc.added_rules[0]["permissions"] == "rw"


def test_prep_staging_idempotent() -> None:
    tc = _FakeTransferClient(existing_dir=True, existing_rule=True)
    result = staging.prep_globus_staging_area(tc, "user-123", "coll-1")  # type: ignore[arg-type]

    # Existing dir + rule: no new rule created, existing rule id returned.
    assert result.acl_rule_id == "rule-existing"
    assert tc.added_rules == []


def test_prep_staging_reraises_unexpected_mkdir_error() -> None:
    tc = _FakeTransferClient(existing_dir=False, existing_rule=False)

    def boom(collection_id: str, path: str) -> None:
        raise _FakeTransferError("permission denied")

    tc.operation_mkdir = boom  # type: ignore[method-assign]
    with pytest.raises(_FakeTransferError, match="permission denied"):
        staging.prep_globus_staging_area(tc, "user-123", "coll-1")  # type: ignore[arg-type]


@pytest.fixture
def staging_collection(monkeypatch: pytest.MonkeyPatch) -> str:
    """Configure the staging collection before the `client` lifespan reads Settings."""
    collection_id = "coll-1"
    monkeypatch.setenv("FIRST_DATA_STAGING_GLOBUS_COLLECTION_ID", collection_id)
    return collection_id


async def test_staging_route_returns_area_for_caller(
    staging_collection: str,
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tc = _FakeTransferClient(existing_dir=False, existing_rule=False)
    monkeypatch.setattr(staging, "get_transfer_client", lambda _state: tc)

    resp = await client.put("/data/v1/staging", headers=auth_header(USER_TOKEN))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["principal"] == "bbbbbbbb-0000-0000-0000-000000000002"
    assert body["path"] == "/user-staging/bbbbbbbb-0000-0000-0000-000000000002/"
    assert body["acl_rule_id"] == "rule-new"
    assert body["collection_id"] == staging_collection

    # The legacy V1 path still works and delegates to the same service.
    resp = await client.put(
        "/resource_server/data/staging", headers=auth_header(USER_TOKEN)
    )
    assert resp.status_code == 200 and resp.json()["path"] == body["path"]


# --- the staging client --------------------------------------------------------


@pytest.fixture
def fresh_transfer_client(monkeypatch: pytest.MonkeyPatch) -> None:
    """Drop the module's cached TransferClient for this test only."""
    monkeypatch.setattr(staging, "_transfer_client", None)


@pytest.fixture
def staging_app(monkeypatch: pytest.MonkeyPatch) -> None:
    """Configure the Transfer-only staging client before the lifespan runs."""
    monkeypatch.setenv("FIRST_DATA_STAGING_CLIENT_ID", "staging-app")
    monkeypatch.setenv("FIRST_DATA_STAGING_CLIENT_SECRET", "staging-secret")


@pytest.fixture
def no_staging_app(monkeypatch: pytest.MonkeyPatch) -> None:
    """Leave the staging client unconfigured (overriding any .env.secret.gateway)."""
    monkeypatch.setenv("FIRST_DATA_STAGING_CLIENT_ID", "")


async def test_staging_route_uses_the_staging_client(
    staging_collection: str,
    staging_app: None,
    fresh_transfer_client: None,
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The TransferClient authenticates as the staging app (not Compute)."""
    app_clients: list[tuple[str, str]] = []
    tc = _FakeTransferClient(existing_dir=False, existing_rule=False)

    def fake_app_client(client_id: str, client_secret: str) -> object:
        app_clients.append((client_id, client_secret))
        return object()

    transfer_scope = TransferClient.scopes.all
    requested_scopes: list[object] = []

    def fake_authorizer(app_client: object, scope: object) -> object:
        requested_scopes.append(scope)
        return app_client

    class _TransferClientFactory:
        scopes = TransferClient.scopes

        def __new__(cls, authorizer: object) -> _FakeTransferClient:  # type: ignore[misc]
            assert authorizer is not None
            return tc

    monkeypatch.setattr(staging, "ConfidentialAppAuthClient", fake_app_client)
    monkeypatch.setattr(staging, "ClientCredentialsAuthorizer", fake_authorizer)
    monkeypatch.setattr(staging, "TransferClient", _TransferClientFactory)

    resp = await client.put("/data/v1/staging", headers=auth_header(USER_TOKEN))
    assert resp.status_code == 200, resp.text
    # One app client, from the staging settings, asking for Transfer only.
    assert app_clients == [("staging-app", "staging-secret")]
    assert requested_scopes == [transfer_scope]
    assert tc.added_rules[0]["permissions"] == "rw"


async def test_staging_route_503_without_staging_client(
    staging_collection: str,
    no_staging_app: None,
    fresh_transfer_client: None,
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def no_globus(*args: object, **kwargs: object) -> object:
        raise AssertionError("no Globus client without the staging app")

    monkeypatch.setattr(staging, "ConfidentialAppAuthClient", no_globus)

    for path in ("/data/v1/staging", "/resource_server/data/staging"):
        resp = await client.put(path, headers=auth_header(USER_TOKEN))
        assert resp.status_code == 503, resp.text
        assert "FIRST_DATA_STAGING_CLIENT_ID" in resp.text
