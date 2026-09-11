"""
Tests for the Globus data-staging service and its V2 route (/data/v1/staging).
The Globus TransferClient is mocked.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest

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


async def test_staging_route_returns_area_for_caller(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    tc = _FakeTransferClient(existing_dir=False, existing_rule=False)
    monkeypatch.setattr(staging, "get_transfer_client", lambda _state: tc)

    resp = await client.put("/data/v1/staging", headers=auth_header(USER_TOKEN))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["principal"] == "bbbbbbbb-0000-0000-0000-000000000002"
    assert body["path"] == "/user-staging/bbbbbbbb-0000-0000-0000-000000000002/"
    assert body["acl_rule_id"] == "rule-new"

    # The legacy V1 path still works and delegates to the same service.
    resp = await client.put("/resource_server/staging", headers=auth_header(USER_TOKEN))
    assert resp.status_code == 200 and resp.json()["path"] == body["path"]
