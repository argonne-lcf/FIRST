"""Screen-request polling is local, bounded, and never releases a quarantine."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Generator
from unittest.mock import AsyncMock, MagicMock

import globus_sdk
import httpx
import pytest
from fastapi import FastAPI
from typer.testing import CliRunner

from alcf_ai import client as inference_client_module
from alcf_ai.auth import GATEWAY_CLIENT_ID
from alcf_ai.client import AutoGlobusAuth
from first_common.errors import FirstError
from first_common.schema.quarantine_read import (
    NodeQuarantineSummary,
    QuarantineScreenReceipt,
)
from first_gateway.admin import cli as cli_module
from first_gateway.admin import client as client_module
from first_gateway.admin.client import AdminClient
from first_gateway.apiserver.dependencies import get_admin_user, get_state
from first_gateway.apiserver.routes import control
from first_gateway.services.quarantine import QuarantineService
from tests.test_node_quarantine import OPERATION, mock_service, native_sample, row

BASE = datetime(2026, 10, 11, 12, 0, tzinfo=timezone.utc)
NONCE = "b" * 32
REFERENCE = "OPS-123"
AUTHORIZATION = "Bearer qualification-only-test-token"


class Clock:
    """Deterministic monotonic deadline and UTC freshness, without real sleep."""

    def __init__(self) -> None:
        self.elapsed = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.elapsed

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.elapsed += seconds

    def now(self) -> datetime:
        return BASE + timedelta(seconds=self.elapsed)


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    result = Clock()

    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> "FrozenDatetime":
            return cls.fromtimestamp(result.now().timestamp(), tz)

    monkeypatch.setattr(client_module, "monotonic", result.monotonic)
    monkeypatch.setattr(client_module, "sleep", result.sleep)
    monkeypatch.setattr(client_module, "datetime", FrozenDatetime)
    return result


def receipt_dict(**changes: Any) -> dict[str, Any]:
    result: dict[str, Any] = {
        "status": "requested",
        "action": "screen",
        "uid": 1,
        "operation": OPERATION,
        "nonce": NONCE,
        "requested_at": (BASE - timedelta(seconds=2)).isoformat(),
    }
    return result | changes


def summary_dict(**changes: Any) -> dict[str, Any]:
    result: dict[str, Any] = {
        "uid": 1,
        "cluster_name": "tara",
        "host": "x4820c0s4b0n0",
        "environment": "dev",
        "operation": OPERATION,
        "state": "isolated",
        "state_message": "qualification",
        "origin": {"id": "1.pbs"},
        "fault": {},
        "evidence": {},
        "job_identity": {"job_id": "2.pbs"},
        "expires_at": (BASE + timedelta(days=1)).isoformat(),
        "repair_reference": REFERENCE,
        "screen_nonce": NONCE,
        "screen_requested_at": (BASE - timedelta(seconds=2)).isoformat(),
        "screen_outcome": "PASS",
        "screened_at": (BASE - timedelta(seconds=1)).isoformat(),
        "screen_expires_at": (BASE + timedelta(seconds=119)).isoformat(),
        "release_requested_at": None,
        "submission_attempts": 1,
        "submission_uncertain": False,
    }
    return result | changes


def mock_client(
    responses: list[Any],
    *,
    receipt: Any = None,
    post_status: int = 200,
    get_status: int = 200,
    clock: Clock | None = None,
    get_duration: float = 0,
    get_exception: httpx.HTTPError | None = None,
) -> tuple[AdminClient, list[httpx.Request]]:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.headers["Authorization"] == AUTHORIZATION
        if request.method == "POST":
            assert request.url.path == "/control/v1/node-quarantines/1/screen"
            return httpx.Response(
                post_status,
                json=receipt_dict() if receipt is None else receipt,
            )
        assert request.method == "GET"
        assert request.url.path == "/control/v1/node-quarantines"
        if clock is not None:
            clock.elapsed += get_duration
        if get_exception is not None:
            raise get_exception
        value = responses.pop(0) if len(responses) > 1 else responses[0]
        return httpx.Response(get_status, json=value)

    client = AdminClient.__new__(AdminClient)
    httpx.Client.__init__(
        client,
        base_url="http://qualification.invalid",
        transport=httpx.MockTransport(handler),
        headers={"Authorization": AUTHORIZATION},
        auth=None,
    )
    return client, requests


def test_screen_without_wait_remains_asynchronous_and_never_releases(
    clock: Clock,
) -> None:
    client, requests = mock_client([])
    with client:
        result = client.screen_node_quarantine(1, REFERENCE)
    assert result == QuarantineScreenReceipt.model_validate(receipt_dict())
    assert len(requests) == 1 and requests[0].method == "POST"
    assert clock.elapsed == 0 and not clock.sleeps


@pytest.mark.parametrize("pending_polls", [0, 2])
def test_wait_only_succeeds_for_exact_fresh_pass_without_release(
    clock: Clock, pending_polls: int
) -> None:
    pending = summary_dict(
        screen_outcome=None, screened_at=None, screen_expires_at=None
    )
    responses = [[pending]] * pending_polls + [[summary_dict()]]
    client, requests = mock_client(responses)
    with client:
        result = client.screen_node_quarantine(1, REFERENCE, wait=True, timeout=5)
    assert result.nonce == NONCE
    assert len(requests) == pending_polls + 2
    assert clock.elapsed == pending_polls
    assert all(request.method == "GET" for request in requests[1:])
    for request in requests[1:]:
        timeout = request.extensions["timeout"]
        assert 0 < timeout["read"] <= 5


def test_wait_reuses_accepted_authorization_without_auth_refresh(clock: Clock) -> None:
    pending = summary_dict(
        screen_outcome=None, screened_at=None, screen_expires_at=None
    )
    client, requests = mock_client([[pending], [summary_dict()]])

    class CountingAuth(httpx.Auth):
        calls = 0

        def auth_flow(
            self, request: httpx.Request
        ) -> Generator[httpx.Request, httpx.Response, None]:
            self.calls += 1
            assert self.calls == 1, "wait must not refresh authentication"
            request.headers["Authorization"] = AUTHORIZATION
            yield request

    auth = CountingAuth()
    client.auth = auth
    with client:
        client.screen_node_quarantine(1, REFERENCE, wait=True, timeout=5)
    assert auth.calls == 1 and len(requests) == 3


@pytest.mark.parametrize("failure", ["sdk-error", None, "", " "])
def test_wait_initial_sdk_token_failure_never_posts_or_launches_login(
    monkeypatch: pytest.MonkeyPatch, clock: Clock, failure: str | None
) -> None:
    login = MagicMock()
    original_config = globus_sdk.GlobusAppConfig(token_validation_error_handler=login)
    app = MagicMock(config=original_config)
    if failure == "sdk-error":
        app.get_authorizer.side_effect = RuntimeError("qualification expired token")
    else:
        app.get_authorizer.return_value.get_authorization_header.return_value = failure
    build = MagicMock(return_value=app)
    monkeypatch.setattr(client_module, "build_user_app", build)
    client, requests = mock_client([])
    auth = AutoGlobusAuth()
    client.auth = auth
    with client, pytest.raises(FirstError, match="authentication") as caught:
        client.screen_node_quarantine(1, REFERENCE, wait=True)
    assert caught.value.status_code == 401
    assert not requests
    assert client.auth is auth
    assert app.config.token_validation_error_handler is None
    assert original_config.token_validation_error_handler is login
    login.assert_not_called()
    app.login.assert_not_called()
    build.assert_called_once_with()


def test_wait_sdk_header_disables_login_handler_and_reuses_post_get_auth(
    monkeypatch: pytest.MonkeyPatch, clock: Clock
) -> None:
    login = MagicMock()
    original_config = globus_sdk.GlobusAppConfig(token_validation_error_handler=login)
    app = MagicMock(config=original_config)
    app.get_authorizer.return_value.get_authorization_header.return_value = (
        AUTHORIZATION
    )
    monkeypatch.setattr(client_module, "build_user_app", MagicMock(return_value=app))
    normal_auth = MagicMock(side_effect=AssertionError("wait used normal auth flow"))
    monkeypatch.setattr(
        inference_client_module, "get_inference_authorizer", normal_auth
    )
    client, requests = mock_client([[summary_dict()]])
    auth = AutoGlobusAuth()
    client.auth = auth
    with client:
        client.screen_node_quarantine(1, REFERENCE, wait=True)
    assert client.auth is auth
    assert app.config is not original_config
    assert app.config.token_validation_error_handler is None
    assert original_config.token_validation_error_handler is login
    app.get_authorizer.assert_called_once_with(GATEWAY_CLIENT_ID)
    app.get_authorizer.return_value.get_authorization_header.assert_called_once_with()
    assert [request.method for request in requests] == ["POST", "GET"]
    assert all(
        request.headers["Authorization"] == AUTHORIZATION for request in requests
    )
    normal_auth.assert_not_called()
    login.assert_not_called()
    app.login.assert_not_called()


def test_async_screen_preserves_normal_auth_without_wait_helper(
    monkeypatch: pytest.MonkeyPatch, clock: Clock
) -> None:
    helper = MagicMock(
        side_effect=AssertionError("asynchronous screen used wait helper")
    )
    monkeypatch.setattr(client_module, "_screen_wait_authorization", helper)
    authorizer = MagicMock(access_token="qualification-only-test-token")
    provider = MagicMock(return_value=authorizer)
    monkeypatch.setattr(inference_client_module, "get_inference_authorizer", provider)
    client, requests = mock_client([])
    auth = AutoGlobusAuth()
    client.auth = auth
    with client:
        client.screen_node_quarantine(1, REFERENCE)
    assert client.auth is auth
    assert len(requests) == 1 and requests[0].method == "POST"
    provider.assert_called_once_with()
    authorizer.ensure_valid_token.assert_called_once_with()
    helper.assert_not_called()


@pytest.mark.parametrize("timeout", [1, 600])
def test_timeout_bounds_are_accepted_with_bounded_get(
    clock: Clock, timeout: int
) -> None:
    client, requests = mock_client([[summary_dict()]])
    with client:
        client.screen_node_quarantine(1, REFERENCE, wait=True, timeout=timeout)
    assert requests[1].extensions["timeout"]["read"] == min(10, timeout)


@pytest.mark.parametrize(
    "changes",
    [
        {"screen_outcome": "FAIL"},
        {"screened_at": (BASE - timedelta(seconds=121)).isoformat()},
        {"screened_at": (BASE - timedelta(seconds=3)).isoformat()},
        {"screen_expires_at": BASE.isoformat()},
        {"screen_expires_at": (BASE - timedelta(seconds=1)).isoformat()},
        {"screen_expires_at": None},
        {"screened_at": None},
        {"screened_at": (BASE + timedelta(seconds=6)).isoformat()},
        {"expires_at": BASE.isoformat()},
        {"expires_at": None},
        {"state": "lost"},
        {"state": "failed"},
        {"state": "released"},
        {"operation": "c" * 32},
        {"screen_nonce": "c" * 32},
        {"screen_nonce": None},
        {"repair_reference": "OPS-OTHER"},
        {"screen_requested_at": BASE.isoformat()},
        {"screen_requested_at": None},
    ],
    ids=[
        "fail",
        "expired-envelope",
        "pre-request-envelope",
        "expired-oldest-sample",
        "stale-oldest-sample",
        "missing-sample-expiry",
        "missing-envelope-time",
        "future-envelope",
        "expired-isolation",
        "missing-isolation-expiry",
        "lost-isolation",
        "failed-isolation",
        "released-isolation",
        "reestablished-operation",
        "superseded-nonce",
        "missing-nonce",
        "changed-reference",
        "superseded-request-time",
        "missing-request-time",
    ],
)
def test_wait_fails_closed_for_negative_stale_or_superseded_screen(
    clock: Clock, changes: dict[str, Any]
) -> None:
    client, requests = mock_client([[summary_dict(**changes)]])
    with client, pytest.raises(FirstError):
        client.screen_node_quarantine(1, REFERENCE, wait=True, timeout=3)
    assert len(requests) == 2
    assert not clock.sleeps


@pytest.mark.parametrize(
    "rows",
    [[], [summary_dict(uid=2)], [summary_dict(), summary_dict()], {"items": []}],
    ids=["missing", "wrong-uid", "duplicate-uid", "malformed-list"],
)
def test_wait_rejects_missing_duplicate_or_malformed_lookup(
    clock: Clock, rows: Any
) -> None:
    client, _ = mock_client([rows])
    with client, pytest.raises(FirstError):
        client.screen_node_quarantine(1, REFERENCE, wait=True)


def test_wait_does_not_accept_another_nodes_pass(clock: Clock) -> None:
    client, _ = mock_client(
        [[summary_dict(uid=2), summary_dict(screen_outcome="FAIL")]]
    )
    with client, pytest.raises(FirstError):
        client.screen_node_quarantine(1, REFERENCE, wait=True)


def test_request_superseded_after_pending_poll_is_not_latest_pass_success(
    clock: Clock,
) -> None:
    pending = summary_dict(
        screen_outcome=None, screened_at=None, screen_expires_at=None
    )
    client, requests = mock_client([[pending], [summary_dict(screen_nonce="c" * 32)]])
    with client, pytest.raises(FirstError):
        client.screen_node_quarantine(1, REFERENCE, wait=True, timeout=5)
    assert len(requests) == 3 and clock.elapsed == 1


def test_wait_timeout_is_bounded_and_does_not_resubmit(clock: Clock) -> None:
    client, requests = mock_client(
        [[summary_dict(screen_outcome=None, screened_at=None, screen_expires_at=None)]]
    )
    with client, pytest.raises(FirstError, match="(?i)time"):
        client.screen_node_quarantine(1, REFERENCE, wait=True, timeout=3)
    assert clock.elapsed == 3
    assert all(0 < seconds <= 1 for seconds in clock.sleeps)
    assert sum(request.method == "POST" for request in requests) == 1
    assert [request.extensions["timeout"]["read"] for request in requests[1:]] == [
        3,
        2,
        1,
    ]


def test_pass_received_after_deadline_is_failure(clock: Clock) -> None:
    client, requests = mock_client([[summary_dict()]], clock=clock, get_duration=4)
    with client, pytest.raises(FirstError, match="(?i)time"):
        client.screen_node_quarantine(1, REFERENCE, wait=True, timeout=3)
    assert len(requests) == 2 and clock.elapsed == 4


@pytest.mark.parametrize("status", [401, 403, 404, 500])
def test_wait_authentication_and_lookup_errors_are_not_retried(
    clock: Clock, status: int
) -> None:
    client, requests = mock_client([{"detail": "test rejection"}], get_status=status)
    with client, pytest.raises(FirstError):
        client.screen_node_quarantine(1, REFERENCE, wait=True)
    assert len(requests) == 2 and not clock.sleeps


@pytest.mark.parametrize("status", [401, 403])
def test_screen_post_authentication_failure_never_polls(
    clock: Clock, status: int
) -> None:
    client, requests = mock_client([], post_status=status)
    with client, pytest.raises(FirstError):
        client.screen_node_quarantine(1, REFERENCE, wait=True)
    assert len(requests) == 1


@pytest.mark.parametrize(
    "receipt",
    [
        {},
        {"status": "requested", "action": "screen"},
        receipt_dict(uid=2),
        receipt_dict(operation="not-an-operation"),
        receipt_dict(nonce="not-a-nonce"),
        receipt_dict(requested_at="2026-10-11T12:00:00"),
        receipt_dict(requested_at="not-a-timestamp"),
        receipt_dict(action="release"),
    ],
)
def test_invalid_or_incomplete_screen_receipt_is_not_polled(
    clock: Clock, receipt: dict[str, Any]
) -> None:
    client, requests = mock_client([], receipt=receipt)
    with client, pytest.raises(FirstError):
        client.screen_node_quarantine(1, REFERENCE, wait=True)
    assert len(requests) == 1


@pytest.mark.parametrize("timeout", [0, -1, 601])
def test_timeout_range_rejected_before_request(clock: Clock, timeout: int) -> None:
    client, requests = mock_client([])
    with client, pytest.raises((FirstError, ValueError)):
        client.screen_node_quarantine(1, REFERENCE, wait=True, timeout=timeout)
    assert not requests


def test_get_transport_error_is_failure_without_retry(clock: Clock) -> None:
    client, requests = mock_client(
        [], get_exception=httpx.ConnectError("qualification lookup failure")
    )
    with client, pytest.raises((FirstError, httpx.HTTPError)):
        client.screen_node_quarantine(1, REFERENCE, wait=True)
    assert len(requests) == 2 and not clock.sleeps


@pytest.mark.parametrize("wait", [False, True])
def test_cli_wait_option_remains_explicit_and_does_not_release(
    monkeypatch: pytest.MonkeyPatch, wait: bool
) -> None:
    client = MagicMock(spec=AdminClient)
    client.base_url = "http://qualification.invalid"
    client.screen_node_quarantine.return_value = QuarantineScreenReceipt.model_validate(
        receipt_dict()
    )
    monkeypatch.setattr(cli_module, "AdminClient", MagicMock(return_value=client))
    command = ["node-quarantine", "screen", "1", REFERENCE]
    if wait:
        command.extend(["--wait", "--timeout", "20"])
    result = CliRunner().invoke(cli_module.cli, command)
    assert result.exit_code == 0, result.output
    client.screen_node_quarantine.assert_called_once_with(
        1, REFERENCE, wait=wait, timeout=20 if wait else 180
    )
    client.node_quarantine_action.assert_not_called()


@pytest.mark.parametrize("timeout", ["0", "601", "-1", "not-a-number"])
def test_cli_timeout_is_bounded(monkeypatch: pytest.MonkeyPatch, timeout: str) -> None:
    client = MagicMock(spec=AdminClient)
    client.base_url = "http://qualification.invalid"
    monkeypatch.setattr(cli_module, "AdminClient", MagicMock(return_value=client))
    result = CliRunner().invoke(
        cli_module.cli,
        ["node-quarantine", "screen", "1", REFERENCE, "--wait", "--timeout", timeout],
    )
    assert result.exit_code != 0
    client.screen_node_quarantine.assert_not_called()
    client.node_quarantine_action.assert_not_called()


def test_cli_screen_failure_has_nonzero_exit_and_cannot_chain_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = MagicMock(spec=AdminClient)
    client.base_url = "http://qualification.invalid"
    client.screen_node_quarantine.side_effect = FirstError("fresh screen failed")
    monkeypatch.setattr(cli_module, "AdminClient", MagicMock(return_value=client))
    result = CliRunner().invoke(
        cli_module.cli, ["node-quarantine", "screen", "1", REFERENCE, "--wait"]
    )
    assert result.exit_code != 0
    client.node_quarantine_action.assert_not_called()


async def test_service_screen_receipt_captures_locked_request_without_refetch() -> None:
    node = row()
    service, session, adapter = mock_service(node)
    result = await service.request_screen(1, REFERENCE)
    assert result.uid == node.uid
    assert result.operation == node.operation
    assert result.nonce == node.screen_nonce
    assert result.requested_at == node.screen_requested_at
    assert node.repair_reference == REFERENCE
    assert node.screen_result is None and node.release_requested_at is None
    session.get.assert_awaited_once_with(type(node), 1, with_for_update=True)
    adapter.request_quarantine_screen.assert_not_awaited()
    adapter.delete_quarantine.assert_not_awaited()


async def test_second_screen_does_not_mutate_first_request_receipt() -> None:
    node = row()
    service, _, adapter = mock_service(node)
    first = await service.request_screen(1, REFERENCE)
    second = await service.request_screen(1, "OPS-SECOND")
    assert first.uid == second.uid == 1
    assert first.operation == second.operation == OPERATION
    assert first.nonce != second.nonce
    assert first.nonce != node.screen_nonce
    assert second.nonce == node.screen_nonce
    assert node.repair_reference == "OPS-SECOND"
    adapter.delete_quarantine.assert_not_awaited()


@pytest.mark.parametrize("action", ["screen", "release", "reestablish"])
async def test_control_route_returns_screen_identity_and_preserves_other_actions(
    monkeypatch: pytest.MonkeyPatch, action: str
) -> None:
    service = MagicMock(spec=QuarantineService)
    receipt = QuarantineScreenReceipt.model_validate(receipt_dict())
    service.request_screen = AsyncMock(return_value=receipt)
    service.request_release = AsyncMock(return_value=None)
    service.request_reestablish = AsyncMock(return_value=None)
    monkeypatch.setattr(control, "QuarantineService", MagicMock(return_value=service))
    application = FastAPI()
    application.include_router(control.admin_router)
    application.dependency_overrides[get_state] = lambda: MagicMock()
    application.dependency_overrides[get_admin_user] = lambda: SimpleNamespace(
        id="qualification-only-admin"
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=application),
        base_url="http://qualification.invalid",
    ) as client:
        response = await client.post(
            f"/control/v1/node-quarantines/1/{action}",
            json={"reference": REFERENCE},
        )
    assert response.status_code == 200
    assert response.json() == (
        receipt.model_dump(mode="json")
        if action == "screen"
        else {"status": "requested", "action": action}
    )
    getattr(service, f"request_{action}").assert_awaited_once_with(1, REFERENCE)


def test_summary_expires_at_oldest_native_sample_not_envelope_time() -> None:
    newest = int(BASE.timestamp() * 1e9)
    node = row(
        screen_nonce=NONCE,
        screen_result={
            "operation": OPERATION,
            "nonce": NONCE,
            "outcome": "PASS",
            "observed_unix_ns": newest,
            "reports": [
                native_sample(faulty=False, collected=newest - 119_000_000_000)
            ],
            "confirmation": [
                native_sample(faulty=False, collected=newest - 117_000_000_000)
            ],
        },
    )
    result = NodeQuarantineSummary.model_validate(node)
    assert result.screened_at == BASE
    assert result.screen_expires_at == BASE + timedelta(seconds=1)


@pytest.mark.parametrize(
    "changes",
    [
        {"operation": "c" * 32},
        {"nonce": "c" * 32},
        {"outcome": "FAIL"},
        {"reports": []},
        {"confirmation": [{"collected_unix_ns": True}]},
        {"reports": [{"collected_unix_ns": -1}]},
    ],
)
def test_invalid_native_sample_identity_never_gets_freshness_expiry(
    changes: dict[str, Any],
) -> None:
    newest = int(BASE.timestamp() * 1e9)
    payload: dict[str, Any] = {
        "operation": OPERATION,
        "nonce": NONCE,
        "outcome": "PASS",
        "observed_unix_ns": newest,
        "reports": [{"collected_unix_ns": newest - 4_000_000_000}],
        "confirmation": [{"collected_unix_ns": newest - 2_000_000_000}],
    }
    assert (
        row(screen_nonce=NONCE, screen_result=payload | changes).screen_expires_at
        is None
    )
