"""Failure details reported by first_common.health; no network required."""

from typing import Callable

import pytest
from httpx import Client, ConnectError, MockTransport, Request, Response

from first_common.health import HealthCheckOutcome, perform_health_check_sync
from first_common.schema.types import HealthCheckParams, HealthCheckResult

_KEY_VAR = "TEST_HEALTH_CHECK_KEY"


def _check(
    handler: Callable[[Request], Response],
    monkeypatch: pytest.MonkeyPatch,
    **params: object,
) -> HealthCheckOutcome:
    monkeypatch.setenv(_KEY_VAR, "secret-sentinel")
    client = Client(transport=MockTransport(handler))
    return perform_health_check_sync(
        client,
        HealthCheckParams.model_validate(
            {
                "url": "http://backend/health",
                "api_key": f"env_var://{_KEY_VAR}",
                "attempt_delay": 0,
                **params,
            }
        ),
    )


def test_healthy_has_no_detail(monkeypatch: pytest.MonkeyPatch) -> None:
    outcome = _check(lambda _: Response(200), monkeypatch)
    assert outcome == HealthCheckOutcome(HealthCheckResult.healthy, "")


def test_status_out_of_range_includes_body_excerpt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outcome = _check(
        lambda _: Response(503, text="  engine not ready" + "x" * 1000), monkeypatch
    )
    assert outcome.result == HealthCheckResult.unhealthy
    assert outcome.detail.startswith("HTTP 503 (expected 200-299): engine not ready")
    assert outcome.detail.endswith("…")
    assert len(outcome.detail) < 600


def test_pattern_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    outcome = _check(
        lambda _: Response(200, text="degraded"), monkeypatch, match_pattern="ok"
    )
    assert outcome == HealthCheckOutcome(
        HealthCheckResult.unhealthy, "Response body did not match 'ok': degraded"
    )


def test_request_error_redacts_resolved_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(req: Request) -> Response:
        raise ConnectError(f"refused with {req.headers['Authorization']}")

    outcome = _check(handler, monkeypatch)
    assert outcome == HealthCheckOutcome(
        HealthCheckResult.unhealthy,
        "Request failed: ConnectError: refused with Bearer <redacted>",
    )


def test_body_echoing_secret_is_redacted(monkeypatch: pytest.MonkeyPatch) -> None:
    outcome = _check(
        lambda req: Response(401, text=f"bad token {req.headers['Authorization']}"),
        monkeypatch,
    )
    assert "secret-sentinel" not in outcome.detail
    assert outcome.detail == "HTTP 401 (expected 200-299): bad token Bearer <redacted>"


def test_unresolvable_secret_names_the_ref(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TEST_HEALTH_CHECK_MISSING", raising=False)
    outcome = _check(
        lambda _: Response(200),
        monkeypatch,
        api_key="env_var://TEST_HEALTH_CHECK_MISSING",
    )
    assert outcome == HealthCheckOutcome(
        HealthCheckResult.unhealthy,
        "Could not resolve api_key env_var://TEST_HEALTH_CHECK_MISSING (ValueError)",
    )
