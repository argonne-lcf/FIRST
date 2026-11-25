from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from first_gateway.controllers.workers.health_alerter import gateway_errors
from first_gateway.controllers.workers.health_alerter.checks import CHECK_REGISTRY
from first_gateway.controllers.workers.health_alerter.gateway_errors import (
    check_gateway_errors,
)
from first_gateway.settings import GatewayErrorAlertSettings

Series = list[tuple[dict[str, str], float]]

UP: Series = [
    ({"instance": "10.0.0.2:9101"}, 1.0),
    ({"instance": "10.0.0.3:9101"}, 1.0),
]


def _client_state(**overrides: Any) -> Any:
    client_state = MagicMock()
    client_state.settings.gateway_error_alerts = GatewayErrorAlertSettings(
        prometheus_ca_crt_file=None, **overrides
    )
    return client_state


def _prometheus(
    *,
    up: Series = UP,
    n500: float = 0.0,
    total: float = 1000.0,
    errors: dict[str, float] | None = None,
    dispatched: dict[str, float] | None = None,
) -> Any:
    """Answer the check's queries by what they ask for."""

    async def fake_query(http: object, url: str, promql: str) -> Series:
        if promql.startswith("up{"):
            return up
        if 'code="500"' in promql:
            return [({}, n500)]
        if "gateway_http_responses_total" in promql:
            return [({}, total)]
        if 'outcome="upstream_error"' in promql:
            return [({"deployment": d}, v) for d, v in (errors or {}).items()]
        if 'outcome!="admission_rejected"' in promql:
            return [({"deployment": d}, v) for d, v in (dispatched or {}).items()]
        raise AssertionError(f"unexpected query {promql}")

    return patch.object(gateway_errors, "query", fake_query)


def test_registered() -> None:
    assert check_gateway_errors in [c.func for c in CHECK_REGISTRY]


async def test_skipped_where_not_configured() -> None:
    client_state = MagicMock()
    client_state.settings.gateway_error_alerts = None
    assert await check_gateway_errors(client_state) == []


async def test_healthy() -> None:
    with _prometheus(n500=2, errors={"a": 3}, dispatched={"a": 500}):
        assert await check_gateway_errors(_client_state()) == []


@pytest.mark.parametrize("up", [[], [({"instance": "x"}, 0.0)]])
async def test_no_gateway_metrics_is_one_warning(up: Series) -> None:
    """Without scraped replicas the other rules are blind, so say so."""
    with _prometheus(up=up, n500=100):
        [obs] = await check_gateway_errors(_client_state())
    assert obs.key == "gateway_errors/metrics"
    assert obs.severity == "warn"
    assert "blind" in obs.summary


async def test_500s() -> None:
    with _prometheus(n500=12, total=4000):
        [obs] = await check_gateway_errors(_client_state())
    assert obs.key == "gateway_errors/http_500"
    assert obs.status == "elevated"
    assert obs.severity == "crit"
    assert "12 HTTP 500s in the last 10m (0.3% of responses)" in obs.summary
    assert "Uncaught Exception" in obs.detail
    assert obs.debounce_s == 300.0


async def test_unhealthy_upstream_needs_both_count_and_ratio() -> None:
    with _prometheus(
        errors={
            "sophia/broken": 60.0,  # 60% of 100: alert, critical
            "tara/flaky": 30.0,  # 30% of 100: alert, warning
            "tara/busy": 40.0,  # 4% of 1000: high count, low share
            "minerva/quiet": 3.0,  # 100% of 3: high share, too few
        },
        dispatched={
            "sophia/broken": 100.0,
            "tara/flaky": 100.0,
            "tara/busy": 1000.0,
            "minerva/quiet": 3.0,
        },
    ):
        obs = await check_gateway_errors(_client_state())
    by_key = {o.key: o for o in obs}
    assert set(by_key) == {
        "gateway_errors/upstream/sophia/broken",
        "gateway_errors/upstream/tara/flaky",
    }
    broken = by_key["gateway_errors/upstream/sophia/broken"]
    assert broken.severity == "crit"
    assert broken.status == "unhealthy"
    assert broken.group == "Deployments"
    assert "60 of 100 upstream attempts failed in the last 10m (60%)" in broken.summary
    assert by_key["gateway_errors/upstream/tara/flaky"].severity == "warn"


async def test_thresholds_are_configurable() -> None:
    with _prometheus(n500=2, errors={"a": 5}, dispatched={"a": 10}):
        obs = await check_gateway_errors(
            _client_state(min_500s=1, upstream_min_errors=5, window="30m")
        )
    assert {o.key for o in obs} == {
        "gateway_errors/http_500",
        "gateway_errors/upstream/a",
    }
    assert all("last 30m" in o.summary for o in obs)
