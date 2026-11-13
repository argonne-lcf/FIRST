"""Tests for the health alerter's check_log_shipping.

Prometheus is replaced by a dict of query -> series; the prompt shipper's
status file is written to a temp directory.
"""

import json
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from first_gateway.controllers.workers.health_alerter import log_shipping
from first_gateway.controllers.workers.health_alerter.checks import CHECK_REGISTRY
from first_gateway.controllers.workers.health_alerter.log_shipping import (
    _VECTOR_RULES,
    _VECTOR_UP,
    _query,
    check_log_shipping,
)
from first_gateway.settings import LogShippingSettings

Series = list[tuple[dict[str, str], float]]
VECTOR_UP: dict[str, Series] = {_VECTOR_UP: [({"job": "vector"}, 1.0)]}
RULES = {rule.name: rule for rule in _VECTOR_RULES}


def _client_state(tmp_path: Path) -> MagicMock:
    client_state = MagicMock()
    client_state.settings.log_shipping = LogShippingSettings(
        status_dir=tmp_path, prometheus_ca_crt_file=None
    )
    return client_state


def _write_status(tmp_path: Path, name: str = "v2", **overrides: Any) -> None:
    now = int(time.time())
    status = {
        "updated_at": now,
        "last_success_at": now,
        "exit_code": 0,
        "consecutive_failures": 0,
        "pending_files": 0,
        "oldest_pending_age_secs": 0,
        "expired_deleted_total": 0,
    }
    (tmp_path / f"prompt-shipper-{name}.json").write_text(
        json.dumps(status | overrides)
    )


def _prometheus(results: dict[str, Series]) -> Any:
    """Patch the Prometheus query; a query not in `results` returns no series."""

    async def fake_query(http: object, url: str, promql: str) -> Series:
        return results.get(promql, [])

    return patch.object(log_shipping, "_query", fake_query)


def test_registered() -> None:
    assert check_log_shipping in [c.func for c in CHECK_REGISTRY]


def test_prometheus_defaults_to_verified_tls() -> None:
    settings = LogShippingSettings()
    assert settings.prometheus_url == "https://prometheus:9090"
    assert settings.prometheus_ca_crt_file == Path("/run/secrets/pilot_ca.crt")


async def test_vector_client_uses_private_ca(tmp_path: Path) -> None:
    ca_file = tmp_path / "ca.crt"
    with _prometheus({}), patch.object(log_shipping, "AsyncClient") as client:
        await log_shipping._check_vector("https://prometheus:9090", ca_file)
    client.assert_called_once_with(timeout=10.0, verify=str(ca_file))


async def test_skipped_where_not_deployed() -> None:
    client_state = MagicMock()
    client_state.settings.log_shipping = None
    assert await check_log_shipping(client_state) == []


async def test_healthy(tmp_path: Path) -> None:
    _write_status(tmp_path)
    with _prometheus(VECTOR_UP):
        assert await check_log_shipping(_client_state(tmp_path)) == []


@pytest.mark.parametrize(
    "up, expected",
    [([({"job": "vector"}, 0.0)], "Vector is down"), ([], "no `vector` target")],
)
async def test_vector_down_is_one_alert(
    tmp_path: Path, up: Series, expected: str
) -> None:
    """With Vector down its metrics are stale, so only `down` is reported."""
    _write_status(tmp_path)
    silent = RULES["v2_silent"].query
    with _prometheus({_VECTOR_UP: up, silent: [({}, 0.0)]}):
        [obs] = await check_log_shipping(_client_state(tmp_path))
    assert (obs.status, obs.severity) == ("down", "crit")
    assert expected in obs.summary


async def test_vector_rules_become_observations(tmp_path: Path) -> None:
    _write_status(tmp_path)
    results = VECTOR_UP | {
        RULES["sink_stuck"].query: [({"component_id": "clickhouse"}, 812.0)],
        RULES["buffer_filling"].query: [({"component_id": "s3_archive"}, 0.62)],
        RULES["errors"].query: [
            ({"component_id": "v2_docker", "error_type": "reader_failed"}, 3.0)
        ],
    }
    with _prometheus(results):
        observations = await check_log_shipping(_client_state(tmp_path))

    by_key = {o.key: o for o in observations}
    stuck = by_key["log_shipping/vector/sink_stuck/clickhouse"]
    assert stuck.severity == "crit"
    assert "`clickhouse` has sent nothing for 15m; 812 events" in stuck.summary
    assert "62% full" in by_key["log_shipping/vector/buffer_filling/s3_archive"].summary
    errors = by_key["log_shipping/vector/errors/v2_docker"]
    assert errors.severity == "warn"
    assert "3 `reader_failed` errors" in errors.summary
    assert len(observations) == 3


async def test_prometheus_failure_fails_the_check(tmp_path: Path) -> None:
    """An unreachable Prometheus must surface as a failed check, not as 'healthy'."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="down")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        with pytest.raises(httpx.HTTPStatusError):
            await _query(http, "http://prometheus:9090", "up")


async def test_query_parses_instant_vector() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/query"
        assert request.url.params["query"] == "up"
        result = [{"metric": {"job": "vector"}, "value": [1794320000.0, "1"]}]
        return httpx.Response(
            200, json={"status": "success", "data": {"result": result}}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        assert await _query(http, "http://prometheus:9090", "up") == [
            ({"job": "vector"}, 1.0)
        ]


async def test_prompt_shipper_missing_status(tmp_path: Path) -> None:
    with _prometheus(VECTOR_UP):
        [obs] = await check_log_shipping(_client_state(tmp_path))
    assert (obs.key, obs.status, obs.severity) == (
        "log_shipping/prompts/v2",
        "not_running",
        "crit",
    )


async def test_prompt_shipper_stale_status(tmp_path: Path) -> None:
    _write_status(tmp_path, updated_at=int(time.time()) - 3600)
    with _prometheus(VECTOR_UP):
        [obs] = await check_log_shipping(_client_state(tmp_path))
    assert (obs.status, obs.severity) == ("not_running", "crit")
    assert "no status for 60m" in obs.summary


@pytest.mark.parametrize(
    "status, expected_status, expected_severity, expected_text",
    [
        # One failed cycle is a blip, not an alert.
        ({"consecutive_failures": 1, "exit_code": 1}, None, None, ""),
        (
            {"consecutive_failures": 3, "exit_code": 1, "pending_files": 4},
            "failing",
            "warn",
            "upload failed 3 times in a row (rclone exit 1); 4 files waiting",
        ),
        (
            {"pending_files": 2, "oldest_pending_age_secs": 2 * 3600},
            "failing",
            "warn",
            "2 files waiting, the oldest for 2h",
        ),
        (
            {
                "consecutive_failures": 300,
                "pending_files": 900,
                "oldest_pending_age_secs": 3 * 86400,
            },
            "backlog",
            "crit",
            "900 files waiting, the oldest for 3d",
        ),
    ],
)
async def test_prompt_shipper_backlog(
    tmp_path: Path,
    status: dict[str, Any],
    expected_status: str | None,
    expected_severity: str | None,
    expected_text: str,
) -> None:
    _write_status(tmp_path, **status)
    with _prometheus(VECTOR_UP):
        observations = await check_log_shipping(_client_state(tmp_path))
    if expected_status is None:
        assert observations == []
        return
    [obs] = observations
    assert (obs.status, obs.severity) == (expected_status, expected_severity)
    assert expected_text in obs.summary


async def test_prompt_shipper_expired_deletion_is_its_own_alert(
    tmp_path: Path,
) -> None:
    _write_status(tmp_path, expired_deleted_total=7)
    with _prometheus(VECTOR_UP):
        [obs] = await check_log_shipping(_client_state(tmp_path))
    assert (obs.key, obs.severity) == ("log_shipping/prompts/v2/expired", "crit")
    assert "deleted 7 prompt files that were never uploaded" in obs.summary


async def test_each_configured_shipper_needs_a_status(tmp_path: Path) -> None:
    """V1 and V2 side by side: a missing V1 status alerts, V2 stays quiet."""
    _write_status(tmp_path, name="v2")
    client_state = _client_state(tmp_path)
    client_state.settings.log_shipping.prompt_shippers = ["v2", "v1"]
    with _prometheus(VECTOR_UP):
        [obs] = await check_log_shipping(client_state)
    assert obs.key == "log_shipping/prompts/v1"
    assert "prompt-shipper-v1" in obs.summary
