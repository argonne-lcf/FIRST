"""CPU-only health-check isolation; no database, secrets, or network required."""

import asyncio
from collections import defaultdict
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import Client, Response
from sqlalchemy.ext.asyncio import AsyncSession

from first_common.schema.types import HealthCheckResult, SecretRef
from first_gateway.controllers.workers.health_observer import HealthObserver
from first_gateway.database.models import Cluster, StaticDeployment

_CHECK = "first_gateway.controllers.workers.health_observer.perform_health_check"


def _observer() -> HealthObserver:
    observer = HealthObserver.__new__(HealthObserver)
    observer.fail_counts = defaultdict(int)
    observer.health_client = MagicMock(spec=Client)
    observer.health_client.request.return_value = Response(200)
    return observer


def _resource(uid: int, *, debounce: int = 1) -> StaticDeployment:
    return StaticDeployment(
        uid=uid,
        name=f"model-{uid}",
        health_check={
            "url": f"https://model-{uid}.invalid/health",
            "debounce": debounce,
        },
    )


async def test_missing_secret_does_not_block_healthy_neighbor_or_log_credentials(
    caplog: pytest.LogCaptureFixture,
) -> None:
    observer = _observer()
    broken, healthy = _resource(1), _resource(2)
    broken.health_check["api_key"] = "env_var://TEST_MISSING_HEALTH_KEY"
    broken.health_check["url"] += "?credential=url-secret-sentinel"

    with patch.object(SecretRef, "resolve", side_effect=ValueError("secret-sentinel")):
        results = await asyncio.gather(
            observer._check(broken), observer._check(healthy)
        )

    assert tuple(results) == (
        ("StaticDeployment", 1, HealthCheckResult.unhealthy),
        ("StaticDeployment", 2, HealthCheckResult.healthy),
    )
    request = cast(MagicMock, observer.health_client.request)
    request.assert_called_once()
    assert request.call_args.kwargs["url"] == healthy.health_check["url"]
    assert "StaticDeployment model-1" in caplog.text
    assert "uid=1" in caplog.text and "ValueError" in caplog.text
    assert "secret-sentinel" not in caplog.text
    assert "TEST_MISSING_HEALTH_KEY" not in caplog.text
    assert all(record.exc_info is None for record in caplog.records)


async def test_invalid_configuration_is_debounced_without_leaking_validation_input(
    caplog: pytest.LogCaptureFixture,
) -> None:
    observer = _observer()
    broken = _resource(1)
    broken.health_check = {"url": 123, "api_key": "config-secret-sentinel"}

    assert await observer._check(broken) is None
    assert await observer._check(broken) is None
    assert await observer._check(broken) == (
        "StaticDeployment",
        1,
        HealthCheckResult.unhealthy,
    )
    cast(MagicMock, observer.health_client.request).assert_not_called()
    assert "ValidationError" in caplog.text
    assert "config-secret-sentinel" not in caplog.text


async def test_check_exception_preserves_debounce_and_recovery() -> None:
    observer = _observer()
    resource = _resource(1, debounce=2)
    with patch(_CHECK, new_callable=AsyncMock) as check:
        check.side_effect = RuntimeError("check failed")
        assert await observer._check(resource) is None
        assert await observer._check(resource) == (
            "StaticDeployment",
            1,
            HealthCheckResult.unhealthy,
        )
        check.side_effect = None
        check.return_value = HealthCheckResult.healthy
        assert await observer._check(resource) == (
            "StaticDeployment",
            1,
            HealthCheckResult.healthy,
        )
        assert not observer.fail_counts
        check.side_effect = RuntimeError("check failed again")
        assert await observer._check(resource) is None


async def test_check_does_not_swallow_cancellation(
    caplog: pytest.LogCaptureFixture,
) -> None:
    observer = _observer()
    entered = asyncio.Event()

    async def wait_for_cancel(*_args: object) -> HealthCheckResult:
        entered.set()
        await asyncio.Event().wait()
        return HealthCheckResult.healthy

    with patch(_CHECK, side_effect=wait_for_cancel):
        task = asyncio.create_task(observer._check(_resource(1)))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert not observer.fail_counts
    assert not caplog.records


async def test_disabled_check_remains_skipped() -> None:
    observer = _observer()
    resource = _resource(1)
    resource.health_check["url"] = ""
    with patch(_CHECK, new_callable=AsyncMock) as check:
        assert await observer._check(resource) is None
        check.assert_not_awaited()
    assert not observer.fail_counts


async def test_poll_writes_both_bad_and_healthy_resource_transitions() -> None:
    observer = _observer()
    observer.client_state = MagicMock()
    read_session, write_session = (
        AsyncMock(spec=AsyncSession),
        AsyncMock(spec=AsyncSession),
    )
    factory = observer.client_state.db_sessionmaker
    factory.return_value.__aenter__.return_value = read_session
    factory.begin.return_value.__aenter__.return_value = write_session
    broken, healthy = _resource(1), _resource(2)
    broken.health_check["api_key"] = "env_var://TEST_MISSING_HEALTH_KEY"

    with (
        patch.object(Cluster, "list", new=AsyncMock(return_value=[])),
        patch.object(
            StaticDeployment, "list", new=AsyncMock(return_value=[broken, healthy])
        ),
        patch.object(
            SecretRef, "resolve", side_effect=ValueError("missing test secret")
        ),
    ):
        await observer._poll()

    updates = [
        call.args[0].compile().params for call in write_session.execute.await_args_list
    ]
    assert {update["health"]: update["uid_1"] for update in updates} == {
        "healthy": [2],
        "unhealthy": [1],
    }
