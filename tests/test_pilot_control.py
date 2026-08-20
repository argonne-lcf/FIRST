"""Tests for PilotControlClient's built-in timeout/retry behavior."""

from unittest.mock import MagicMock

import httpx
import pytest

from first_common.errors import ReplicaTeardownError
from first_gateway.services.pilot_control import PilotControlClient
from first_pilot.control_api import app, get_manager


def _make_client(handler: object) -> PilotControlClient:
    client = PilotControlClient.__new__(PilotControlClient)
    client._client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler)  # type: ignore[arg-type]
    )
    return client


async def test_retries_transient_transport_error_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Skip the backoff sleeps to keep the test fast.
    async def _no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr("first_gateway.services.pilot_control.asyncio.sleep", _no_sleep)

    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectError("boom", request=request)
        return httpx.Response(200)

    client = _make_client(handler)
    resp = await client._request("POST", "https://mgr/stop-replica/r0")

    assert resp.status_code == 200
    assert calls["n"] == 2


async def test_retries_transient_5xx_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr("first_gateway.services.pilot_control.asyncio.sleep", _no_sleep)

    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(503)
        return httpx.Response(200)

    client = _make_client(handler)
    resp = await client._request("GET", "https://mgr/status")

    assert resp.status_code == 200
    assert calls["n"] == 3


async def test_gives_up_and_raises_after_persistent_transport_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr("first_gateway.services.pilot_control.asyncio.sleep", _no_sleep)

    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        raise httpx.ConnectError("down", request=request)

    client = _make_client(handler)
    with pytest.raises(httpx.ConnectError):
        await client._request("GET", "https://mgr/status")

    assert calls["n"] == 3


async def test_4xx_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr("first_gateway.services.pilot_control.asyncio.sleep", _no_sleep)

    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(409)

    client = _make_client(handler)
    resp = await client._request("POST", "https://mgr/start-replica")

    assert resp.status_code == 409
    assert calls["n"] == 1


async def test_pilot_api_returns_structured_teardown_error() -> None:
    manager = MagicMock()
    manager.stop_replica.side_effect = ReplicaTeardownError(
        "Replica r0 teardown incomplete; still present: model process group"
    )
    app.dependency_overrides[get_manager] = lambda: manager
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://pilot",
        ) as client:
            response = await client.post("/stop-replica/r0")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 409
    assert response.json() == {
        "error": {
            "code": "replica_teardown_incomplete",
            "message": (
                "Replica r0 teardown incomplete; still present: model process group"
            ),
            "info": {},
        }
    }
