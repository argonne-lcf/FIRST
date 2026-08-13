from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, cast

import pytest
import uvicorn
from pydantic import ValidationError

from first_gateway.controllers import metrics_server
from first_gateway.controllers.workers.health_alerter import checks
from first_gateway.settings import ClientState, Settings


def build_settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "db_url": "postgresql+psycopg://first:private@127.0.0.1:5433/firstv2",
        "redis_url": "redis://127.0.0.1:6380/0",
        "globus": {
            "app_id": "00000000-0000-0000-0000-000000000001",
            "app_secret": "private",
            "compute_client_id": "00000000-0000-0000-0000-000000000002",
            "compute_client_secret": "private",
            "admin_group": "00000000-0000-0000-0000-000000000003",
        },
        "pilot_ca_crt": "test certificate",
        "pilot_ca_key": "private",
    }
    values.update(overrides)
    return Settings(**values)  # type: ignore[arg-type]


def test_controller_metrics_bind_defaults_remain_compose_compatible() -> None:
    settings = build_settings()
    assert str(settings.controller_metrics_host) == "0.0.0.0"
    assert settings.controller_metrics_port == 9100


def test_controller_metrics_bind_accepts_loopback_override() -> None:
    settings = build_settings(
        controller_metrics_host="127.0.0.1", controller_metrics_port=9101
    )
    assert str(settings.controller_metrics_host) == "127.0.0.1"
    assert settings.controller_metrics_port == 9101


def test_controller_metrics_bind_loads_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FIRST_CONTROLLER_METRICS_HOST", "127.0.0.1")
    monkeypatch.setenv("FIRST_CONTROLLER_METRICS_PORT", "9101")
    settings = build_settings()
    assert str(settings.controller_metrics_host) == "127.0.0.1"
    assert settings.controller_metrics_port == 9101


@pytest.mark.parametrize(
    "value",
    ["localhost", "not-an-address", "::1", "10.0.0.1"],
)
def test_controller_metrics_bind_rejects_invalid_values(value: object) -> None:
    with pytest.raises(ValidationError):
        build_settings(controller_metrics_host=value)


@pytest.mark.parametrize("value", [0, 65536])
def test_controller_metrics_bind_rejects_invalid_ports(value: int) -> None:
    with pytest.raises(ValidationError):
        build_settings(controller_metrics_port=value)


@pytest.mark.asyncio
async def test_metrics_server_passes_reviewed_bind_to_uvicorn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, Any] = {}

    class FakeConfig:
        def __init__(self, app: object, **kwargs: object) -> None:
            observed.update(app=app, **kwargs)

    class FakeServer:
        def __init__(self, config: FakeConfig) -> None:
            observed["config"] = config
            self.install_signal_handlers = lambda: None

        async def serve(self) -> None:
            observed["served"] = True

    monkeypatch.setattr(uvicorn, "Config", FakeConfig)
    monkeypatch.setattr(uvicorn, "Server", FakeServer)

    await metrics_server.serve([], host="127.0.0.1", port=9101)

    assert observed["app"] is metrics_server.app
    assert observed["host"] == "127.0.0.1"
    assert observed["port"] == 9101
    assert observed["log_level"] == "warning"
    assert observed["served"] is True


@pytest.mark.asyncio
async def test_health_alerter_uses_configured_controller_port(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    urls: list[str] = []

    class FakeResponse:
        status_code = 200
        text = "ok"

    class FakeClient:
        def __init__(self, **_: object) -> None:
            pass

        async def get(self, url: str) -> FakeResponse:
            urls.append(url)
            return FakeResponse()

    class FakeProcess:
        async def communicate(self) -> tuple[bytes, bytes]:
            return b"Filesystem 1024-blocks Used Available Capacity Mounted on\n", b""

    async def fake_subprocess(*_: object, **__: object) -> FakeProcess:
        return FakeProcess()

    monkeypatch.setattr(checks, "AsyncClient", FakeClient)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_subprocess)
    state = cast(
        ClientState,
        SimpleNamespace(
            settings=SimpleNamespace(
                gateway_health_url="http://127.0.0.1:8001/health",
                controller_metrics_port=9101,
            )
        ),
    )

    assert await checks.check_host(state) == []
    assert urls == [
        "http://127.0.0.1:8001/health",
        "http://127.0.0.1:9101/healthz",
    ]
