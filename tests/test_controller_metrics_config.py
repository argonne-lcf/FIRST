from __future__ import annotations

from typing import Any

import pytest
import uvicorn
from pydantic import ValidationError

from first_gateway.controllers import metrics_server
from first_gateway.settings import CONTROLLER_METRICS_PORT, Settings


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


def test_controller_metrics_bind_accepts_loopback_override() -> None:
    settings = build_settings(controller_metrics_host="127.0.0.1")
    assert str(settings.controller_metrics_host) == "127.0.0.1"


def test_controller_metrics_bind_loads_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FIRST_CONTROLLER_METRICS_HOST", "127.0.0.1")
    settings = build_settings()
    assert str(settings.controller_metrics_host) == "127.0.0.1"


@pytest.mark.parametrize(
    "value",
    ["localhost", "not-an-address", "::1", "10.0.0.1"],
)
def test_controller_metrics_bind_rejects_invalid_values(value: object) -> None:
    with pytest.raises(ValidationError):
        build_settings(controller_metrics_host=value)


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

    await metrics_server.serve([], host="127.0.0.1")

    assert observed["app"] is metrics_server.app
    assert observed["host"] == "127.0.0.1"
    assert observed["port"] == CONTROLLER_METRICS_PORT == 9100
    assert observed["log_level"] == "warning"
    assert observed["served"] is True
