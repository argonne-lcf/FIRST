"""Transport receipts without database fixtures or real Slack messages."""

from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock

import httpx
import pytest
from pydantic import SecretStr

from first_gateway.controllers.workers.health_alerter.worker import HealthAlerter
from first_gateway.settings import ClientState


@pytest.mark.asyncio
async def test_actual_slack_ack_is_logged_without_secrets_or_payload(
    caplog: pytest.LogCaptureFixture,
) -> None:
    worker = object.__new__(HealthAlerter)
    worker.client_state = cast(
        ClientState,
        SimpleNamespace(
            settings=SimpleNamespace(
                health_slack_bot_token=SecretStr("test-token-never-log"),
                health_slack_channel="test-channel-never-log",
            )
        ),
    )
    response = httpx.Response(200, json={"ok": True, "ts": "1791663000.123456"})
    worker.http = cast(
        httpx.AsyncClient, SimpleNamespace(post=AsyncMock(return_value=response))
    )
    with caplog.at_level("INFO"):
        result = await worker._post_message(
            [{"type": "section", "text": "test-private-details-never-log"}]
        )
    assert result == "1791663000.123456"
    assert "ts=1791663000.123456 threaded=False" in caplog.text
    for private in (
        "test-token-never-log",
        "test-channel-never-log",
        "test-private-details-never-log",
    ):
        assert private not in caplog.text


@pytest.mark.asyncio
async def test_slack_failure_does_not_emit_delivery_ack(
    caplog: pytest.LogCaptureFixture,
) -> None:
    worker = object.__new__(HealthAlerter)
    worker.client_state = cast(
        ClientState,
        SimpleNamespace(
            settings=SimpleNamespace(
                health_slack_bot_token=SecretStr("test-token-never-log"),
                health_slack_channel="test-channel-never-log",
            )
        ),
    )
    response = httpx.Response(200, json={"ok": False, "error": "not_in_channel"})
    worker.http = cast(
        httpx.AsyncClient, SimpleNamespace(post=AsyncMock(return_value=response))
    )
    with caplog.at_level("INFO"):
        assert await worker._post_message([]) is None
    assert "delivery acknowledged" not in caplog.text
    assert "test-token-never-log" not in caplog.text
