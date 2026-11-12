"""Secrets never reach Slack: health-alert text is redacted before posting."""

import traceback
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import SecretStr

from first_gateway import Settings
from first_gateway.controllers.workers.health_alerter.redact import Redactor
from first_gateway.controllers.workers.health_alerter.slack import (
    build_alert_blocks,
    build_thread_blocks,
)
from first_gateway.controllers.workers.health_alerter.worker import HealthAlerter

GLOBUS_SECRET = "globus-planted-secret-0001"
KEYCLOAK_SECRET = "keycloak-planted-secret-0002"
DB_PASSWORD = "dbPlantedPass0003"
ENV_KEY = "sk-upstream-planted-key-0004"


def _settings() -> Settings:
    fields: dict[str, Any] = dict(
        db_url=f"postgresql+asyncpg://first:{DB_PASSWORD}@db:5432/first",
        redis_url="redis://redis:6379/0",
        globus={
            "app_id": "app",
            "app_secret": GLOBUS_SECRET,
            "admin_group": "admins",
            "policies": ["p"],
            "authorized_idp_domains": ["anl.gov"],
        },
        keycloak_clients={
            "metis": {
                "base_url": "https://kc",
                "realm": "r",
                "impersonation_client_id": "c",
                "impersonation_client_secret": KEYCLOAK_SECRET,
                "audience": "a",
            }
        },
        health_slack_bot_token="xoxb-planted-slack-token",
    )
    return Settings(_env_file=None, **fields)


@pytest.fixture
def redactor() -> Redactor:
    env = {
        "FIRST_UPSTREAM_KEY_METIS": ENV_KEY,
        "METIS_API_KEY_2": "metis-key-planted-0005",
        "PILOT_CLIENT_KEY_FILE": "/run/secrets/pilot_client.key",
        "HOME": "/home/planted-not-secret",
        "SHORT_TOKEN": "abc123",
    }
    return Redactor.from_settings(_settings(), environ=env)


def test_settings_secrets_masked_by_name(redactor: Redactor) -> None:
    text = f"boom {GLOBUS_SECRET} / {KEYCLOAK_SECRET} / xoxb-planted-slack-token"
    out = redactor.redact(text)
    assert GLOBUS_SECRET not in out and KEYCLOAK_SECRET not in out
    assert "xoxb-planted" not in out
    assert "***globus.app_secret***" in out
    assert "***keycloak_clients.metis.impersonation_client_secret***" in out
    assert "***health_slack_bot_token***" in out


def test_db_url_and_its_password_masked(redactor: Redactor) -> None:
    out = redactor.redact(f"could not connect: password {DB_PASSWORD} rejected")
    assert DB_PASSWORD not in out
    assert "***db_url.password***" in out

    # The whole URL is longer than the password, so it is masked first, whole.
    out = redactor.redact(
        f"engine postgresql+asyncpg://first:{DB_PASSWORD}@db:5432/first"
    )
    assert out == "engine ***db_url***"


def test_env_keys_masked_by_name(redactor: Redactor) -> None:
    out = redactor.redact(
        f"401 from upstream with key {ENV_KEY}; metis-key-planted-0005"
    )
    assert ENV_KEY not in out and "metis-key-planted-0005" not in out
    assert "***FIRST_UPSTREAM_KEY_METIS***" in out
    assert "***METIS_API_KEY_2***" in out


def test_short_and_non_secret_values_not_masked(redactor: Redactor) -> None:
    text = (
        "token abc123 in /home/planted-not-secret reading /run/secrets/pilot_client.key"
    )
    assert redactor.redact(text) == text


def test_longest_value_wins() -> None:
    r = Redactor({"outer-secret-inner-secret": "OUTER", "inner-secret": "INNER"})
    assert r.redact("x outer-secret-inner-secret y inner-secret") == (
        "x ***OUTER*** y ***INNER***"
    )


@pytest.mark.parametrize(
    ("text", "leaked", "expected"),
    [
        ("Authorization: Bearer eyJhbGciOi.payload.sig", "eyJhbG", "Bearer ***"),
        ("bearer abcdefghijk", "abcdefghijk", "Bearer ***"),
        ("redis://default:hunter2pass@redis:6379", "hunter2pass", "://***:***@redis"),
        ("https://:p4ss@host/x", "p4ss", "://***:***@host"),
        ("url?password=hunter2&x=1", "hunter2", "password=***&x=1"),
        ("client_secret=abc access_token=def", "abc", "client_secret=***"),
        ("API_KEY=zzz", "zzz", "API_KEY=***"),
        ('{"api_key": "zzz", "n": 1}', "zzz", '"api_key": "***"'),
        ("{'Password': 'p@ss w'}", "p@ss", "'Password': '***'"),
    ],
)
def test_patterns_masked(text: str, leaked: str, expected: str) -> None:
    out = Redactor({}).redact(text)
    assert leaked not in out
    assert expected in out


def test_pem_blocks_masked() -> None:
    body = "MIIEvQIBADANBgkqhkiG9w0BAQEFAASC\nBKcwggSjAgEAAoIBAQC7planted0000\n"
    pem = f"-----BEGIN PRIVATE KEY-----\n{body}-----END PRIVATE KEY-----"
    r = Redactor({})
    assert r.redact(f"before\n{pem}\nafter") == "before\n***PEM***\nafter"
    # Truncation can cut a block in half; neither half leaks key material.
    head = r.redact(f"x\n-----BEGIN PRIVATE KEY-----\n{body}")
    tail = r.redact(f"…(truncated)\n{body}-----END PRIVATE KEY-----\n```")
    assert "planted" not in head and "MIIE" not in head
    assert "planted" not in tail and "MIIE" not in tail
    assert tail.endswith("```")


def test_ordinary_text_unchanged(redactor: Redactor) -> None:
    try:
        {"a": 1}["missing_token_key"]
    except KeyError:
        tb = traceback.format_exc()
    alert = (
        "🔴 cl/pilot-job/rec: reconcile failing (3 retries): boom "
        "(in PilotJobController.reconcile)\nqsub: Job exceeds queue resource limits "
        "http://prometheus:9090/api/v1/query max_tokens=100 tokens: 5"
    )
    for text in (tb, alert):
        assert redactor.redact(text) == text


def test_redact_blocks_covers_every_text() -> None:
    r = Redactor({"planted-secret-value": "S"})
    blocks = build_alert_blocks([], [], [("check_x", "fail planted-secret-value")])
    blocks += build_thread_blocks([("check_x", "Traceback planted-secret-value")])
    blocks.append(
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "planted-secret-value"}],
        }
    )
    out = r.redact_blocks(blocks)
    assert "planted-secret-value" not in repr(out)
    assert "planted-secret-value" in repr(blocks)  # input not mutated
    assert out[0] == blocks[0]  # header without secrets is untouched


def _alerter() -> HealthAlerter:
    cs = MagicMock()
    cs.settings = MagicMock()
    cs.settings.health_slack_bot_token = SecretStr("xoxb-test-token")
    cs.settings.health_slack_channel = "C0TEST"
    alerter = HealthAlerter("health-alerter", cs, MagicMock())
    alerter._redactor = Redactor({"planted-secret-value": "PLANTED"})
    return alerter


async def test_post_message_redacts_header_summary_and_thread() -> None:
    alerter = _alerter()
    header = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "x planted-secret-value"},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "summary planted-secret-value"},
        },
    ]
    thread = build_thread_blocks([("detail", "Traceback planted-secret-value")])
    resp = MagicMock()
    resp.json.return_value = {"ok": True, "ts": "1.2"}
    with patch.object(alerter.http, "post", new_callable=AsyncMock) as post:
        post.return_value = resp
        await alerter._post_message(header, text="x planted-secret-value")
        await alerter._post_message(thread, thread_ts="1.2", text="Details")

    payloads = [c.kwargs["json"] for c in post.call_args_list]
    assert "planted-secret-value" not in repr(payloads)
    assert payloads[0]["text"] == "x ***PLANTED***"
    assert payloads[0]["blocks"][0]["text"]["text"] == "x ***PLANTED***"
    assert payloads[0]["blocks"][1]["text"]["text"] == "summary ***PLANTED***"
    assert "Traceback ***PLANTED***" in payloads[1]["blocks"][0]["text"]["text"]
    # The bot token itself still authenticates the request.
    assert post.call_args.kwargs["headers"]["Authorization"] == "Bearer xoxb-test-token"
