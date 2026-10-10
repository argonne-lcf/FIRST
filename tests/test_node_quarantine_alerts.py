"""Durable transition notifications and exact consumed screen outcomes."""

import time
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from first_common.schema.quarantine import QuarantineReport, QuarantineScreenPayload
from first_common.schema.quarantine_read import NodeQuarantineSummary
from first_common.schema.resources.runtime import HealthAlertState
from first_gateway.controllers.workers.health_alerter.checks import (
    check_node_quarantine,
)
from first_gateway.controllers.workers.health_alerter.worker import advance
from first_gateway.database.models import Cluster, QuarantineReportInbox
from first_gateway.services.quarantine import transition

from .test_node_quarantine import (
    COMMIT,
    HOST,
    IDENTITY,
    OPERATION,
    POLICY_JSON,
    POLICY_SHA,
    SOURCE,
    mock_service,
    pilot,
    row,
    status,
)


async def observations(node: Any) -> list[Any]:
    service, session, _ = mock_service(node)
    values, empty = MagicMock(), MagicMock()
    values.all.return_value, empty.all.return_value = [node], []
    session.scalars.side_effect = [values, empty]
    result = await check_node_quarantine(service.clients)
    return [
        observation
        for observation in result
        if observation.key.startswith("node-quarantine/event/")
    ]


async def test_brief_pending_survives_until_alert_poll_and_deduplicates() -> None:
    node = row(
        state="isolated",
        evidence={"path": "/evidence/DEV-QUAL-SYNTHETIC-run", "report_id": "fault.pbs"},
    )
    facts = await observations(node)
    assert len(facts) == 1 and facts[0].status == "fault_detected:fault.pbs"
    assert "SYNTHETIC DEV QUALIFICATION" in facts[0].summary
    assert "current state isolated" in facts[0].summary
    facts[0] = facts[0].__class__(
        **{**facts[0].__dict__, "owner": "check_node_quarantine"}
    )
    state = HealthAlertState()
    current = datetime.now(timezone.utc)
    assert not advance(
        state, facts, {"check_node_quarantine"}, current, timedelta(seconds=110)
    ).degradations
    delivered = advance(
        state,
        facts,
        {"check_node_quarantine"},
        current + timedelta(seconds=30),
        timedelta(seconds=110),
    )
    assert len(delivered.degradations) == 1
    from first_gateway.controllers.workers.health_alerter.worker import HealthAlerter

    HealthAlerter._commit(state, delivered)
    assert not advance(
        state,
        facts,
        {"check_node_quarantine"},
        current + timedelta(seconds=60),
        timedelta(seconds=110),
    ).degradations


async def test_failed_and_lost_facts_survive_advance_without_false_recovery() -> None:
    node = row(state="pending")
    transition(node, "failed", "explicit failure")
    transition(node, "pending", "reconcile progressing")
    transition(node, "lost", "allocation disappeared")
    transition(node, "pending", "explicit reestablish")
    node.operation = "f" * 32
    transition(node, "isolated", "running again")
    before = await observations(node)
    assert {fact.status for fact in before} == {
        "failed:" + OPERATION,
        "lost:" + OPERATION,
    }
    assert all("current state isolated" in fact.summary for fact in before)
    node.state = "released"
    after = await observations(node)
    assert {fact.key for fact in after} == {fact.key for fact in before}
    assert {fact.status for fact in after} == {fact.status for fact in before}
    assert all("current state released" in fact.summary for fact in after)


def negative_screen(**changes: Any) -> QuarantineScreenPayload:
    payload: dict[str, Any] = dict(
        schema_version=3,
        event="screen_result",
        environment="dev",
        cluster="tara",
        operation=OPERATION,
        origin=SOURCE.model_dump(mode="json"),
        host=HOST,
        observed_unix_ns=time.time_ns(),
        quarantine_job_id=IDENTITY.job_id,
        nonce=OPERATION,
        outcome="FAIL",
        reports=[],
        confirmation=[],
        master_hsn=None,
        bundle_commit=COMMIT,
        policy_sha256=POLICY_SHA,
        policy_json=POLICY_JSON,
        error="ValueError",
    )
    payload.update(changes)
    return QuarantineScreenPayload.model_validate(payload)


async def test_valid_negative_screen_persists_without_repair_or_release() -> None:
    node = row(
        screen_nonce=OPERATION,
        screen_requested_at=datetime.now(timezone.utc) - timedelta(seconds=1),
    )
    service, session, adapter = mock_service(node)
    session.scalar.return_value = node
    report = QuarantineReport(
        "screen.pbs", "screen", SOURCE.ctime, negative_screen(), "1" * 64
    )
    await service._worker_report(Cluster(uid=1, name="tara"), pilot(), adapter, report)
    saved = session.execute.await_args.args[0].compile().params["screen_result"]
    assert saved["outcome"] == "FAIL" and node.state == "isolated"
    node.screen_result = saved
    node.repair_reference = "OPS-123"
    session.scalar.return_value = Cluster(
        uid=1, name="tara", pilot_system=pilot().model_dump(mode="json")
    )
    with pytest.raises(ValueError, match="PASS screen"):
        await service.request_release(node.uid, "OPS-123")
    adapter.delete_quarantine.assert_not_awaited()


@pytest.mark.parametrize(
    "changes",
    [
        {"nonce": "f" * 32},
        {"policy_json": POLICY_JSON + " "},
        {"error": ""},
        {"observed_unix_ns": time.time_ns() - 121_000_000_000},
    ],
)
async def test_unbound_negative_screens_remain_rejected(
    changes: dict[str, Any],
) -> None:
    node = row(
        screen_nonce=OPERATION,
        screen_requested_at=datetime.now(timezone.utc) - timedelta(seconds=200),
    )
    service, session, adapter = mock_service(node)
    session.scalar.return_value = node
    report = QuarantineReport(
        "screen.pbs", "screen", SOURCE.ctime, negative_screen(**changes), "1" * 64
    )
    with pytest.raises(ValueError):
        await service._worker_report(
            Cluster(uid=1, name="tara"), pilot(), adapter, report
        )
    session.execute.assert_not_awaited()
    adapter.acknowledge_quarantine_screen_request.assert_not_awaited()


@pytest.mark.parametrize("validation_error", [None, "unbound invalid screen"])
async def test_request_cleanup_follows_valid_persistence_and_precedes_report_ack(
    validation_error: str | None,
) -> None:
    service, session, adapter = mock_service(row())
    report = QuarantineReport(
        "screen.pbs", "screen", SOURCE.ctime, negative_screen(), "1" * 64
    )
    inbox = QuarantineReportInbox(
        uid=2,
        name="screen",
        cluster_name="tara",
        environment="dev",
        report_job_id=report.job_id,
        fingerprint=report.fingerprint,
        payload=report.payload.model_dump(mode="json"),
        processed_at=datetime.now(timezone.utc),
        acknowledged_at=None,
        validation_error=validation_error,
        reconcile_failures=0,
        reconcile_retry_at=None,
    )
    session.scalar.side_effect = [2, inbox]
    calls: list[str] = []
    adapter.acknowledge_quarantine_screen_request.side_effect = lambda _: calls.append(
        "request"
    )
    adapter.acknowledge_quarantine_report.side_effect = lambda _: calls.append("report")
    await service.ingest_report(Cluster(uid=1, name="tara"), pilot(), adapter, report)
    assert calls == (["request", "report"] if validation_error is None else ["report"])


def test_summary_exposes_screen_status_not_policy_body() -> None:
    node = row(screen_result=negative_screen().model_dump(mode="json"))
    summary = NodeQuarantineSummary.model_validate(node).model_dump(mode="json")
    assert summary["screen_outcome"] == "FAIL" and summary["screened_at"] is not None
    assert "screen_result" not in summary and "policy_json" not in summary


async def test_late_screen_cannot_overwrite_a_new_explicit_nonce() -> None:
    submitted = datetime.now(timezone.utc) - timedelta(seconds=1)
    snapshot = row(screen_nonce=OPERATION, screen_requested_at=submitted)
    live = row(screen_nonce=OPERATION, screen_requested_at=submitted)
    service, session, adapter = mock_service(snapshot)
    session.scalar.return_value = snapshot

    async def concurrent_new_screen(_: str) -> Any:
        live.screen_nonce = "f" * 32
        return status()

    async def guarded_update(statement: Any) -> Any:
        parameters = statement.compile().params
        assert parameters["screen_nonce_1"] == OPERATION
        if live.screen_nonce == parameters["screen_nonce_1"]:
            live.screen_result = parameters["screen_result"]
        return MagicMock(rowcount=0)

    adapter.get_quarantine_status.side_effect = concurrent_new_screen
    session.execute.side_effect = guarded_update
    report = QuarantineReport(
        "screen.pbs", "screen", SOURCE.ctime, negative_screen(), "1" * 64
    )
    await service._worker_report(Cluster(uid=1, name="tara"), pilot(), adapter, report)
    assert live.screen_nonce == "f" * 32 and live.screen_result is None


async def test_screen_timeout_alert_is_reporting_only_and_does_not_resubmit() -> None:
    node = row(
        screen_nonce=OPERATION,
        screen_requested_at=datetime.now(timezone.utc) - timedelta(seconds=180),
        screen_submission_started_at=datetime.now(timezone.utc)
        - timedelta(seconds=180),
    )
    service, session, adapter = mock_service(node)
    values, empty = MagicMock(), MagicMock()
    values.all.return_value, empty.all.return_value = [node], []
    session.scalars.side_effect = [values, empty]
    observed = await check_node_quarantine(service.clients)
    reporting = [
        item for item in observed if item.key.startswith("node-quarantine/screen/")
    ]
    assert len(reporting) == 1 and reporting[0].status.startswith("timeout:")
    assert "infrastructure timeout" in reporting[0].summary
    assert "reporting issue only" in reporting[0].recovery_hint
    assert node.state == "isolated"
    adapter.request_quarantine_screen.assert_not_awaited()
    adapter.delete_quarantine.assert_not_awaited()
