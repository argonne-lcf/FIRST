"""Tests for the admin /catalog/v1/system-health snapshot."""

from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from first_common.schema.base_scheduler import SchedulerJobState
from first_gateway.database.models import Cluster, PilotJob
from first_gateway.database.redis.keys import Keys

from .fixtures.auth import ADMIN_TOKEN, auth_header
from .test_pilot_job_observer import _insert_pilot_job, _seed_cluster


async def _system_health(client: httpx.AsyncClient) -> dict[str, Any]:
    resp = await client.get(
        "/catalog/v1/system-health", headers=auth_header(ADMIN_TOKEN)
    )
    assert resp.status_code == 200, resp.text
    return resp.json()  # type: ignore[no-any-return]


async def _insert_dead_job(sess: AsyncSession, name: str, deleted_at: datetime) -> int:
    uid = await _insert_pilot_job(sess, name, f"{name}.pbs", SchedulerJobState.gone)
    await sess.execute(
        sa.update(PilotJob)
        .where(PilotJob.uid == uid)
        .values(
            deleted_at=deleted_at,
            failure_reason=f"{name} died (gone) before its manager came up",
        )
    )
    return uid


async def test_system_health_reports_backoff_and_pilot_start_failures(
    db: async_sessionmaker[AsyncSession], control_client: httpx.AsyncClient
) -> None:
    now = datetime.now(timezone.utc)
    async with db.begin() as sess:
        await _seed_cluster(sess)
        # Outside both the window and the current streak of 2: omitted.
        await _insert_dead_job(sess, "ancient", now - timedelta(days=4))
        # Outside the window, but within the streak.
        await _insert_dead_job(sess, "streak-old", now - timedelta(days=3))
        await _insert_dead_job(sess, "recent", now - timedelta(hours=1))
        await _insert_pilot_job(sess, "alive", "alive.pbs", SchedulerJobState.running)
        await sess.execute(
            sa.update(Cluster).values(
                pilot_start_failures=2,
                health="unhealthy",
                health_detail="Scheduler poll failed: boom",
                reconcile_failures=3,
                reconcile_last_error="boom (in PilotJobObserver._poll_cluster)",
                reconcile_last_traceback="Traceback ...\nConnectionError: boom",
                reconcile_retry_at=now + timedelta(minutes=5),
            )
        )

    health = await _system_health(control_client)

    [cluster] = health["clusters"]
    assert cluster["pilot_start_failures"] == 2
    assert cluster["max_consecutive_pilot_start_failures"] == 3
    assert cluster["reconcile_failures"] == 3
    assert cluster["reconcile_last_error"].endswith(
        "(in PilotJobObserver._poll_cluster)"
    )
    assert cluster["reconcile_last_traceback"].endswith("ConnectionError: boom")
    assert cluster["reconcile_retry_at"] is not None
    assert cluster["status"] == "unhealthy"
    assert cluster["health_detail"] == "Scheduler poll failed: boom"

    assert all(j["health_detail"] is None for j in health["pilot_jobs"])

    assert [j["name"] for j in health["pilot_jobs"]] == ["alive"]
    failed = health["failed_pilot_jobs"]
    assert [j["name"] for j in failed] == ["recent", "streak-old"]
    assert failed[0]["cluster_name"] == "polaris"
    assert failed[0]["scheduler_state"] == "gone"
    assert failed[0]["failure_reason"].startswith("recent died")


async def test_system_health_includes_health_alerter_state(
    db: async_sessionmaker[AsyncSession], control_client: httpx.AsyncClient
) -> None:
    from first_common.schema.resources.runtime import (
        CommittedAlert,
        HealthAlertState,
    )
    from first_gateway.controllers.control_server import app as control_app

    repo = control_app.state.client_state.redis_repo
    await repo.set_health_alert_state(
        HealthAlertState(
            committed={
                "disk:/dev/sda1": CommittedAlert(
                    key="disk:/dev/sda1",
                    status="low",
                    severity="warn",
                    summary="Disk /dev/sda1 is 91% full",
                    detail="/var/lib/docker: 40G",
                    group="Infrastructure",
                    owner="check_disk",
                )
            },
            reported_failures={"check_log_shipping": "connection refused"},
        )
    )
    try:
        alerter = (await _system_health(control_client))["health_alerter"]
    finally:
        await repo.client.delete(Keys.health_alert_state())

    alert = alerter["committed"]["disk:/dev/sda1"]
    assert (alert["severity"], alert["detail"]) == ("warn", "/var/lib/docker: 40G")
    assert alerter["reported_failures"] == {"check_log_shipping": "connection refused"}


async def test_system_health_without_health_alerter_state(
    db: async_sessionmaker[AsyncSession], control_client: httpx.AsyncClient
) -> None:
    alerter = (await _system_health(control_client))["health_alerter"]
    assert alerter["committed"] == {} and alerter["reported_failures"] == {}
