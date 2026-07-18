"""Tests for PilotJobController: lifecycle management of HPC pilot jobs."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Self
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from first_common.schema.base_scheduler import (
    JobSubmitPayload,
    JobSubmitResult,
    SchedulerAdapter,
    SchedulerJobState,
)
from first_common.schema.types import HealthCheckResult
from first_gateway.controllers.controller import StaleReconcile
from first_gateway.controllers.workers.pilot_job_controller import PilotJobController
from first_gateway.database.models import Cluster, PilotJob
from first_gateway.services.certmanager import gen_ca_pem

_PATCH_BUILD = "first_gateway.controllers.workers.pilot_job_controller.build_scheduler"

NOW = datetime(2026, 7, 12, 12, 0, 0, tzinfo=timezone.utc)


class FakeSchedulerAdapter(SchedulerAdapter):
    """In-memory adapter that records terminate and submit calls."""

    def __init__(self) -> None:
        self.files: dict[str, tuple[str, int]] = {}
        self.terminated: list[str] = []
        self.submitted: list[JobSubmitPayload] = []

    @classmethod
    async def build(cls, _client_state: Any, _config: dict[str, Any]) -> Self:
        return cls()

    async def submit_job(self, job_spec: JobSubmitPayload) -> JobSubmitResult:
        self.submitted.append(job_spec)
        return JobSubmitResult(job_name=job_spec.name, scheduler_id="42.pbs")

    async def get_job_statuses(self) -> list[Any]:
        return []

    async def terminate_job(self, job_id: str) -> None:
        self.terminated.append(job_id)

    async def put_file(self, content: str, path: Path, mode: int) -> None:
        self.files[str(path)] = (content, mode)

    async def list_files(self, directory: Path) -> list[str]:
        return []

    async def read_file(self, path: Path) -> str:
        return self.files[str(path)][0]


PILOT_SYSTEM: dict[str, Any] = {
    "scheduler_adapter": (
        "first_gateway.platforms.schedulers.globus_compute_pbs.GlobusComputePBSAdapter"
    ),
    "scheduler_config": {},
    "job_walltime_min": 60,
    "pilot_max_idle_time_min": 60,
    "pilot_max_unhealthy_time_min": 5,
    "max_concurrent_jobs": 3,
    "max_num_nodes": 10,
    "queue": "debug",
    "account": "TestAcct",
    "scheduler_flags": "",
    "workdir": "/tmp/pilot_workdir",
    "external_port": 8443,
    "nginx_path": "/usr/sbin/nginx",
    "ip_allowlist": ["10.0.0.0/8"],
    "node_file_env": "PBS_NODEFILE",
    "submit_script_preamble": "#!/bin/bash",
    "pilot_version": "0.1.0",
}


@pytest.fixture(scope="module")
def ca_pair() -> tuple[str, str]:
    return gen_ca_pem(name="test-ca")


@pytest.fixture
def adapter() -> FakeSchedulerAdapter:
    return FakeSchedulerAdapter()


def _make_client_state(
    db: async_sessionmaker[AsyncSession],
    ca_pair: tuple[str, str],
) -> MagicMock:
    ca_crt, ca_key = ca_pair
    settings = MagicMock()
    settings.pilot_ca_crt = ca_crt
    settings.pilot_ca_key.get_secret_value.return_value = ca_key
    cs = MagicMock()
    cs.db_sessionmaker = db
    cs.settings = settings
    return cs


def _make_controller(
    db: async_sessionmaker[AsyncSession],
    ca_pair: tuple[str, str],
) -> PilotJobController:
    return PilotJobController(
        "pilot-job-controller", _make_client_state(db, ca_pair), MagicMock()
    )


async def _seed_cluster(
    sess: AsyncSession, pilot_system: dict[str, Any] | None = None
) -> None:
    sess.add(
        Cluster(
            name="polaris",
            health_check={"url": "http://x/health", "debounce": 2},
            pilot_system=pilot_system if pilot_system is not None else PILOT_SYSTEM,
        )
    )
    await sess.flush()


async def _insert_pilot_job(
    sess: AsyncSession,
    name: str,
    *,
    cluster_name: str = "polaris",
    scheduler_job_id: str | None = None,
    scheduler_state: SchedulerJobState = SchedulerJobState.running,
    scheduled_deletion_at: datetime | None = None,
    deleted_at: datetime | None = None,
    idle_since: datetime | None = None,
    manager_health: str = HealthCheckResult.unknown.value,
    manager_unhealthy_since: datetime | None = None,
    num_nodes: int = 1,
    reconcile_retry_at: datetime | None = None,
) -> int:
    job = PilotJob(
        name=name,
        cluster_name=cluster_name,
        scheduler_job_id=scheduler_job_id,
        scheduler_state=scheduler_state.value,
        scheduled_deletion_at=scheduled_deletion_at,
        deleted_at=deleted_at,
        idle_since=idle_since,
        manager_health=manager_health,
        manager_unhealthy_since=manager_unhealthy_since,
        walltime_min=60,
        num_nodes=num_nodes,
        gpus_per_node=4,
        reconcile_retry_at=reconcile_retry_at,
    )
    sess.add(job)
    await sess.flush()
    return job.uid


async def _get_job(db: async_sessionmaker[AsyncSession], uid: int) -> PilotJob:
    async with db() as sess:
        job = await sess.get(PilotJob, uid)
    assert job is not None
    return job


# ---------------------------------------------------------------------------
# list_actionable
# ---------------------------------------------------------------------------


async def test_list_actionable(
    db: async_sessionmaker[AsyncSession], ca_pair: tuple[str, str]
) -> None:
    async with db.begin() as sess:
        await _seed_cluster(sess)
        uid_sched_del = await _insert_pilot_job(
            sess, "sched-del", scheduled_deletion_at=NOW
        )
        uid_pending = await _insert_pilot_job(
            sess, "pending", scheduler_state=SchedulerJobState.pending_submit
        )
        uid_idle = await _insert_pilot_job(sess, "idle", idle_since=NOW)
        uid_unhealthy = await _insert_pilot_job(
            sess,
            "unhealthy",
            manager_health=HealthCheckResult.unhealthy.value,
        )
        uid_exiting = await _insert_pilot_job(
            sess, "exiting", scheduler_state=SchedulerJobState.exiting
        )
        # NOT actionable: running with no issues
        await _insert_pilot_job(sess, "healthy-running")
        # NOT actionable: queued with no issues
        await _insert_pilot_job(
            sess, "queued", scheduler_state=SchedulerJobState.queued
        )
        # NOT actionable: already soft-deleted
        await _insert_pilot_job(
            sess,
            "deleted",
            scheduled_deletion_at=NOW,
            deleted_at=NOW,
        )
        # NOT actionable: retry in the future
        await _insert_pilot_job(
            sess,
            "backoff",
            scheduler_state=SchedulerJobState.pending_submit,
            reconcile_retry_at=datetime.now(timezone.utc) + timedelta(hours=1),
        )

    controller = _make_controller(db, ca_pair)
    async with db() as sess:
        actionable = await controller.list_actionable(sess)

    assert set(actionable) == {
        uid_sched_del,
        uid_pending,
        uid_idle,
        uid_unhealthy,
        uid_exiting,
    }


# ---------------------------------------------------------------------------
# reconcile: edge cases
# ---------------------------------------------------------------------------


async def test_reconcile_missing_job_is_noop(
    db: async_sessionmaker[AsyncSession], ca_pair: tuple[str, str]
) -> None:
    async with db.begin() as sess:
        await _seed_cluster(sess)

    controller = _make_controller(db, ca_pair)
    await controller.reconcile(999_999)


async def test_reconcile_cluster_without_pilot_system(
    db: async_sessionmaker[AsyncSession], ca_pair: tuple[str, str]
) -> None:
    """Jobs on a cluster whose pilot_system was removed are skipped."""
    async with db.begin() as sess:
        sess.add(
            Cluster(
                name="bare-cluster",
                health_check={"url": "http://x/health", "debounce": 2},
                pilot_system=None,
            )
        )
        await sess.flush()
        uid = await _insert_pilot_job(
            sess,
            "orphan",
            cluster_name="bare-cluster",
            scheduler_state=SchedulerJobState.pending_submit,
        )

    controller = _make_controller(db, ca_pair)
    await controller.reconcile(uid)

    job = await _get_job(db, uid)
    assert job.scheduler_state == SchedulerJobState.pending_submit.value


# ---------------------------------------------------------------------------
# reconcile: scheduled_deletion  →  _terminate_and_delete
# ---------------------------------------------------------------------------


async def test_scheduled_deletion_terminates_running_job(
    db: async_sessionmaker[AsyncSession],
    ca_pair: tuple[str, str],
    adapter: FakeSchedulerAdapter,
) -> None:
    async with db.begin() as sess:
        await _seed_cluster(sess)
        uid = await _insert_pilot_job(
            sess,
            "job-to-kill",
            scheduler_job_id="100.pbs",
            scheduled_deletion_at=NOW,
        )

    controller = _make_controller(db, ca_pair)
    with patch(_PATCH_BUILD, new_callable=AsyncMock, return_value=adapter):
        await controller.reconcile(uid)

    assert "100.pbs" in adapter.terminated

    job = await _get_job(db, uid)
    assert job.deleted_at is not None


async def test_scheduled_deletion_without_scheduler_job_skips_terminate(
    db: async_sessionmaker[AsyncSession],
    ca_pair: tuple[str, str],
    adapter: FakeSchedulerAdapter,
) -> None:
    """A job that was never submitted can still be soft-deleted."""
    async with db.begin() as sess:
        await _seed_cluster(sess)
        uid = await _insert_pilot_job(
            sess,
            "never-submitted",
            scheduler_state=SchedulerJobState.pending_submit,
            scheduled_deletion_at=NOW,
        )

    controller = _make_controller(db, ca_pair)
    with patch(_PATCH_BUILD, new_callable=AsyncMock, return_value=adapter):
        await controller.reconcile(uid)

    assert adapter.terminated == []

    job = await _get_job(db, uid)
    assert job.deleted_at is not None


async def test_scheduled_deletion_skips_terminate_when_already_terminal(
    db: async_sessionmaker[AsyncSession],
    ca_pair: tuple[str, str],
    adapter: FakeSchedulerAdapter,
) -> None:
    """No terminate RPC when the scheduler already reports the job as gone."""
    async with db.begin() as sess:
        await _seed_cluster(sess)
        uid = await _insert_pilot_job(
            sess,
            "already-gone",
            scheduler_job_id="200.pbs",
            scheduler_state=SchedulerJobState.gone,
            scheduled_deletion_at=NOW,
        )

    controller = _make_controller(db, ca_pair)
    with patch(_PATCH_BUILD, new_callable=AsyncMock, return_value=adapter):
        await controller.reconcile(uid)

    assert adapter.terminated == []

    job = await _get_job(db, uid)
    assert job.deleted_at is not None


# ---------------------------------------------------------------------------
# reconcile: terminal state  →  _mark_scheduled_deletion
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("state", [SchedulerJobState.exiting, SchedulerJobState.gone])
async def test_terminal_state_marks_scheduled_deletion(
    db: async_sessionmaker[AsyncSession],
    ca_pair: tuple[str, str],
    state: SchedulerJobState,
) -> None:
    async with db.begin() as sess:
        await _seed_cluster(sess)
        uid = await _insert_pilot_job(
            sess, f"terminal-{state.value}", scheduler_state=state
        )

    controller = _make_controller(db, ca_pair)
    await controller.reconcile(uid)

    job = await _get_job(db, uid)
    assert job.scheduled_deletion is True


# ---------------------------------------------------------------------------
# reconcile: idle timeout
# ---------------------------------------------------------------------------


async def test_idle_past_threshold_marks_deletion(
    db: async_sessionmaker[AsyncSession], ca_pair: tuple[str, str]
) -> None:
    async with db.begin() as sess:
        await _seed_cluster(sess)
        uid = await _insert_pilot_job(
            sess,
            "idle-job",
            idle_since=datetime.now(timezone.utc) - timedelta(minutes=90),
        )

    controller = _make_controller(db, ca_pair)
    await controller.reconcile(uid)

    job = await _get_job(db, uid)
    assert job.scheduled_deletion is True


async def test_idle_within_threshold_no_action(
    db: async_sessionmaker[AsyncSession], ca_pair: tuple[str, str]
) -> None:
    async with db.begin() as sess:
        await _seed_cluster(sess)
        uid = await _insert_pilot_job(
            sess,
            "not-yet-idle",
            idle_since=datetime.now(timezone.utc) - timedelta(minutes=30),
        )

    controller = _make_controller(db, ca_pair)
    await controller.reconcile(uid)

    job = await _get_job(db, uid)
    assert job.scheduled_deletion is False


# ---------------------------------------------------------------------------
# reconcile: unhealthy timeout
# ---------------------------------------------------------------------------


async def test_unhealthy_past_threshold_marks_deletion(
    db: async_sessionmaker[AsyncSession], ca_pair: tuple[str, str]
) -> None:
    async with db.begin() as sess:
        await _seed_cluster(sess)
        uid = await _insert_pilot_job(
            sess,
            "sick-job",
            manager_health=HealthCheckResult.unhealthy.value,
            manager_unhealthy_since=datetime.now(timezone.utc) - timedelta(minutes=10),
        )

    controller = _make_controller(db, ca_pair)
    await controller.reconcile(uid)

    job = await _get_job(db, uid)
    assert job.scheduled_deletion is True


async def test_unhealthy_within_threshold_no_action(
    db: async_sessionmaker[AsyncSession], ca_pair: tuple[str, str]
) -> None:
    async with db.begin() as sess:
        await _seed_cluster(sess)
        uid = await _insert_pilot_job(
            sess,
            "briefly-sick",
            manager_health=HealthCheckResult.unhealthy.value,
            manager_unhealthy_since=datetime.now(timezone.utc) - timedelta(minutes=2),
        )

    controller = _make_controller(db, ca_pair)
    await controller.reconcile(uid)

    job = await _get_job(db, uid)
    assert job.scheduled_deletion is False


# ---------------------------------------------------------------------------
# reconcile: pending_submit  →  _submit
# ---------------------------------------------------------------------------


async def test_submit_under_caps(
    db: async_sessionmaker[AsyncSession],
    ca_pair: tuple[str, str],
    adapter: FakeSchedulerAdapter,
) -> None:
    """A pending job is submitted when under both caps."""
    async with db.begin() as sess:
        await _seed_cluster(sess)
        uid = await _insert_pilot_job(
            sess,
            "new-job",
            scheduler_state=SchedulerJobState.pending_submit,
            num_nodes=2,
        )

    controller = _make_controller(db, ca_pair)
    with patch(_PATCH_BUILD, new_callable=AsyncMock, return_value=adapter):
        await controller.reconcile(uid)

    assert len(adapter.submitted) == 1
    assert adapter.submitted[0].num_nodes == 2

    job = await _get_job(db, uid)
    assert job.scheduler_job_id == "42.pbs"
    assert job.scheduler_state == SchedulerJobState.queued.value


async def test_submit_deferred_by_concurrent_jobs_cap(
    db: async_sessionmaker[AsyncSession],
    ca_pair: tuple[str, str],
    adapter: FakeSchedulerAdapter,
) -> None:
    """Submission is deferred when max_concurrent_jobs (3) is exceeded."""
    async with db.begin() as sess:
        await _seed_cluster(sess)
        for i in range(3):
            await _insert_pilot_job(sess, f"running-{i}")
        uid = await _insert_pilot_job(
            sess,
            "waiting",
            scheduler_state=SchedulerJobState.pending_submit,
        )

    controller = _make_controller(db, ca_pair)
    with patch(_PATCH_BUILD, new_callable=AsyncMock, return_value=adapter):
        await controller.reconcile(uid)

    assert adapter.submitted == []

    job = await _get_job(db, uid)
    assert job.scheduler_job_id is None
    assert job.scheduler_state == SchedulerJobState.pending_submit.value


async def test_submit_deferred_by_node_cap(
    db: async_sessionmaker[AsyncSession],
    ca_pair: tuple[str, str],
    adapter: FakeSchedulerAdapter,
) -> None:
    """Submission is deferred when max_num_nodes (10) is exceeded."""
    async with db.begin() as sess:
        await _seed_cluster(sess)
        await _insert_pilot_job(sess, "big-job", num_nodes=9)
        uid = await _insert_pilot_job(
            sess,
            "too-big",
            scheduler_state=SchedulerJobState.pending_submit,
            num_nodes=2,
        )

    controller = _make_controller(db, ca_pair)
    with patch(_PATCH_BUILD, new_callable=AsyncMock, return_value=adapter):
        await controller.reconcile(uid)

    assert adapter.submitted == []

    job = await _get_job(db, uid)
    assert job.scheduler_job_id is None


# ---------------------------------------------------------------------------
# Premise safety: condition resolved between read and write
# ---------------------------------------------------------------------------


async def test_idle_premise_prevents_stale_mark(
    db: async_sessionmaker[AsyncSession], ca_pair: tuple[str, str]
) -> None:
    """If idle_since is cleared between the read and write phase, the
    premised UPDATE correctly rejects the stale mark."""
    async with db.begin() as sess:
        await _seed_cluster(sess)
        uid = await _insert_pilot_job(
            sess,
            "idle-race",
            idle_since=datetime.now(timezone.utc) - timedelta(hours=2),
        )

    # Load a detached snapshot (simulates the controller's read phase)
    async with db() as sess:
        job = await sess.get(PilotJob, uid)
    assert job is not None

    # Simulate the observer clearing idle_since concurrently
    async with db.begin() as sess:
        await sess.execute(
            sa.update(PilotJob).where(PilotJob.uid == uid).values(idle_since=None)
        )

    controller = _make_controller(db, ca_pair)
    with pytest.raises(StaleReconcile):
        await controller._mark_scheduled_deletion(job, PilotJob.idle_since.is_not(None))

    job = await _get_job(db, uid)
    assert job.scheduled_deletion is False


async def test_unhealthy_premise_prevents_stale_mark(
    db: async_sessionmaker[AsyncSession], ca_pair: tuple[str, str]
) -> None:
    """If manager recovers between read and write, the premised UPDATE
    correctly rejects the stale mark."""
    async with db.begin() as sess:
        await _seed_cluster(sess)
        uid = await _insert_pilot_job(
            sess,
            "unhealthy-race",
            manager_health=HealthCheckResult.unhealthy.value,
            manager_unhealthy_since=datetime.now(timezone.utc) - timedelta(minutes=10),
        )

    async with db() as sess:
        job = await sess.get(PilotJob, uid)
    assert job is not None

    # Simulate the observer marking the manager as healthy
    async with db.begin() as sess:
        await sess.execute(
            sa.update(PilotJob)
            .where(PilotJob.uid == uid)
            .values(
                manager_health=HealthCheckResult.healthy.value,
                manager_unhealthy_since=None,
            )
        )

    controller = _make_controller(db, ca_pair)
    with pytest.raises(StaleReconcile):
        await controller._mark_scheduled_deletion(
            job,
            PilotJob.manager_health == HealthCheckResult.unhealthy.value,
        )

    job = await _get_job(db, uid)
    assert job.scheduled_deletion is False
