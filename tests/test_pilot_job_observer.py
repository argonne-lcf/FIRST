"""Tests for PilotJobObserver: scheduler state sync and endpoint discovery."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Self
from unittest.mock import AsyncMock, MagicMock, patch

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from first_common.schema.base_scheduler import (
    JobStatusInfo,
    JobSubmitPayload,
    JobSubmitResult,
    SchedulerAdapter,
    SchedulerJobState,
)
from first_common.schema.pilot import AddressInfo
from first_common.schema.types import HealthCheckResult, PilotConfig
from first_gateway.controllers.workers.pilot_job_observer import PilotJobObserver
from first_gateway.database.models import Cluster, PilotJob
from first_gateway.database.redis.pubsub import Channel
from first_gateway.services.pilot_submitter import PilotSubmitter

_PATCH_BUILD = "first_gateway.controllers.workers.pilot_job_observer.build_scheduler"

NOW = datetime(2026, 7, 12, 12, 0, 0, tzinfo=timezone.utc)


class FakeSchedulerAdapter(SchedulerAdapter):
    """In-memory adapter that records calls and returns canned results."""

    def __init__(self) -> None:
        self.files: dict[str, tuple[str, int]] = {}
        self.directories: dict[str, list[str]] = {}
        self.statuses: list[JobStatusInfo] = []
        self.terminated: list[str] = []

    @classmethod
    async def build(cls, _client_state: Any, _config: dict[str, Any]) -> Self:
        return cls()

    async def submit_job(self, job_spec: JobSubmitPayload) -> JobSubmitResult:
        raise NotImplementedError

    async def get_job_statuses(self) -> list[JobStatusInfo]:
        return list(self.statuses)

    async def terminate_job(self, job_id: str) -> None:
        self.terminated.append(job_id)

    async def put_file(self, content: str, path: Path, mode: int) -> None:
        self.files[str(path)] = (content, mode)

    async def list_files(self, directory: Path) -> list[str]:
        return list(self.directories.get(str(directory), []))

    async def read_file(self, path: Path) -> str:
        return self.files[str(path)][0]


WORKDIR = "/tmp/pilot_workdir"

PILOT_SYSTEM: dict[str, Any] = {
    "scheduler_adapter": (
        "first_gateway.platforms.schedulers.globus_compute_pbs.GlobusComputePBSAdapter"
    ),
    "scheduler_config": {},
    "job_walltime_min": 60,
    "queue": "debug",
    "account": "TestAcct",
    "max_num_nodes": 10,
    "gpus_per_node": 8,
    "scheduler_flags": "",
    "workdir": WORKDIR,
    "external_port": 8443,
    "nginx_path": "/usr/sbin/nginx",
    "ip_allowlist": ["10.0.0.0/8"],
    "node_file_env": "PBS_NODEFILE",
    "submit_script_preamble": "#!/bin/bash",
    "pilot_path": "/test/first-pilot",
}


def _make_client_state(
    db: async_sessionmaker[AsyncSession],
) -> MagicMock:
    settings = MagicMock()
    settings.pilot_ca_crt = "fake-ca-crt"
    settings.pilot_ca_key.get_secret_value.return_value = "fake-ca-key"
    cs = MagicMock()
    cs.db_sessionmaker = db
    cs.settings = settings
    cs.redis_pubsub.publish = AsyncMock()
    return cs


def _make_observer(
    db: async_sessionmaker[AsyncSession],
) -> PilotJobObserver:
    return PilotJobObserver("pilot-job-observer", _make_client_state(db), MagicMock())


async def _seed_cluster(sess: AsyncSession) -> None:
    sess.add(
        Cluster(
            name="polaris",
            health_check={"url": "http://x/health", "debounce": 2},
            pilot_system=PILOT_SYSTEM,
        )
    )
    await sess.flush()


async def _insert_pilot_job(
    sess: AsyncSession,
    name: str,
    scheduler_job_id: str,
    state: SchedulerJobState = SchedulerJobState.pending_submit,
    manager_url: str | None = None,
    scheduled_deletion_at: datetime | None = None,
) -> int:
    job = PilotJob(
        name=name,
        cluster_name="polaris",
        scheduler_job_id=scheduler_job_id,
        scheduler_state=state.value,
        manager_url=manager_url,
        scheduled_deletion_at=scheduled_deletion_at,
        manager_health=HealthCheckResult.unknown.value,
        walltime_min=60,
        num_nodes=1,
        gpus_per_node=4,
    )
    sess.add(job)
    await sess.flush()
    return job.uid


async def test_submitted_to_running_and_endpoint_discovery(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """
    End-to-end happy path: a PilotJob in submitted state transitions to running,
    then its manager_url is discovered via the readyfile.
    """
    adapter = FakeSchedulerAdapter()

    async with db.begin() as sess:
        await _seed_cluster(sess)
        uid = await _insert_pilot_job(sess, "job-alpha", "123.pbs")

    prefix = "__FIRST_PILOT_"
    adapter.statuses = [
        JobStatusInfo(
            id="123.pbs",
            name=f"{prefix}job-alpha",
            state=SchedulerJobState.running,
            created_at=NOW,
            started_at=NOW,
            walltime_minutes=60,
        ),
    ]

    observer = _make_observer(db)
    pilot_config = PilotConfig.model_validate(PILOT_SYSTEM)
    submitter = PilotSubmitter(pilot_config, adapter, "fake-ca-crt", "fake-ca-key")

    await observer._poll_cluster(submitter, "polaris")

    async with db() as sess:
        job = (await sess.scalars(select(PilotJob).where(PilotJob.uid == uid))).one()
    assert job.scheduler_state == SchedulerJobState.running.value
    assert job.time_started == NOW
    assert job.manager_url is None

    addr = AddressInfo(
        hostname="x3001",
        ip="10.1.2.3",
        external_port=8443,
        control_path="/control",
    )
    readyfile_dir = str(Path(WORKDIR) / "readyfiles")
    readyfile_path = str(Path(readyfile_dir) / "job-alpha.ready.json")
    adapter.directories[readyfile_dir] = ["job-alpha.ready.json"]
    adapter.files[readyfile_path] = (addr.model_dump_json(), 0o644)

    await observer._poll_cluster(submitter, "polaris")

    async with db() as sess:
        job = (await sess.scalars(select(PilotJob).where(PilotJob.uid == uid))).one()
    assert job.manager_url == "https://10.1.2.3:8443/control"

    # Discovering the endpoint wakes the Launch controller exactly once.
    publish: AsyncMock = observer.client_state.redis_pubsub.publish  # type: ignore[assignment]
    publish.assert_awaited_once_with(Channel.pilot_job_ready, "job-alpha")

    # A subsequent poll must NOT re-publish: manager_url is already set, so the
    # premised UPDATE affects zero rows.
    publish.reset_mock()
    await observer._poll_cluster(submitter, "polaris")
    publish.assert_not_awaited()


async def test_orphan_scheduler_job_reaped(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Scheduler jobs with the FIRST prefix but no DB row are terminated."""
    adapter = FakeSchedulerAdapter()

    async with db.begin() as sess:
        await _seed_cluster(sess)

    prefix = "__FIRST_PILOT_"
    adapter.statuses = [
        JobStatusInfo(
            id="999.pbs",
            name=f"{prefix}orphan-job",
            state=SchedulerJobState.running,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=4),
            started_at=NOW,
            walltime_minutes=60,
        ),
    ]

    observer = _make_observer(db)

    with patch(_PATCH_BUILD, new_callable=AsyncMock, return_value=adapter):
        await observer._poll_all_clusters()

    assert "999.pbs" in adapter.terminated


async def test_no_update_when_state_unchanged(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """A job already in `running` state is not re-written."""
    adapter = FakeSchedulerAdapter()

    async with db.begin() as sess:
        await _seed_cluster(sess)
        uid = await _insert_pilot_job(
            sess, "job-steady", "456.pbs", state=SchedulerJobState.running
        )

    adapter.statuses = [
        JobStatusInfo(
            id="456.pbs",
            name="__FIRST_PILOT_job-steady",
            state=SchedulerJobState.running,
            created_at=NOW,
            started_at=NOW,
            walltime_minutes=60,
        ),
    ]

    observer = _make_observer(db)

    with patch(_PATCH_BUILD, new_callable=AsyncMock, return_value=adapter):
        await observer._poll_all_clusters()

    async with db() as sess:
        job = (await sess.scalars(select(PilotJob).where(PilotJob.uid == uid))).one()
    assert job.scheduler_state == SchedulerJobState.running.value


async def test_missing_exiting_job_completes_nonblocking_termination(
    db: async_sessionmaker[AsyncSession],
) -> None:
    adapter = FakeSchedulerAdapter()

    async with db.begin() as sess:
        await _seed_cluster(sess)
        uid = await _insert_pilot_job(
            sess,
            "job-exiting",
            "457.pbs",
            state=SchedulerJobState.exiting,
            scheduled_deletion_at=NOW,
        )

    observer = _make_observer(db)
    pilot_config = PilotConfig.model_validate(PILOT_SYSTEM)
    submitter = PilotSubmitter(pilot_config, adapter, "fake-ca-crt", "fake-ca-key")
    await observer._poll_cluster(submitter, "polaris")

    async with db() as sess:
        job = (await sess.scalars(select(PilotJob).where(PilotJob.uid == uid))).one()
    assert job.scheduler_state == SchedulerJobState.gone.value
    assert job.deleted_at is not None


async def test_cluster_without_pilot_system_skipped(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Clusters without a pilot_system configuration are silently skipped."""
    async with db.begin() as sess:
        sess.add(
            Cluster(
                name="no-pilot",
                health_check={"url": "http://x/health", "debounce": 2},
                pilot_system=None,
            )
        )

    observer = _make_observer(db)
    await observer._poll_all_clusters()
