"""Tests for PilotJobObserver: scheduler state sync and endpoint discovery."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Self, cast
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
from first_common.schema.pilot import AddressInfo, PilotJobStatus, PilotResources
from first_common.schema.types import HealthCheckResult, PilotConfig
from first_gateway.controllers.worker import Worker
from first_gateway.controllers.workers.pilot_job_observer import PilotJobObserver
from first_gateway.database.models import (
    AccessGroup,
    Cluster,
    Model,
    PilotDeployment,
    PilotJob,
    PilotReplica,
)
from first_gateway.database.redis.pubsub import Channel
from first_gateway.platforms.schedulers.graphql_pbs import GraphQLPBSAdapter
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
    # Bypass the production mTLS client constructor; individual tests control
    # the candidate-endpoint /status result through this AsyncMock.
    observer = PilotJobObserver.__new__(PilotJobObserver)
    Worker.__init__(observer, "pilot-job-observer", _make_client_state(db), MagicMock())
    observer.client = MagicMock()
    observer.client.get_status = AsyncMock(
        return_value=PilotJobStatus(resources=PilotResources(), replicas=[])
    )
    return observer


async def _seed_cluster(sess: AsyncSession) -> None:
    sess.add(
        Cluster(
            name="polaris",
            health_check={"url": "http://x/health", "debounce": 2},
            pilot_system=PILOT_SYSTEM,
        )
    )
    await sess.flush()


async def _seed_deployment_parents(sess: AsyncSession) -> None:
    sess.add(AccessGroup(name="ag", allowed_groups=[], allowed_domains=[]))
    await sess.flush()
    sess.add(Model(name="model", access_group_name="ag", supported_endpoints=["chat"]))
    await sess.flush()


async def _insert_deployment(
    sess: AsyncSession,
    name: str,
    *,
    max_consecutive_launch_failures: int = 1,
) -> None:
    sess.add(
        PilotDeployment(
            name=name,
            cluster_name="polaris",
            model_name="model",
            router_params={},
            prometheus_scrape_interval_sec=30,
            min_replicas=1,
            max_replicas=1,
            launch_spec={},
            desired_replicas=1,
            max_consecutive_launch_failures=max_consecutive_launch_failures,
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


async def test_graphql_head_ip_requires_live_manager_status(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """A scheduler-assigned head IP is not pilot-manager readiness evidence."""
    async with db.begin() as sess:
        await _seed_cluster(sess)
        uid = await _insert_pilot_job(
            sess,
            "job-graphql",
            "graphql.pbs",
            state=SchedulerJobState.running,
        )

    scheduler_status = JobStatusInfo(
        id="graphql.pbs",
        name="__FIRST_PILOT_job-graphql",
        state=SchedulerJobState.running,
        created_at=NOW,
        started_at=NOW,
        walltime_minutes=60,
        head_node_ip_address="10.1.2.3",
        head_node_hostname="x3001",
    )
    adapter = GraphQLPBSAdapter(client=MagicMock(), owner="svc", url="https://gql")
    submitter = PilotSubmitter(
        PilotConfig.model_validate(PILOT_SYSTEM),
        adapter,
        "fake-ca-crt",
        "fake-ca-key",
    )
    observer = _make_observer(db)
    get_status = cast(AsyncMock, observer.client.get_status)
    publish = cast(AsyncMock, observer.client_state.redis_pubsub.publish)
    get_status.side_effect = RuntimeError("manager not bound")

    with patch.object(
        adapter,
        "get_job_statuses",
        new=AsyncMock(return_value=[scheduler_status]),
    ):
        await observer._discover_endpoints(submitter, "polaris")

        async with db() as sess:
            job = await sess.get(PilotJob, uid)
            assert job is not None
            assert job.manager_url is None
        publish.assert_not_awaited()

        # Once the exact candidate answers /status over mTLS, publish it and
        # wake launchers.  This is the first point at which manager_url is set.
        get_status.side_effect = None
        get_status.return_value = PilotJobStatus(
            resources=PilotResources(), replicas=[]
        )
        await observer._discover_endpoints(submitter, "polaris")

    candidate = "https://10.1.2.3:8443/control/"
    get_status.assert_awaited_with(candidate)
    async with db() as sess:
        job = await sess.get(PilotJob, uid)
        assert job is not None
        assert job.manager_url == candidate
    publish.assert_awaited_once_with(Channel.pilot_job_ready, "job-graphql")


async def test_graphql_known_job_omitted_from_bulk_page_uses_exact_truth(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_cluster(sess)
        uid = await _insert_pilot_job(
            sess,
            "job-omitted",
            "omitted.pbs",
            state=SchedulerJobState.queued,
        )

    exact = JobStatusInfo(
        id="omitted.pbs",
        name="__FIRST_PILOT_job-omitted",
        state=SchedulerJobState.running,
        created_at=NOW,
        started_at=NOW,
        walltime_minutes=60,
        head_node_ip_address="10.1.2.7",
        head_node_hostname="x3007",
    )
    adapter = GraphQLPBSAdapter(client=MagicMock(), owner="svc", url="https://gql")
    bulk = AsyncMock(return_value=[])
    exact_lookup = AsyncMock(return_value=exact)
    adapter.get_job_statuses = bulk  # type: ignore[method-assign]
    adapter.get_exact_job_status = exact_lookup  # type: ignore[method-assign]
    submitter = PilotSubmitter(
        PilotConfig.model_validate(PILOT_SYSTEM),
        adapter,
        "fake-ca-crt",
        "fake-ca-key",
    )
    observer = _make_observer(db)
    await observer._poll_cluster(submitter, "polaris")

    async with db() as sess:
        job = await sess.get(PilotJob, uid)
        assert job is not None
        assert job.scheduler_state == SchedulerJobState.running.value
        assert job.deleted_at is None
    exact_lookup.assert_awaited_once_with("omitted.pbs")


async def test_graphql_unready_terminal_allocation_is_charged_once(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """A PALS-era failure behind a synthesized head IP gets one replacement."""
    async with db.begin() as sess:
        await _seed_cluster(sess)
        await _seed_deployment_parents(sess)
        await _insert_deployment(sess, "dep-a")
        uid = await _insert_pilot_job(
            sess,
            "job-inventory-failed",
            "inventory-failed.pbs",
            state=SchedulerJobState.running,
        )
        sess.add(
            PilotReplica(
                name="dep-a/replica/inventory-failed",
                pilot_deployment_name="dep-a",
                pilot_job_name="job-inventory-failed",
                state="placed",
            )
        )

    scheduler_status = JobStatusInfo(
        id="inventory-failed.pbs",
        name="__FIRST_PILOT_job-inventory-failed",
        state=SchedulerJobState.running,
        created_at=NOW,
        started_at=NOW,
        walltime_minutes=60,
        head_node_ip_address="10.1.2.4",
        head_node_hostname="x3002",
    )
    adapter = GraphQLPBSAdapter(client=MagicMock(), owner="svc", url="https://gql")
    submitter = PilotSubmitter(
        PilotConfig.model_validate(PILOT_SYSTEM),
        adapter,
        "fake-ca-crt",
        "fake-ca-key",
    )
    observer = _make_observer(db)
    get_status = cast(AsyncMock, observer.client.get_status)
    get_status.side_effect = RuntimeError("PALS inventory failed before bind")
    with patch.object(
        adapter,
        "get_job_statuses",
        new=AsyncMock(return_value=[scheduler_status]),
    ):
        await observer._discover_endpoints(submitter, "polaris")

    async with db() as sess:
        job = await sess.get(PilotJob, uid)
        assert job is not None
        assert job.manager_url is None

    terminal = JobStatusInfo(
        id="inventory-failed.pbs",
        name="__FIRST_PILOT_job-inventory-failed",
        state=SchedulerJobState.exiting,
        created_at=NOW,
        started_at=NOW,
        walltime_minutes=60,
    )
    await observer._update_job(job, terminal)
    await observer._update_job(job, terminal)

    async with db() as sess:
        dep = await PilotDeployment.get_by_name(sess, "dep-a")
        assert dep.consecutive_launch_failures == 0

    await observer._update_job(job, None)
    await observer._update_job(job, None)

    async with db() as sess:
        dep = await PilotDeployment.get_by_name(sess, "dep-a")
        assert dep.consecutive_launch_failures == 1


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
        JobStatusInfo(
            id="998.pbs",
            name=f"{prefix}orphan-exiting",
            state=SchedulerJobState.exiting,
            created_at=datetime.now(timezone.utc) - timedelta(minutes=4),
            started_at=NOW,
            walltime_minutes=60,
        ),
    ]

    observer = _make_observer(db)

    with patch(_PATCH_BUILD, new_callable=AsyncMock, return_value=adapter):
        await observer._poll_all_clusters()

    assert "999.pbs" in adapter.terminated
    assert "998.pbs" in adapter.terminated


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


async def test_pre_manager_terminal_jobs_count_once_per_assigned_deployment(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """A failed allocation is charged once, even across exiting/gone polls.

    With a configured maximum of one launch failure, the first dead allocation
    permits one replacement and the replacement's failure advances the counter
    to two, which the autoscaler treats as over the bound.
    """
    async with db.begin() as sess:
        await _seed_cluster(sess)
        await _seed_deployment_parents(sess)
        await _insert_deployment(sess, "dep-a")
        await _insert_deployment(sess, "dep-b")
        first_uid = await _insert_pilot_job(
            sess,
            "job-first",
            "first.pbs",
            state=SchedulerJobState.running,
        )
        sess.add_all(
            [
                # Two replicas from dep-a must still count as one allocation
                # failure for that deployment.
                PilotReplica(
                    name="dep-a/replica/one",
                    pilot_deployment_name="dep-a",
                    pilot_job_name="job-first",
                    state="placed",
                ),
                PilotReplica(
                    name="dep-a/replica/two",
                    pilot_deployment_name="dep-a",
                    pilot_job_name="job-first",
                    state="placed",
                ),
                PilotReplica(
                    name="dep-b/replica/one",
                    pilot_deployment_name="dep-b",
                    pilot_job_name="job-first",
                    state="placed",
                ),
            ]
        )

    async with db() as sess:
        first_job = await sess.get(PilotJob, first_uid)
    assert first_job is not None

    observer = _make_observer(db)
    exiting = JobStatusInfo(
        id="first.pbs",
        name="__FIRST_PILOT_job-first",
        state=SchedulerJobState.exiting,
        created_at=NOW,
        started_at=NOW,
        walltime_minutes=60,
    )
    await observer._update_job(first_job, exiting)
    # Repeat with the intentionally stale object, then advance exiting -> gone.
    # The locked current row is the exactly-once guard, not caller freshness.
    await observer._update_job(first_job, exiting)
    await observer._update_job(first_job, None)

    async with db() as sess:
        dep_a = await PilotDeployment.get_by_name(sess, "dep-a")
        dep_b = await PilotDeployment.get_by_name(sess, "dep-b")
        first_job_current = await sess.get(PilotJob, first_uid)
        assert dep_a.consecutive_launch_failures == 1
        assert dep_b.consecutive_launch_failures == 1
        assert first_job_current is not None
        assert first_job_current.scheduler_state == SchedulerJobState.gone.value

    # A single fresh replacement allocation for dep-a fails the same way.  It
    # is charged once and moves 1 -> 2; repeat observation remains idempotent.
    async with db.begin() as sess:
        second_uid = await _insert_pilot_job(
            sess,
            "job-replacement",
            "replacement.pbs",
            state=SchedulerJobState.running,
        )
        sess.add(
            PilotReplica(
                name="dep-a/replica/replacement",
                pilot_deployment_name="dep-a",
                pilot_job_name="job-replacement",
                state="placed",
            )
        )

    async with db() as sess:
        replacement = await sess.get(PilotJob, second_uid)
    assert replacement is not None
    await observer._update_job(replacement, None)
    await observer._update_job(replacement, None)

    async with db() as sess:
        dep_a = await PilotDeployment.get_by_name(sess, "dep-a")
        dep_b = await PilotDeployment.get_by_name(sess, "dep-b")
        assert dep_a.consecutive_launch_failures == 2
        assert dep_a.max_consecutive_launch_failures == 1
        assert dep_b.consecutive_launch_failures == 1


async def test_intentional_or_ready_terminal_job_is_not_charged(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Only an unready, not-already-draining allocation is a launch failure."""
    async with db.begin() as sess:
        await _seed_cluster(sess)
        await _seed_deployment_parents(sess)
        await _insert_deployment(sess, "dep-a")
        ready_uid = await _insert_pilot_job(
            sess,
            "job-was-ready",
            "ready.pbs",
            state=SchedulerJobState.running,
            manager_url="https://10.0.0.1/control",
        )
        draining_uid = await _insert_pilot_job(
            sess,
            "job-draining",
            "draining.pbs",
            state=SchedulerJobState.running,
        )
        draining = await sess.get(PilotJob, draining_uid)
        assert draining is not None
        draining.scheduled_deletion_at = NOW
        sess.add_all(
            [
                PilotReplica(
                    name="dep-a/replica/ready",
                    pilot_deployment_name="dep-a",
                    pilot_job_name="job-was-ready",
                    state="ready",
                ),
                PilotReplica(
                    name="dep-a/replica/draining",
                    pilot_deployment_name="dep-a",
                    pilot_job_name="job-draining",
                    state="placed",
                ),
            ]
        )

    observer = _make_observer(db)
    async with db() as sess:
        ready = await sess.get(PilotJob, ready_uid)
        draining = await sess.get(PilotJob, draining_uid)
    assert ready is not None and draining is not None
    await observer._update_job(ready, None)
    await observer._update_job(draining, None)

    async with db() as sess:
        dep = await PilotDeployment.get_by_name(sess, "dep-a")
        assert dep.consecutive_launch_failures == 0


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
