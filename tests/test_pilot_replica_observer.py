"""Tests for PilotReplicaObserver: replica state sync, orphan reaping,
and consecutive_launch_failures tracking."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import httpx
import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from first_common.schema.base_scheduler import SchedulerJobState
from first_common.schema.pilot import (
    GpuInfo,
    HostGpus,
    PilotJobStatus,
    PilotResources,
    ReplicaInfo,
)
from first_common.schema.types import (
    HealthCheckResult,
    PilotDeploymentState,
    ReplicaState,
)
from first_gateway.controllers.worker import Worker
from first_gateway.controllers.workers.pilot_replica_observer import (
    PilotReplicaObserver,
)
from first_gateway.database.models import (
    AccessGroup,
    Cluster,
    Model,
    PilotDeployment,
    PilotJob,
    PilotReplica,
)

NOW = datetime(2026, 7, 12, 12, 0, 0, tzinfo=timezone.utc)

RESOURCES = PilotResources(
    hosts=[
        HostGpus(
            hostname="x3001",
            gpus=[
                GpuInfo(
                    index="0", name="A100", memory_total_mib=40960, memory_used_mib=0
                )
            ],
        )
    ]
)


def _status_json(
    replicas: list[ReplicaInfo] | None = None,
    resources: PilotResources | None = None,
) -> dict[str, object]:
    status = PilotJobStatus(
        resources=resources or RESOURCES,
        replicas=replicas or [],
    )
    return status.model_dump(mode="json")


def _replica_info(
    name: str = "rep-1",
    state: ReplicaState = ReplicaState.ready,
    url: str = "http://10.0.0.1:8000",
    served_model_name: str = "llama-3",
    state_message: str = "Running",
    started_at: datetime = NOW,
) -> ReplicaInfo:
    return ReplicaInfo(
        name=name,
        state=state,
        url=url,
        served_model_name=served_model_name,
        state_message=state_message,
        started_at=started_at,
    )


def _make_transport(responses: dict[str, httpx.Response]) -> httpx.MockTransport:
    """Return a transport that maps URL paths to canned responses.

    ``responses`` maps a (method, path) string like ``"GET /status"`` to an
    ``httpx.Response``.  Any unmatched request returns 404.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        key = f"{request.method} {request.url.path}"
        return responses.get(key, httpx.Response(404))

    return httpx.MockTransport(handler)


def _make_observer(
    db: async_sessionmaker[AsyncSession],
    transport: httpx.MockTransport,
) -> PilotReplicaObserver:
    cs = MagicMock()
    cs.db_sessionmaker = db
    cs.redis_repo = AsyncMock()
    observer = PilotReplicaObserver.__new__(PilotReplicaObserver)
    Worker.__init__(observer, "pilot-replica-observer", cs)
    observer._http = httpx.AsyncClient(transport=transport)
    return observer


# ── Seed helpers ─────────────────────────────────────────────────────


async def _seed_base(sess: AsyncSession) -> None:
    sess.add(AccessGroup(name="ag", allowed_groups=[], allowed_domains=[]))
    sess.add(Cluster(name="cl", health_check={"url": "http://x/health", "debounce": 2}))
    await sess.flush()
    sess.add(Model(name="mdl", access_group_name="ag", supported_endpoints=["chat"]))
    await sess.flush()
    sess.add(
        PilotDeployment(
            name="pd",
            cluster_name="cl",
            model_name="mdl",
            router_params={},
            prometheus_scrape_interval_sec=30,
            min_replicas=0,
            max_replicas=4,
            launch_spec={},
        )
    )
    await sess.flush()


async def _add_job(
    sess: AsyncSession,
    name: str = "job-1",
    manager_url: str | None = "https://10.0.0.1:8443",
    state: str = SchedulerJobState.running.value,
    manager_health: str = HealthCheckResult.unknown.value,
) -> PilotJob:
    job = PilotJob(
        name=name,
        cluster_name="cl",
        scheduler_state=state,
        manager_url=manager_url,
        manager_health=manager_health,
        walltime_min=60,
        num_nodes=1,
        gpus_per_node=4,
    )
    sess.add(job)
    await sess.flush()
    return job


async def _add_replica(
    sess: AsyncSession,
    name: str = "rep-1",
    deployment: str = "pd",
    job: str = "job-1",
    state: str = ReplicaState.placed.value,
) -> PilotReplica:
    replica = PilotReplica(
        name=name,
        pilot_deployment_name=deployment,
        pilot_job_name=job,
        state=state,
    )
    sess.add(replica)
    await sess.flush()
    return replica


# ── Tests ────────────────────────────────────────────────────────────


async def test_replica_transitions_to_ready(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """A placed replica transitions to ready and its info fields are populated."""
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess)
        await _add_replica(sess)

    ri = _replica_info()
    transport = _make_transport(
        {"GET /status": httpx.Response(200, json=_status_json([ri]))}
    )
    observer = _make_observer(db, transport)
    await observer.poll()

    async with db() as sess:
        rep = (
            await sess.scalars(select(PilotReplica).where(PilotReplica.name == "rep-1"))
        ).one()
    assert rep.state == ReplicaState.ready.value
    assert rep.model_url == "http://10.0.0.1:8000"
    assert rep.observed_served_name == "llama-3"
    assert rep.state_message == "Running"
    assert rep.started_at == NOW


async def test_replica_transitions_to_error(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """A placed replica that fails transitions to error state."""
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess)
        await _add_replica(sess)

    ri = _replica_info(state=ReplicaState.error, state_message="OOM killed")
    transport = _make_transport(
        {"GET /status": httpx.Response(200, json=_status_json([ri]))}
    )
    observer = _make_observer(db, transport)
    await observer.poll()

    async with db() as sess:
        rep = (
            await sess.scalars(select(PilotReplica).where(PilotReplica.name == "rep-1"))
        ).one()
    assert rep.state == ReplicaState.error.value
    assert rep.state_message == "OOM killed"


async def test_job_resources_populated(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """PilotJob.resources is populated from the /status response."""
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess)

    transport = _make_transport(
        {"GET /status": httpx.Response(200, json=_status_json())}
    )
    observer = _make_observer(db, transport)
    await observer.poll()

    async with db() as sess:
        job = (
            await sess.scalars(select(PilotJob).where(PilotJob.name == "job-1"))
        ).one()
    assert job.resources == RESOURCES.model_dump(mode="json")


async def test_manager_health_set_healthy_on_success(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """A successful /status call sets manager_health to healthy and clears
    manager_unhealthy_since."""
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess, manager_health=HealthCheckResult.unhealthy.value)

    transport = _make_transport(
        {"GET /status": httpx.Response(200, json=_status_json())}
    )
    observer = _make_observer(db, transport)
    await observer.poll()

    async with db() as sess:
        job = (
            await sess.scalars(select(PilotJob).where(PilotJob.name == "job-1"))
        ).one()
    assert job.manager_health == HealthCheckResult.healthy.value
    assert job.manager_unhealthy_since is None


async def test_manager_health_set_unhealthy_on_failure(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """A failed /status call marks manager_health unhealthy with a timestamp."""
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess)

    transport = _make_transport({"GET /status": httpx.Response(500)})
    observer = _make_observer(db, transport)
    await observer.poll()

    async with db() as sess:
        job = (
            await sess.scalars(select(PilotJob).where(PilotJob.name == "job-1"))
        ).one()
    assert job.manager_health == HealthCheckResult.unhealthy.value
    assert job.manager_unhealthy_since is not None


async def test_idle_since_set_when_no_replicas(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """idle_since is set when zero replicas are running on a job."""
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess)

    transport = _make_transport(
        {"GET /status": httpx.Response(200, json=_status_json(replicas=[]))}
    )
    observer = _make_observer(db, transport)
    await observer.poll()

    async with db() as sess:
        job = (
            await sess.scalars(select(PilotJob).where(PilotJob.name == "job-1"))
        ).one()
    assert job.idle_since is not None


async def test_idle_since_cleared_when_replica_running(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """idle_since is cleared when at least one replica is active."""
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess)
        await _add_replica(sess)
        await sess.execute(
            sa.update(PilotJob).where(PilotJob.name == "job-1").values(idle_since=NOW)
        )

    ri = _replica_info()
    transport = _make_transport(
        {"GET /status": httpx.Response(200, json=_status_json([ri]))}
    )
    observer = _make_observer(db, transport)
    await observer.poll()

    async with db() as sess:
        job = (
            await sess.scalars(select(PilotJob).where(PilotJob.name == "job-1"))
        ).one()
    assert job.idle_since is None


async def test_orphan_replica_reaped(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """A replica reported by the pilot manager with no matching DB row is stopped."""
    stop_called = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal stop_called
        key = f"{request.method} {request.url.path}"
        if key == "GET /status":
            orphan = _replica_info(name="orphan-rep")
            return httpx.Response(200, json=_status_json([orphan]))
        if key == "POST /stop-replica/orphan-rep":
            stop_called = True
            return httpx.Response(200)
        return httpx.Response(404)

    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess)

    transport = httpx.MockTransport(handler)
    observer = _make_observer(db, transport)
    await observer.poll()

    assert stop_called


async def test_orphan_replica_wrong_job_fk_reaped(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """A replica that exists in DB but points at a different PilotJob is reaped."""
    stop_called = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal stop_called
        key = f"{request.method} {request.url.path}"
        if key == "GET /status":
            ri = _replica_info(name="rep-mismatch")
            return httpx.Response(200, json=_status_json([ri]))
        if key == "POST /stop-replica/rep-mismatch":
            stop_called = True
            return httpx.Response(200)
        return httpx.Response(404)

    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess, name="job-1")
        await _add_job(sess, name="job-2", manager_url="https://10.0.0.2:8443")
        await _add_replica(sess, name="rep-mismatch", job="job-2")

    transport = httpx.MockTransport(handler)
    observer = _make_observer(db, transport)
    await observer.poll()

    assert stop_called


async def test_consecutive_launch_failures_incremented(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Replicas in error state increment consecutive_launch_failures on
    their parent PilotDeployment."""
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess)
        await _add_replica(sess, name="rep-fail-1")
        await _add_replica(sess, name="rep-fail-2")

    r1 = _replica_info(
        name="rep-fail-1", state=ReplicaState.error, state_message="crash"
    )
    r2 = _replica_info(
        name="rep-fail-2", state=ReplicaState.start_timeout, state_message="timeout"
    )
    transport = _make_transport(
        {"GET /status": httpx.Response(200, json=_status_json([r1, r2]))}
    )
    observer = _make_observer(db, transport)
    await observer.poll()

    async with db() as sess:
        dep = (
            await sess.scalars(
                select(PilotDeployment).where(PilotDeployment.name == "pd")
            )
        ).one()
    assert dep.consecutive_launch_failures == 2


async def test_consecutive_launch_failures_reset_on_success(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """A ready replica resets consecutive_launch_failures to 0."""
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess)
        await _add_replica(sess, name="rep-ok")
        await sess.execute(
            sa.update(PilotDeployment)
            .where(PilotDeployment.name == "pd")
            .values(consecutive_launch_failures=5)
        )

    ri = _replica_info(name="rep-ok", state=ReplicaState.ready)
    transport = _make_transport(
        {"GET /status": httpx.Response(200, json=_status_json([ri]))}
    )
    observer = _make_observer(db, transport)
    await observer.poll()

    async with db() as sess:
        dep = (
            await sess.scalars(
                select(PilotDeployment).where(PilotDeployment.name == "pd")
            )
        ).one()
    assert dep.consecutive_launch_failures == 0


async def test_consecutive_launch_failures_accumulates(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Failures accumulate across multiple polls."""
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess)
        await _add_replica(sess, name="rep-f")
        await sess.execute(
            sa.update(PilotDeployment)
            .where(PilotDeployment.name == "pd")
            .values(consecutive_launch_failures=3)
        )

    ri = _replica_info(name="rep-f", state=ReplicaState.error, state_message="crash")
    transport = _make_transport(
        {"GET /status": httpx.Response(200, json=_status_json([ri]))}
    )
    observer = _make_observer(db, transport)
    await observer.poll()

    async with db() as sess:
        dep = (
            await sess.scalars(
                select(PilotDeployment).where(PilotDeployment.name == "pd")
            )
        ).one()
    assert dep.consecutive_launch_failures == 4


async def test_no_update_when_state_unchanged(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Polling with identical state is idempotent (no writes)."""
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess, manager_health=HealthCheckResult.healthy.value)
        await _add_replica(sess, name="rep-stable", state=ReplicaState.ready.value)
        await sess.execute(
            sa.update(PilotReplica)
            .where(PilotReplica.name == "rep-stable")
            .values(
                model_url="http://10.0.0.1:8000",
                observed_served_name="llama-3",
                state_message="Running",
                started_at=NOW,
            )
        )

    ri = _replica_info(name="rep-stable")
    transport = _make_transport(
        {"GET /status": httpx.Response(200, json=_status_json([ri]))}
    )
    observer = _make_observer(db, transport)
    await observer.poll()

    async with db() as sess:
        rep = (
            await sess.scalars(
                select(PilotReplica).where(PilotReplica.name == "rep-stable")
            )
        ).one()
    assert rep.state == ReplicaState.ready.value
    assert rep.model_url == "http://10.0.0.1:8000"


async def test_non_running_jobs_skipped(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Jobs not in running state or without manager_url are not polled."""
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(
            sess,
            name="pending-job",
            state=SchedulerJobState.pending_submit.value,
            manager_url=None,
        )

    transport = _make_transport({})
    observer = _make_observer(db, transport)
    await observer.poll()


async def test_http_failure_per_job_does_not_block_others(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """If one job's /status fails, other jobs are still polled."""
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess, name="job-bad", manager_url="https://10.0.0.1:8443")
        await _add_job(sess, name="job-good", manager_url="https://10.0.0.2:8443")
        await _add_replica(sess, name="rep-good", job="job-good")

    ri = _replica_info(name="rep-good")
    good_status = _status_json([ri])

    def handler(request: httpx.Request) -> httpx.Response:
        host = request.url.host
        if host == "10.0.0.1":
            return httpx.Response(500)
        if host == "10.0.0.2" and request.url.path == "/status":
            return httpx.Response(200, json=good_status)
        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    observer = _make_observer(db, transport)
    await observer.poll()

    async with db() as sess:
        rep = (
            await sess.scalars(
                select(PilotReplica).where(PilotReplica.name == "rep-good")
            )
        ).one()
    assert rep.state == ReplicaState.ready.value

    async with db() as sess:
        bad_job = (
            await sess.scalars(select(PilotJob).where(PilotJob.name == "job-bad"))
        ).one()
    assert bad_job.manager_health == HealthCheckResult.unhealthy.value


# ── Deployment aggregate state tests ────────────────────────────────


async def test_deployment_state_healthy(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Deployment is healthy when serving replicas >= desired."""
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess)
        await _add_replica(sess, name="rep-1")
        await sess.execute(
            sa.update(PilotDeployment)
            .where(PilotDeployment.name == "pd")
            .values(desired_replicas=1)
        )

    ri = _replica_info(name="rep-1", state=ReplicaState.ready)
    transport = _make_transport(
        {"GET /status": httpx.Response(200, json=_status_json([ri]))}
    )
    observer = _make_observer(db, transport)
    await observer.poll()

    async with db() as sess:
        dep = (
            await sess.scalars(
                select(PilotDeployment).where(PilotDeployment.name == "pd")
            )
        ).one()
    assert dep.state == PilotDeploymentState.healthy.value


async def test_deployment_state_degraded(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Deployment is degraded when some replicas serve but fewer than desired."""
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess)
        await _add_replica(sess, name="rep-1")
        await _add_replica(sess, name="rep-2")
        await sess.execute(
            sa.update(PilotDeployment)
            .where(PilotDeployment.name == "pd")
            .values(desired_replicas=2)
        )

    r1 = _replica_info(name="rep-1", state=ReplicaState.ready)
    r2 = _replica_info(name="rep-2", state=ReplicaState.launching)
    transport = _make_transport(
        {"GET /status": httpx.Response(200, json=_status_json([r1, r2]))}
    )
    observer = _make_observer(db, transport)
    await observer.poll()

    async with db() as sess:
        dep = (
            await sess.scalars(
                select(PilotDeployment).where(PilotDeployment.name == "pd")
            )
        ).one()
    assert dep.state == PilotDeploymentState.degraded.value


async def test_deployment_state_starting(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Deployment is starting when no replicas serve but some are in-flight."""
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess)
        await _add_replica(sess, name="rep-1")
        await sess.execute(
            sa.update(PilotDeployment)
            .where(PilotDeployment.name == "pd")
            .values(desired_replicas=1)
        )

    ri = _replica_info(name="rep-1", state=ReplicaState.launching)
    transport = _make_transport(
        {"GET /status": httpx.Response(200, json=_status_json([ri]))}
    )
    observer = _make_observer(db, transport)
    await observer.poll()

    async with db() as sess:
        dep = (
            await sess.scalars(
                select(PilotDeployment).where(PilotDeployment.name == "pd")
            )
        ).one()
    assert dep.state == PilotDeploymentState.starting.value


async def test_deployment_state_stopping(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Deployment is stopping when replicas are draining (terminating)."""
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess)
        await _add_replica(sess, name="rep-1", state=ReplicaState.ready.value)
        await sess.execute(
            sa.update(PilotDeployment)
            .where(PilotDeployment.name == "pd")
            .values(desired_replicas=0)
        )

    ri = _replica_info(
        name="rep-1", state=ReplicaState.terminating, state_message="Shutting down"
    )
    transport = _make_transport(
        {"GET /status": httpx.Response(200, json=_status_json([ri]))}
    )
    observer = _make_observer(db, transport)
    await observer.poll()

    async with db() as sess:
        dep = (
            await sess.scalars(
                select(PilotDeployment).where(PilotDeployment.name == "pd")
            )
        ).one()
    assert dep.state == PilotDeploymentState.stopping.value


async def test_deployment_state_failed_from_launch_failures(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Deployment is failed when desired > 0 and consecutive launch failures accumulate."""
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess)
        await _add_replica(sess, name="rep-1")
        await sess.execute(
            sa.update(PilotDeployment)
            .where(PilotDeployment.name == "pd")
            .values(desired_replicas=1)
        )

    ri = _replica_info(
        name="rep-1", state=ReplicaState.error, state_message="OOM killed"
    )
    transport = _make_transport(
        {"GET /status": httpx.Response(200, json=_status_json([ri]))}
    )
    observer = _make_observer(db, transport)
    await observer.poll()

    async with db() as sess:
        dep = (
            await sess.scalars(
                select(PilotDeployment).where(PilotDeployment.name == "pd")
            )
        ).one()
    assert dep.state == PilotDeploymentState.failed.value
    assert dep.consecutive_launch_failures == 1


async def test_deployment_state_failed_from_unhealthy_replicas(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Deployment is failed when desired > 0 and all replicas are unhealthy."""
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess)
        await _add_replica(sess, name="rep-1", state=ReplicaState.ready.value)
        await sess.execute(
            sa.update(PilotDeployment)
            .where(PilotDeployment.name == "pd")
            .values(desired_replicas=1)
        )

    ri = _replica_info(
        name="rep-1",
        state=ReplicaState.unhealthy,
        state_message="Health check failing",
    )
    transport = _make_transport(
        {"GET /status": httpx.Response(200, json=_status_json([ri]))}
    )
    observer = _make_observer(db, transport)
    await observer.poll()

    async with db() as sess:
        dep = (
            await sess.scalars(
                select(PilotDeployment).where(PilotDeployment.name == "pd")
            )
        ).one()
    assert dep.state == PilotDeploymentState.failed.value


async def test_deployment_state_awaiting_capacity(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Deployment is awaiting_capacity when desired > 0 but no replicas exist."""
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess)
        await sess.execute(
            sa.update(PilotDeployment)
            .where(PilotDeployment.name == "pd")
            .values(desired_replicas=1)
        )

    transport = _make_transport(
        {"GET /status": httpx.Response(200, json=_status_json())}
    )
    observer = _make_observer(db, transport)
    await observer.poll()

    async with db() as sess:
        dep = (
            await sess.scalars(
                select(PilotDeployment).where(PilotDeployment.name == "pd")
            )
        ).one()
    assert dep.state == PilotDeploymentState.awaiting_capacity.value


async def test_deployment_state_offline(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Deployment is offline when desired is 0 and no live replicas exist."""
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess)

    transport = _make_transport(
        {"GET /status": httpx.Response(200, json=_status_json())}
    )
    observer = _make_observer(db, transport)
    await observer.poll()

    async with db() as sess:
        dep = (
            await sess.scalars(
                select(PilotDeployment).where(PilotDeployment.name == "pd")
            )
        ).one()
    assert dep.state == PilotDeploymentState.offline.value
