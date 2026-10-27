"""Tests for PilotReplicaObserver: replica state sync, orphan reaping,
and consecutive_launch_failures tracking."""

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import monotonic
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
    GpuClaim,
    HealthCheckResult,
    PilotDeploymentState,
    ReplicaState,
)
from first_gateway.controllers.worker import Heartbeat, Worker
from first_gateway.controllers.workers.pilot_replica_observer import (
    MAINTENANCE_UNREACHABLE,
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
from first_gateway.services.pilot_control import PilotControlClient

from .fixtures.db import LAUNCH_TEMPLATE_NAME, launch_template

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
        resources=[GpuClaim(hostname="foo", gpu_ids=["0", "1"])],
        log_path=Path("/path/to/replica.log"),
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
    cs.redis_pubsub.publish = AsyncMock()
    observer = PilotReplicaObserver.__new__(PilotReplicaObserver)
    Worker.__init__(observer, "pilot-replica-observer", cs, MagicMock())
    client = PilotControlClient.__new__(PilotControlClient)
    client._client = httpx.AsyncClient(transport=transport)
    observer.client = client
    observer._status_cooldown = {}
    observer.hb = observer.register_heartbeat("poll")
    return observer


# ── Seed helpers ─────────────────────────────────────────────────────


async def _seed_base(sess: AsyncSession) -> None:
    sess.add(AccessGroup(name="ag", allowed_groups=[], allowed_domains=[]))
    sess.add(Cluster(name="cl", health_check={"url": "http://x/health", "debounce": 2}))
    await sess.flush()
    sess.add(Model(name="mdl", access_group_name="ag", supported_endpoints=["chat"]))
    sess.add(launch_template())
    await sess.flush()
    sess.add(
        PilotDeployment(
            name="pd",
            cluster_name="cl",
            model_name="mdl",
            launch_template_name=LAUNCH_TEMPLATE_NAME,
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
    placed_at: datetime | None = None,
) -> PilotReplica:
    replica = PilotReplica(
        name=name,
        pilot_deployment_name=deployment,
        pilot_job_name=job,
        state=state,
        placed_at=placed_at,
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


# ── /status failure backoff tests ───────────────────────────────────


async def test_failed_status_puts_job_on_cooldown(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """After a /status failure, the same job is skipped on the next poll."""
    async with db.begin() as sess:
        await _seed_base(sess)
        job = await _add_job(sess)

    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(500)

    observer = _make_observer(db, httpx.MockTransport(handler))
    await observer.poll()
    assert call_count == 1
    assert observer._status_cooldown[job.uid][1] == 1

    # Second poll happens immediately: job is still within its 10s cooldown, so
    # no new /status request is made and the failure count is unchanged.
    await observer.poll()
    assert call_count == 1
    assert observer._status_cooldown[job.uid][1] == 1


async def test_cooldown_grows_on_repeated_failures(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Each elapsed-cooldown failure increments the count and lengthens the delay."""
    async with db.begin() as sess:
        await _seed_base(sess)
        job = await _add_job(sess)

    observer = _make_observer(db, _make_transport({"GET /status": httpx.Response(500)}))

    await observer.poll()
    first_deadline, first_failures = observer._status_cooldown[job.uid]
    assert first_failures == 1

    # Force the cooldown to appear elapsed so the next poll retries and fails again.
    observer._status_cooldown[job.uid] = (0.0, first_failures)
    await observer.poll()
    second_deadline, second_failures = observer._status_cooldown[job.uid]
    assert second_failures == 2


async def test_cooldown_cleared_on_recovery(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """A successful /status clears the cooldown so polling resumes every tick."""
    async with db.begin() as sess:
        await _seed_base(sess)
        job = await _add_job(sess)
        await _add_replica(sess)

    healthy = False

    def handler(request: httpx.Request) -> httpx.Response:
        if not healthy:
            return httpx.Response(500)
        return httpx.Response(200, json=_status_json([_replica_info()]))

    observer = _make_observer(db, httpx.MockTransport(handler))

    await observer.poll()
    assert job.uid in observer._status_cooldown

    # Manager recovers; pretend the cooldown has elapsed so the job is retried.
    healthy = True
    observer._status_cooldown[job.uid] = (0.0, observer._status_cooldown[job.uid][1])
    await observer.poll()

    assert job.uid not in observer._status_cooldown
    async with db() as sess:
        rep = (
            await sess.scalars(select(PilotReplica).where(PilotReplica.name == "rep-1"))
        ).one()
    assert rep.state == ReplicaState.ready.value


async def test_failure_still_records_unhealthy(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Backoff does not suppress the manager_health=unhealthy escalation signal."""
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess)

    observer = _make_observer(db, _make_transport({"GET /status": httpx.Response(500)}))
    await observer.poll()

    async with db() as sess:
        job = (
            await sess.scalars(select(PilotJob).where(PilotJob.name == "job-1"))
        ).one()
    assert job.manager_health == HealthCheckResult.unhealthy.value
    assert job.manager_unhealthy_since is not None


async def test_cooldown_forgotten_when_job_leaves_running(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """A job that stops running is dropped from the in-memory cooldown map."""
    async with db.begin() as sess:
        await _seed_base(sess)
        job = await _add_job(sess)

    observer = _make_observer(db, _make_transport({"GET /status": httpx.Response(500)}))
    await observer.poll()
    assert job.uid in observer._status_cooldown

    # Job leaves the running set; the next poll finds no running jobs and prunes it.
    async with db.begin() as sess:
        await sess.execute(
            sa.update(PilotJob)
            .where(PilotJob.uid == job.uid)
            .values(scheduler_state=SchedulerJobState.exiting.value)
        )
    await observer.poll()
    assert job.uid not in observer._status_cooldown


async def test_hung_managers_are_all_probed_in_one_pass(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Probes run concurrently and beat the heartbeat, so a pass over many
    hung managers reaches every job without going stale."""
    n_jobs = 24
    hang = 0.2
    async with db.begin() as sess:
        await _seed_base(sess)
        for i in range(n_jobs):
            await _add_job(
                sess, name=f"job-{i}", manager_url=f"https://10.0.1.{i}:8443"
            )

    async def handler(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(hang)
        raise httpx.ConnectTimeout("hung", request=request)

    observer = _make_observer(db, httpx.MockTransport(handler))
    # One probe at a time would take n_jobs * hang = 4.8s.
    observer.hb = Heartbeat("poll", timeout=1.0)
    stale = False

    async def watch() -> None:
        nonlocal stale
        while True:
            stale = stale or observer.hb.timed_out()
            await asyncio.sleep(0.02)

    watcher = asyncio.create_task(watch())
    t0 = monotonic()
    try:
        await observer.poll()
    finally:
        watcher.cancel()
    assert monotonic() - t0 < 2.0
    assert not stale

    async with db() as sess:
        healths = set(await sess.scalars(select(PilotJob.manager_health)))
    assert healths == {HealthCheckResult.unhealthy.value}
    assert len(observer._status_cooldown) == n_jobs


async def test_sync_error_on_one_job_does_not_abort_pass(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess, name="job-bad", manager_url="https://10.0.0.1:8443")
        await _add_job(sess, name="job-good", manager_url="https://10.0.0.2:8443")
        await _add_replica(sess, name="rep-good", job="job-good")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "10.0.0.1":
            return httpx.Response(200, json=_status_json())
        return httpx.Response(200, json=_status_json([_replica_info("rep-good")]))

    observer = _make_observer(db, httpx.MockTransport(handler))
    original = observer.update_job_status

    async def flaky(job: PilotJob, status: PilotJobStatus) -> None:
        if job.name == "job-bad":
            raise RuntimeError("injected")
        await original(job, status)

    observer.update_job_status = flaky  # type: ignore[method-assign]
    await observer.poll()

    async with db() as sess:
        rep = await sess.scalar(
            select(PilotReplica).where(PilotReplica.name == "rep-good")
        )
    assert rep is not None and rep.state == ReplicaState.ready.value


# ── Maintenance: dead pilot's replicas leave routing ────────────────


async def _seed_maintenance_replicas(sess: AsyncSession) -> None:
    await _add_job(sess)
    for state in ("ready", "launching", "placed"):
        await _add_replica(sess, name=f"rep-{state}", state=state)
    draining = await _add_replica(sess, name="rep-draining", state="ready")
    draining.scheduled_deletion_at = NOW
    deleted = await _add_replica(sess, name="rep-deleted", state="ready")
    deleted.deleted_at = NOW


async def _replica_states(db: async_sessionmaker[AsyncSession]) -> dict[str, str]:
    async with db() as sess:
        rows = await sess.execute(select(PilotReplica.name, PilotReplica.state))
    return dict(rows.tuples().all())


async def test_maintenance_failure_marks_serving_replicas_unhealthy(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_base(sess)
        await sess.execute(sa.update(Cluster).values(maintenance_notice="PM until 5pm"))
        await _seed_maintenance_replicas(sess)

    observer = _make_observer(db, _make_transport({"GET /status": httpx.Response(500)}))
    await observer.poll()

    assert await _replica_states(db) == {
        "rep-ready": "unhealthy",
        "rep-launching": "unhealthy",
        "rep-placed": "placed",
        "rep-draining": "ready",
        "rep-deleted": "ready",
    }
    async with db() as sess:
        rep = await sess.scalar(
            select(PilotReplica).where(PilotReplica.name == "rep-ready")
        )
    assert rep is not None and rep.state_message == MAINTENANCE_UNREACHABLE


async def test_maintenance_set_after_manager_unhealthy_still_marks_replicas(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_base(sess)
        await _seed_maintenance_replicas(sess)

    observer = _make_observer(db, _make_transport({"GET /status": httpx.Response(500)}))
    await observer.poll()
    assert (await _replica_states(db))["rep-ready"] == "ready"

    async with db.begin() as sess:
        await sess.execute(sa.update(Cluster).values(maintenance_notice="PM until 5pm"))
    observer._status_cooldown.clear()
    await observer.poll()
    assert (await _replica_states(db))["rep-ready"] == "unhealthy"


async def test_non_maintenance_failure_leaves_replicas_routable(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_base(sess)
        await _seed_maintenance_replicas(sess)

    observer = _make_observer(db, _make_transport({"GET /status": httpx.Response(500)}))
    await observer.poll()

    states = await _replica_states(db)
    assert states["rep-ready"] == "ready"
    assert states["rep-launching"] == "launching"


async def test_manager_recovery_restores_replicas_marked_in_maintenance(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_base(sess)
        await sess.execute(sa.update(Cluster).values(maintenance_notice="PM until 5pm"))
        await _add_job(sess)
        await _add_replica(sess, name="rep-1", state="ready")

    healthy = False

    def handler(request: httpx.Request) -> httpx.Response:
        if not healthy:
            return httpx.Response(500)
        return httpx.Response(200, json=_status_json([_replica_info("rep-1")]))

    observer = _make_observer(db, httpx.MockTransport(handler))
    await observer.poll()
    assert (await _replica_states(db))["rep-1"] == "unhealthy"

    healthy = True
    observer._status_cooldown.clear()
    await observer.poll()
    assert (await _replica_states(db))["rep-1"] == "ready"


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


async def test_deployment_state_halted_wins_over_draining(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """A tripped launch-failure breaker reports halted, not stopping/offline."""
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess)
        await sess.execute(
            sa.update(PilotDeployment)
            .where(PilotDeployment.name == "pd")
            .values(
                consecutive_launch_failures=PilotDeployment.max_consecutive_launch_failures
                + 1
            )
        )

    transport = _make_transport(
        {"GET /status": httpx.Response(200, json=_status_json())}
    )
    observer = _make_observer(db, transport)
    await observer.poll()

    dep = await _get_deployment(db)
    assert dep.desired_replicas == 0
    assert dep.state == PilotDeploymentState.halted.value


# ── Startup duration (last placed -> ready) ───────────────────────────


async def _get_deployment(
    db: async_sessionmaker[AsyncSession], name: str = "pd"
) -> PilotDeployment:
    async with db() as sess:
        return (
            await sess.scalars(
                select(PilotDeployment).where(PilotDeployment.name == name)
            )
        ).one()


async def _poll_ready(
    db: async_sessionmaker[AsyncSession],
    prior_state: ReplicaState,
    prior_last: float | None,
) -> PilotDeployment:
    """Seed one replica placed 100s ago in `prior_state`, report it ready, poll."""
    placed_at = datetime.now(timezone.utc) - timedelta(seconds=100)
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess)
        await _add_replica(sess, state=prior_state.value, placed_at=placed_at)
        await sess.execute(
            sa.update(PilotDeployment)
            .where(PilotDeployment.name == "pd")
            .values(last_startup_sec=prior_last)
        )

    ri = _replica_info(state=ReplicaState.ready)
    transport = _make_transport(
        {"GET /status": httpx.Response(200, json=_status_json([ri]))}
    )
    await _make_observer(db, transport).poll()
    return await _get_deployment(db)


async def test_launching_to_ready_records_last_startup(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """A clean launching -> ready stores the elapsed placed -> ready time."""
    dep = await _poll_ready(db, ReplicaState.launching, prior_last=None)
    assert dep.last_startup_sec is not None
    # Elapsed is computed against a real now(); allow slack for test runtime.
    assert 100 <= dep.last_startup_sec < 110


async def test_last_startup_overwrites_previous(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Only the most recent startup is kept: no averaging with the old value."""
    dep = await _poll_ready(db, ReplicaState.launching, prior_last=10.0)
    assert dep.last_startup_sec is not None
    assert 100 <= dep.last_startup_sec < 110


async def test_unhealthy_to_ready_does_not_record_startup(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Recovery from unhealthy -> ready is not a startup measurement."""
    dep = await _poll_ready(db, ReplicaState.unhealthy, prior_last=None)
    assert dep.last_startup_sec is None


# ── Write-on-change tests ────────────────────────────────────────────


async def test_steady_state_poll_issues_no_replica_or_deployment_writes(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Once state has converged, a poll reads in batches and writes nothing."""
    names = ["rep-1", "rep-2", "rep-3"]
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess, manager_health=HealthCheckResult.healthy.value)
        for name in names:
            await _add_replica(sess, name=name)
        await sess.execute(
            sa.update(PilotDeployment)
            .where(PilotDeployment.name == "pd")
            .values(desired_replicas=len(names))
        )

    replicas = [_replica_info(name=name, state=ReplicaState.ready) for name in names]
    transport = _make_transport(
        {"GET /status": httpx.Response(200, json=_status_json(replicas))}
    )
    observer = _make_observer(db, transport)
    await observer.poll()  # converge: replicas -> ready, deployment -> healthy

    statements: list[str] = []

    def record(conn, cursor, statement, *args):  # type: ignore[no-untyped-def]
        statements.append(statement)

    engine = db.kw["bind"].sync_engine
    sa.event.listen(engine, "before_cursor_execute", record)
    try:
        await observer.poll()
    finally:
        sa.event.remove(engine, "before_cursor_execute", record)

    replica_table = str(PilotReplica.__table__)
    tables = (replica_table, str(PilotDeployment.__table__))
    writes = [
        s for s in statements if s.startswith("UPDATE") and s.split()[1] in tables
    ]
    assert writes == []

    # One batched replica read each for sync_replicas and reap_orphans, plus
    # the selectin load in update_deployments: independent of replica count.
    replica_reads = [
        s for s in statements if s.startswith("SELECT") and f" {replica_table} " in s
    ]
    assert len(replica_reads) == 3

    dep = await _get_deployment(db)
    assert dep.state == PilotDeploymentState.healthy.value


# ── Per-replica transactions ─────────────────────────────────────────


async def test_stale_transition_is_not_counted(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """If the replica changed under us, the guarded write misses and the
    deployment is not charged for a transition that never committed."""
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess)
        await _add_replica(sess, name="rep-1", state=ReplicaState.launching.value)

    async with db() as sess:
        stale = (
            await sess.scalars(select(PilotReplica).where(PilotReplica.name == "rep-1"))
        ).one()

    # e.g. the drainer terminates it between our read and our write
    async with db.begin() as sess:
        await sess.execute(
            sa.update(PilotReplica)
            .where(PilotReplica.name == "rep-1")
            .values(state=ReplicaState.terminated.value)
        )

    observer = _make_observer(db, _make_transport({}))
    ri = _replica_info(name="rep-1", state=ReplicaState.error)
    assert await observer._update_replica(stale, ri) is False

    dep = await _get_deployment(db)
    assert dep.consecutive_launch_failures == 0


async def test_failed_replica_write_is_isolated_and_atomic(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """A replica whose write fails rolls back with its deployment charge, and
    does not stop the other replicas on the job from syncing."""
    async with db.begin() as sess:
        await _seed_base(sess)
        await _add_job(sess)
        await _add_replica(sess, name="rep-bad", state=ReplicaState.launching.value)
        await _add_replica(sess, name="rep-ok", state=ReplicaState.launching.value)

    # Postgres rejects NUL bytes in text columns.
    bad = _replica_info(
        name="rep-bad", state=ReplicaState.error, state_message="crash\x00"
    )
    ok = _replica_info(name="rep-ok", state=ReplicaState.ready)
    transport = _make_transport(
        {"GET /status": httpx.Response(200, json=_status_json([bad, ok]))}
    )
    observer = _make_observer(db, transport)
    publish = AsyncMock()
    observer.client_state.redis_pubsub.publish = publish  # type: ignore[method-assign]
    await observer.poll()

    async with db() as sess:
        rows = await sess.execute(sa.select(PilotReplica.name, PilotReplica.state))
        states = {name: state for name, state in rows}
    assert states == {
        "rep-bad": ReplicaState.launching.value,
        "rep-ok": ReplicaState.ready.value,
    }
    dep = await _get_deployment(db)
    assert dep.consecutive_launch_failures == 0
    publish.assert_awaited_once()
