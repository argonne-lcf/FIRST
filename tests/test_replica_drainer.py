"""Tests for ReplicaDrainer: tearing down replicas flagged for deletion."""

import itertools
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import selectinload

_replica_counter = itertools.count()

from first_common.schema.base_scheduler import SchedulerJobState
from first_common.schema.resources.runtime import BackendRuntime
from first_common.schema.types import ReplicaState
from first_gateway.controllers.controller import StaleReconcile
from first_gateway.controllers.worker import Worker
from first_gateway.controllers.workers.replica_drainer import ReplicaDrainer
from first_gateway.database.models import (
    AccessGroup,
    Cluster,
    Model,
    PilotDeployment,
    PilotJob,
    PilotReplica,
)
from first_gateway.services.pilot_control import PilotControlClient

NOW = datetime(2026, 7, 12, 12, 0, 0, tzinfo=timezone.utc)

MANAGER_URL = "https://10.0.0.1:8443/control"


# ── Transport / controller construction ─────────────────────────────────


def _reject(request: httpx.Request) -> httpx.Response:
    """Handler for tests that never expect an HTTP call to be made."""
    return httpx.Response(404)


def _ok(request: httpx.Request) -> httpx.Response:
    """Handler that accepts any stop-replica call."""
    return httpx.Response(200)


def _not_found(request: httpx.Request) -> httpx.Response:
    """Handler simulating a manager that no longer knows this replica."""
    return httpx.Response(404)


def _server_error(request: httpx.Request) -> httpx.Response:
    """Handler simulating a transient manager failure."""
    return httpx.Response(500)


def _make_controller(
    db: async_sessionmaker[AsyncSession],
    handler: Any = _reject,
    *,
    inflight: int = 0,
) -> ReplicaDrainer:
    cs = MagicMock()
    cs.db_sessionmaker = db
    cs.redis_pubsub.publish = AsyncMock()
    cs.redis_repo.get_backend_runtime = AsyncMock(
        return_value=BackendRuntime(inflight=inflight)
    )
    ctrl = ReplicaDrainer.__new__(ReplicaDrainer)
    Worker.__init__(ctrl, "replica-drainer", cs, MagicMock())
    client = PilotControlClient.__new__(PilotControlClient)
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    ctrl.client = client
    return ctrl


# ── Seed helpers ────────────────────────────────────────────────────────


async def _seed_parents(sess: AsyncSession) -> None:
    sess.add(
        Cluster(
            name="polaris",
            health_check={"url": "http://x/health", "debounce": 2},
        )
    )
    sess.add(AccessGroup(name="default-ag", allowed_groups=[], allowed_domains=[]))
    await sess.flush()
    sess.add(
        Model(
            name="llama", access_group_name="default-ag", supported_endpoints=["chat"]
        )
    )
    await sess.flush()


async def _insert_deployment(
    sess: AsyncSession,
    name: str = "deploy-1",
) -> None:
    sess.add(
        PilotDeployment(
            name=name,
            cluster_name="polaris",
            model_name="llama",
            router_params={},
            prometheus_scrape_interval_sec=30,
            min_replicas=0,
            max_replicas=10,
            launch_spec={"num_nodes": 1, "gpus_per_node": 4},
        )
    )
    await sess.flush()


async def _insert_job(
    sess: AsyncSession,
    name: str = "job-1",
    *,
    scheduler_state: SchedulerJobState = SchedulerJobState.running,
    manager_url: str | None = MANAGER_URL,
    claimed_gpu_ids: list[tuple[int, int]] | None = None,
    scheduled_deletion_at: datetime | None = None,
    deleted_at: datetime | None = None,
) -> str:
    sess.add(
        PilotJob(
            name=name,
            cluster_name="polaris",
            scheduler_state=scheduler_state.value,
            manager_url=manager_url,
            claimed_gpu_ids=claimed_gpu_ids or [(0, 0), (0, 1)],
            scheduled_deletion_at=scheduled_deletion_at,
            deleted_at=deleted_at,
            walltime_min=60,
            num_nodes=1,
            gpus_per_node=4,
        )
    )
    await sess.flush()
    return name


async def _insert_replica(
    sess: AsyncSession,
    deployment_name: str = "deploy-1",
    *,
    state: ReplicaState = ReplicaState.ready,
    pilot_job_name: str | None = "job-1",
    claimed_gpu_ids: list[tuple[int, int]] | None = None,
    scheduled_deletion_at: datetime | None = NOW,
    deleted_at: datetime | None = None,
    stopped_at: datetime | None = None,
    reconcile_retry_at: datetime | None = None,
    reconcile_failures: int = 0,
    reconcile_last_error: str | None = None,
) -> int:
    r = PilotReplica(
        name=f"{deployment_name}/replica/{next(_replica_counter)}",
        pilot_deployment_name=deployment_name,
        state=state.value,
        pilot_job_name=pilot_job_name,
        claimed_gpu_ids=claimed_gpu_ids or [(0, 0), (0, 1)],
        scheduled_deletion_at=scheduled_deletion_at,
        deleted_at=deleted_at,
        stopped_at=stopped_at,
        reconcile_retry_at=reconcile_retry_at,
        reconcile_failures=reconcile_failures,
        reconcile_last_error=reconcile_last_error,
    )
    sess.add(r)
    await sess.flush()
    return r.uid


async def _get_replica(db: async_sessionmaker[AsyncSession], uid: int) -> PilotReplica:
    async with db() as sess:
        r = await sess.get(PilotReplica, uid)
    assert r is not None
    return r


async def _get_job(db: async_sessionmaker[AsyncSession], name: str) -> PilotJob:
    async with db() as sess:
        j = await PilotJob.get_by_name(sess, name)
    return j


# ── list_actionable ──────────────────────────────────────────────────────


async def test_list_actionable_includes_flagged_not_deleted(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess)
        uid = await _insert_replica(sess, scheduled_deletion_at=NOW)

    ctrl = _make_controller(db)
    async with db() as sess:
        actionable = await ctrl.list_actionable(sess)

    assert actionable == [uid]


async def test_list_actionable_excludes_unflagged(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess)
        await _insert_replica(sess, scheduled_deletion_at=None)

    ctrl = _make_controller(db)
    async with db() as sess:
        actionable = await ctrl.list_actionable(sess)

    assert actionable == []


async def test_list_actionable_excludes_already_deleted(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess)
        await _insert_replica(sess, scheduled_deletion_at=NOW, deleted_at=NOW)

    ctrl = _make_controller(db)
    async with db() as sess:
        actionable = await ctrl.list_actionable(sess)

    assert actionable == []


async def test_list_actionable_excludes_backoff(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess)
        await _insert_replica(
            sess,
            scheduled_deletion_at=NOW,
            reconcile_retry_at=datetime.now(timezone.utc) + timedelta(hours=1),
        )

    ctrl = _make_controller(db)
    async with db() as sess:
        actionable = await ctrl.list_actionable(sess)

    assert actionable == []


# ── reconcile: happy path ────────────────────────────────────────────────


async def test_reconcile_stops_and_frees_non_ready_replica(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """A launching replica is drained immediately (no eligibility gate)."""
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess)
        uid = await _insert_replica(sess, state=ReplicaState.launching)

    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        return httpx.Response(200)

    ctrl = _make_controller(db, handler)
    await ctrl.reconcile(uid)

    replica = await _get_replica(db, uid)
    assert replica.state == ReplicaState.terminated.value
    assert replica.deleted_at is not None
    assert replica.stopped_at is not None
    assert replica.claimed_gpu_ids == []
    assert seen["path"].endswith("/stop-replica/" + replica.name)

    # GPUs released on the parent job
    job = await _get_job(db, "job-1")
    assert job.claimed_gpu_ids == []

    # The eligibility gate is not consulted for non-ready replicas.
    ctrl.client_state.redis_repo.get_backend_runtime.assert_not_awaited()  # type: ignore[attr-defined]


async def test_reconcile_missing_replica_is_noop(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)

    ctrl = _make_controller(db)
    await ctrl.reconcile(999_999)


async def test_reconcile_unflagged_replica_is_noop(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Stale wake: row is no longer flagged for deletion."""
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess)
        uid = await _insert_replica(sess, scheduled_deletion_at=None)

    ctrl = _make_controller(db, _reject)
    await ctrl.reconcile(uid)

    replica = await _get_replica(db, uid)
    assert replica.deleted_at is None
    assert replica.state == ReplicaState.ready.value


# ── reconcile: terminal state preservation ───────────────────────────────


async def test_reconcile_preserves_error_state(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess)
        uid = await _insert_replica(sess, state=ReplicaState.error)

    ctrl = _make_controller(db, _ok)
    await ctrl.reconcile(uid)

    replica = await _get_replica(db, uid)
    assert replica.state == ReplicaState.error.value
    assert replica.deleted_at is not None


async def test_reconcile_preserves_existing_stopped_at(
    db: async_sessionmaker[AsyncSession],
) -> None:
    earlier = NOW - timedelta(minutes=10)
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess)
        uid = await _insert_replica(
            sess, state=ReplicaState.start_timeout, stopped_at=earlier
        )

    ctrl = _make_controller(db, _ok)
    await ctrl.reconcile(uid)

    replica = await _get_replica(db, uid)
    assert replica.state == ReplicaState.start_timeout.value
    assert replica.stopped_at == earlier


# ── reconcile: manager unreachable ───────────────────────────────────────


async def test_reconcile_skips_rpc_when_job_not_running(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Job gone/exiting: still free DB resources, but don't call the manager."""
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess, scheduler_state=SchedulerJobState.gone)
        uid = await _insert_replica(sess, state=ReplicaState.launching)

    called = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal called
        called = True
        return httpx.Response(200)

    ctrl = _make_controller(db, handler)
    await ctrl.reconcile(uid)

    assert not called
    replica = await _get_replica(db, uid)
    assert replica.state == ReplicaState.terminated.value
    assert replica.deleted_at is not None
    job = await _get_job(db, "job-1")
    assert job.claimed_gpu_ids == []


async def test_exiting_parent_requests_qdel_and_retains_claims_until_gone(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess, scheduler_state=SchedulerJobState.exiting)
        uid = await _insert_replica(sess, state=ReplicaState.launching)

    ctrl = _make_controller(db, _reject)
    await ctrl.reconcile(uid)

    replica = await _get_replica(db, uid)
    job = await _get_job(db, "job-1")
    assert replica.deleted_at is None
    assert replica.claimed_gpu_ids == [(0, 0), (0, 1)]
    assert replica.state == ReplicaState.launching.value
    assert job.scheduled_deletion_at is not None
    assert job.claimed_gpu_ids == [(0, 0), (0, 1)]


async def test_reconcile_handles_replica_without_job(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        uid = await _insert_replica(
            sess,
            state=ReplicaState.pending,
            pilot_job_name=None,
            claimed_gpu_ids=[],
        )

    ctrl = _make_controller(db, _reject)
    await ctrl.reconcile(uid)

    replica = await _get_replica(db, uid)
    assert replica.state == ReplicaState.terminated.value
    assert replica.deleted_at is not None


# ── reconcile: stop-replica response handling ────────────────────────────


async def test_reconcile_tolerates_404_from_manager(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Double-delete: manager 404 is fine, drain still completes."""
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess)
        uid = await _insert_replica(sess, state=ReplicaState.launching)

    ctrl = _make_controller(db, _not_found)
    await ctrl.reconcile(uid)

    replica = await _get_replica(db, uid)
    assert replica.state == ReplicaState.terminated.value
    assert replica.deleted_at is not None


async def test_reconcile_raises_on_manager_500(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """5xx from the manager bubbles up; the replica is not finalized."""
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess)
        uid = await _insert_replica(sess, state=ReplicaState.launching)

    ctrl = _make_controller(db, _server_error)
    try:
        await ctrl.reconcile(uid)
    except RuntimeError as exc:
        assert "replica cleanup verification failed" in str(exc)
        pass
    else:
        raise AssertionError("expected HTTPStatusError on 500")

    replica = await _get_replica(db, uid)
    assert replica.deleted_at is None
    assert replica.state == ReplicaState.launching.value


async def test_two_stop_failures_force_parent_qdel_without_early_release(
    db: async_sessionmaker[AsyncSession],
) -> None:
    calls = 0

    def failing_handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500)

    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess)
        uid = await _insert_replica(sess, state=ReplicaState.launching)

    ctrl = _make_controller(db, failing_handler)
    await ctrl._reconcile_one(uid)
    await ctrl._reconcile_one(uid)

    replica = await _get_replica(db, uid)
    assert replica.reconcile_failures == 2
    assert replica.deleted_at is None
    assert replica.claimed_gpu_ids == [(0, 0), (0, 1)]
    job = await _get_job(db, "job-1")
    assert job.scheduled_deletion_at is None
    assert job.claimed_gpu_ids == [(0, 0), (0, 1)]

    # The next action escalates without a third manager-stop transaction.
    await ctrl._reconcile_one(uid)
    assert calls == 6  # PilotControlClient's three HTTP attempts x two failures.
    replica = await _get_replica(db, uid)
    job = await _get_job(db, "job-1")
    assert replica.state == ReplicaState.error.value
    assert "cleanup verification FAILED" in replica.state_message
    assert replica.deleted_at is None
    assert replica.claimed_gpu_ids == [(0, 0), (0, 1)]
    assert job.scheduled_deletion_at is not None
    assert job.deleted_at is None
    assert job.claimed_gpu_ids == [(0, 0), (0, 1)]

    # Parent qdel has been requested but the allocation is still live: no DB
    # release and no false PASS are allowed.
    await ctrl.reconcile(uid)
    replica = await _get_replica(db, uid)
    assert replica.deleted_at is None
    assert replica.claimed_gpu_ids == [(0, 0), (0, 1)]

    async with db.begin() as sess:
        await sess.execute(
            sa.update(PilotJob)
            .where(PilotJob.name == "job-1")
            .values(
                scheduler_state=SchedulerJobState.gone.value,
                deleted_at=datetime.now(timezone.utc),
            )
        )
    await ctrl.reconcile(uid)

    replica = await _get_replica(db, uid)
    job = await _get_job(db, "job-1")
    assert replica.deleted_at is not None
    assert replica.claimed_gpu_ids == []
    assert replica.state == ReplicaState.error.value
    assert "cleanup verification FAILED" in replica.state_message
    assert job.claimed_gpu_ids == []


async def test_forced_cleanup_escalation_rejects_parent_assignment_race(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess)
        await _insert_job(sess, name="job-2", claimed_gpu_ids=[])
        uid = await _insert_replica(
            sess,
            state=ReplicaState.launching,
            reconcile_failures=2,
            reconcile_last_error="replica cleanup verification failed: timeout",
        )

    ctrl = _make_controller(db, _reject)
    async with db() as sess:
        replica = await sess.get(
            PilotReplica,
            uid,
            options=[
                # Match the production detached snapshot used by reconcile.
                selectinload(PilotReplica.pilot_job),
                selectinload(PilotReplica.pilot_deployment),
            ],
        )
    assert replica is not None
    async with db.begin() as sess:
        await sess.execute(
            sa.update(PilotReplica)
            .where(PilotReplica.uid == uid)
            .values(pilot_job_name="job-2")
        )

    with pytest.raises(StaleReconcile, match="disappeared during escalation"):
        await ctrl._request_parent_termination(replica, cleanup_unverified=True)

    assert (await _get_job(db, "job-1")).scheduled_deletion_at is None
    assert (await _get_job(db, "job-2")).scheduled_deletion_at is None


# ── reconcile: ready eligibility gate ────────────────────────────────────


async def test_ready_not_eligible_before_min_wait(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """A ready replica flagged <20s ago is left alone this pass."""
    recent = datetime.now(timezone.utc) - timedelta(seconds=5)
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess)
        uid = await _insert_replica(
            sess, state=ReplicaState.ready, scheduled_deletion_at=recent
        )

    ctrl = _make_controller(db, _reject)
    await ctrl.reconcile(uid)

    replica = await _get_replica(db, uid)
    assert replica.deleted_at is None
    assert replica.state == ReplicaState.ready.value


async def test_ready_eligible_after_min_wait_when_idle(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Past 20s with zero inflight → drain."""
    flagged = datetime.now(timezone.utc) - timedelta(seconds=30)
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess)
        uid = await _insert_replica(
            sess, state=ReplicaState.ready, scheduled_deletion_at=flagged
        )

    ctrl = _make_controller(db, _ok, inflight=0)
    await ctrl.reconcile(uid)

    replica = await _get_replica(db, uid)
    assert replica.state == ReplicaState.terminated.value
    assert replica.deleted_at is not None


async def test_ready_not_eligible_with_inflight_before_max_wait(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Past 20s but requests still in flight and under 300s → wait."""
    flagged = datetime.now(timezone.utc) - timedelta(seconds=30)
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess)
        uid = await _insert_replica(
            sess, state=ReplicaState.ready, scheduled_deletion_at=flagged
        )

    ctrl = _make_controller(db, _reject, inflight=3)
    await ctrl.reconcile(uid)

    replica = await _get_replica(db, uid)
    assert replica.deleted_at is None
    assert replica.state == ReplicaState.ready.value


async def test_ready_eligible_after_max_wait_despite_inflight(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Past 300s → drain even with requests still in flight."""
    flagged = datetime.now(timezone.utc) - timedelta(seconds=400)
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess)
        uid = await _insert_replica(
            sess, state=ReplicaState.ready, scheduled_deletion_at=flagged
        )

    ctrl = _make_controller(db, _ok, inflight=5)
    await ctrl.reconcile(uid)

    replica = await _get_replica(db, uid)
    assert replica.state == ReplicaState.terminated.value
    assert replica.deleted_at is not None
    # Past the hard cap we don't even consult inflight.
    ctrl.client_state.redis_repo.get_backend_runtime.assert_not_awaited()  # type: ignore[attr-defined]
