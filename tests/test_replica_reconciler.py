"""Tests for ReplicaReconciler: driving replica count toward desired_replicas."""

import itertools
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

_replica_counter = itertools.count()

from first_common.schema.base_scheduler import SchedulerJobState
from first_common.schema.types import ReplicaState
from first_gateway.controllers.workers.replica_reconciler import ReplicaReconciler
from first_gateway.database.models import (
    AccessGroup,
    Cluster,
    Model,
    PilotDeployment,
    PilotJob,
    PilotReplica,
)

NOW = datetime(2026, 7, 12, 12, 0, 0, tzinfo=timezone.utc)

PILOT_SYSTEM = {
    "scheduler_adapter": "fake",
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


def _make_controller(
    db: async_sessionmaker[AsyncSession],
) -> ReplicaReconciler:
    cs = MagicMock()
    cs.db_sessionmaker = db
    return ReplicaReconciler("replica-reconciler", cs, MagicMock())


async def _seed_parents(sess: AsyncSession) -> None:
    sess.add(
        Cluster(
            name="polaris",
            health_check={"url": "http://x/health", "debounce": 2},
            pilot_system=PILOT_SYSTEM,
        )
    )
    sess.add(AccessGroup(name="default-ag", allowed_groups=[], allowed_domains=[]))
    await sess.flush()
    sess.add(
        Model(
            name="llama",
            access_group_name="default-ag",
            supported_endpoints=["chat"],
        )
    )
    await sess.flush()


async def _insert_deployment(
    sess: AsyncSession,
    name: str = "deploy-1",
    *,
    desired_replicas: int = 0,
    reconcile_retry_at: datetime | None = None,
) -> int:
    dep = PilotDeployment(
        name=name,
        cluster_name="polaris",
        model_name="llama",
        router_params={},
        prometheus_scrape_interval_sec=30,
        min_replicas=0,
        max_replicas=10,
        launch_spec={"num_nodes": 1, "gpus_per_node": 4},
        desired_replicas=desired_replicas,
        reconcile_retry_at=reconcile_retry_at,
    )
    sess.add(dep)
    await sess.flush()
    return dep.uid


async def _insert_pilot_job(
    sess: AsyncSession,
    name: str,
    *,
    scheduler_state: SchedulerJobState = SchedulerJobState.running,
    scheduled_deletion_at: datetime | None = None,
) -> str:
    job = PilotJob(
        name=name,
        cluster_name="polaris",
        scheduler_state=scheduler_state.value,
        scheduled_deletion_at=scheduled_deletion_at,
        walltime_min=60,
        num_nodes=1,
        gpus_per_node=4,
    )
    sess.add(job)
    await sess.flush()
    return name


async def _insert_replica(
    sess: AsyncSession,
    deployment_name: str,
    *,
    state: ReplicaState = ReplicaState.ready,
    pilot_job_name: str | None = None,
    scheduled_deletion_at: datetime | None = None,
    deleted_at: datetime | None = None,
    started_at: datetime | None = None,
) -> int:
    r = PilotReplica(
        name=f"{deployment_name}/replica/{next(_replica_counter)}",
        pilot_deployment_name=deployment_name,
        state=state.value,
        pilot_job_name=pilot_job_name,
        scheduled_deletion_at=scheduled_deletion_at,
        deleted_at=deleted_at,
        started_at=started_at,
    )
    sess.add(r)
    await sess.flush()
    return r.uid


async def _get_replica(db: async_sessionmaker[AsyncSession], uid: int) -> PilotReplica:
    async with db() as sess:
        r = await sess.get(PilotReplica, uid)
    assert r is not None
    return r


async def _count_replicas(
    db: async_sessionmaker[AsyncSession], deployment_name: str
) -> int:
    import sqlalchemy as sa

    async with db() as sess:
        return (
            await sess.scalar(
                sa.select(sa.func.count())
                .select_from(PilotReplica)
                .where(PilotReplica.pilot_deployment_name == deployment_name)
            )
            or 0
        )


# ---------------------------------------------------------------------------
# list_actionable
# ---------------------------------------------------------------------------


async def test_list_actionable_includes_all_deployments(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        uid1 = await _insert_deployment(sess, "dep-a", desired_replicas=1)
        uid2 = await _insert_deployment(sess, "dep-b", desired_replicas=0)

    ctrl = _make_controller(db)
    async with db() as sess:
        actionable = await ctrl.list_actionable(sess)

    assert set(actionable) == {uid1, uid2}


async def test_list_actionable_excludes_backoff(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(
            sess,
            "dep-backoff",
            reconcile_retry_at=datetime.now(timezone.utc) + timedelta(hours=1),
        )

    ctrl = _make_controller(db)
    async with db() as sess:
        actionable = await ctrl.list_actionable(sess)

    assert actionable == []


# ---------------------------------------------------------------------------
# reconcile: no-op when count matches
# ---------------------------------------------------------------------------


async def test_reconcile_noop_when_live_equals_desired(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        uid = await _insert_deployment(sess, "dep-eq", desired_replicas=2)
        job_name = await _insert_pilot_job(sess, "job-eq")
        await _insert_replica(sess, "dep-eq", pilot_job_name=job_name)
        await _insert_replica(sess, "dep-eq", pilot_job_name=job_name)

    ctrl = _make_controller(db)
    await ctrl.reconcile(uid)

    assert await _count_replicas(db, "dep-eq") == 2


async def test_reconcile_missing_deployment_is_noop(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)

    ctrl = _make_controller(db)
    await ctrl.reconcile(999_999)


# ---------------------------------------------------------------------------
# reconcile: scale up
# ---------------------------------------------------------------------------


async def test_scale_up_creates_pending_replicas(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        uid = await _insert_deployment(sess, "dep-up", desired_replicas=3)

    ctrl = _make_controller(db)
    await ctrl.reconcile(uid)

    assert await _count_replicas(db, "dep-up") == 3


async def test_scale_up_accounts_for_existing_live_replicas(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        uid = await _insert_deployment(sess, "dep-partial", desired_replicas=3)
        job_name = await _insert_pilot_job(sess, "job-partial")
        await _insert_replica(sess, "dep-partial", pilot_job_name=job_name)

    ctrl = _make_controller(db)
    await ctrl.reconcile(uid)

    assert await _count_replicas(db, "dep-partial") == 3


# ---------------------------------------------------------------------------
# reconcile: scale down
# ---------------------------------------------------------------------------


async def test_scale_down_drains_excess_replicas(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        uid = await _insert_deployment(sess, "dep-down", desired_replicas=1)
        job_name = await _insert_pilot_job(sess, "job-down")
        r1 = await _insert_replica(
            sess,
            "dep-down",
            state=ReplicaState.pending,
            pilot_job_name=job_name,
        )
        r2 = await _insert_replica(
            sess,
            "dep-down",
            state=ReplicaState.ready,
            pilot_job_name=job_name,
            started_at=NOW,
        )

    ctrl = _make_controller(db)
    await ctrl.reconcile(uid)

    # pending drains before ready
    drained = await _get_replica(db, r1)
    assert drained.scheduled_deletion_at is not None

    kept = await _get_replica(db, r2)
    assert kept.scheduled_deletion_at is None


async def test_scale_down_drain_priority_order(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Drains in order: pending > placed > unhealthy > launching > ready."""
    async with db.begin() as sess:
        await _seed_parents(sess)
        uid = await _insert_deployment(sess, "dep-prio", desired_replicas=1)
        job_name = await _insert_pilot_job(sess, "job-prio")
        r_pending = await _insert_replica(sess, "dep-prio", state=ReplicaState.pending)
        r_placed = await _insert_replica(
            sess, "dep-prio", state=ReplicaState.placed, pilot_job_name=job_name
        )
        r_ready = await _insert_replica(
            sess,
            "dep-prio",
            state=ReplicaState.ready,
            pilot_job_name=job_name,
            started_at=NOW,
        )

    ctrl = _make_controller(db)
    await ctrl.reconcile(uid)

    # With desired=1, 2 of 3 should be drained: pending first, then placed
    assert (await _get_replica(db, r_pending)).scheduled_deletion_at is not None
    assert (await _get_replica(db, r_placed)).scheduled_deletion_at is not None
    assert (await _get_replica(db, r_ready)).scheduled_deletion_at is None


# ---------------------------------------------------------------------------
# reconcile: drain-by-predicate (terminal replicas, dying parent jobs)
# ---------------------------------------------------------------------------


async def test_drain_terminal_replica(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        uid = await _insert_deployment(sess, "dep-term", desired_replicas=1)
        job_name = await _insert_pilot_job(sess, "job-term")
        r_error = await _insert_replica(
            sess, "dep-term", state=ReplicaState.error, pilot_job_name=job_name
        )
        r_ready = await _insert_replica(
            sess,
            "dep-term",
            state=ReplicaState.ready,
            pilot_job_name=job_name,
            started_at=NOW,
        )

    ctrl = _make_controller(db)
    await ctrl.reconcile(uid)

    drained = await _get_replica(db, r_error)
    assert drained.scheduled_deletion_at is not None

    kept = await _get_replica(db, r_ready)
    assert kept.scheduled_deletion_at is None


async def test_drain_replica_with_dying_parent_job(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        uid = await _insert_deployment(sess, "dep-dying", desired_replicas=2)
        dying_job = await _insert_pilot_job(
            sess, "job-dying", scheduler_state=SchedulerJobState.exiting
        )
        healthy_job = await _insert_pilot_job(sess, "job-healthy")
        r_on_dying = await _insert_replica(
            sess,
            "dep-dying",
            state=ReplicaState.ready,
            pilot_job_name=dying_job,
            started_at=NOW,
        )
        r_on_healthy = await _insert_replica(
            sess,
            "dep-dying",
            state=ReplicaState.ready,
            pilot_job_name=healthy_job,
            started_at=NOW,
        )

    ctrl = _make_controller(db)
    await ctrl.reconcile(uid)

    drained = await _get_replica(db, r_on_dying)
    assert drained.scheduled_deletion_at is not None

    kept = await _get_replica(db, r_on_healthy)
    assert kept.scheduled_deletion_at is None


async def test_drain_replica_with_scheduled_deletion_parent_job(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        uid = await _insert_deployment(sess, "dep-deljob", desired_replicas=1)
        del_job = await _insert_pilot_job(sess, "job-deljob", scheduled_deletion_at=NOW)
        r = await _insert_replica(
            sess,
            "dep-deljob",
            state=ReplicaState.ready,
            pilot_job_name=del_job,
            started_at=NOW,
        )

    ctrl = _make_controller(db)
    await ctrl.reconcile(uid)

    drained = await _get_replica(db, r)
    assert drained.scheduled_deletion_at is not None


# ---------------------------------------------------------------------------
# reconcile: already-drained or deleted replicas are skipped
# ---------------------------------------------------------------------------


async def test_already_draining_replicas_not_double_flagged(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        uid = await _insert_deployment(sess, "dep-already", desired_replicas=0)
        job_name = await _insert_pilot_job(sess, "job-already")
        r = await _insert_replica(
            sess,
            "dep-already",
            state=ReplicaState.ready,
            pilot_job_name=job_name,
            scheduled_deletion_at=NOW - timedelta(minutes=5),
        )

    ctrl = _make_controller(db)
    await ctrl.reconcile(uid)

    replica = await _get_replica(db, r)
    # Original timestamp preserved, not overwritten
    assert replica.scheduled_deletion_at == NOW - timedelta(minutes=5)


async def test_deleted_replicas_excluded_from_live_count(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        uid = await _insert_deployment(sess, "dep-del", desired_replicas=1)
        job_name = await _insert_pilot_job(sess, "job-del")
        await _insert_replica(
            sess,
            "dep-del",
            state=ReplicaState.ready,
            pilot_job_name=job_name,
            deleted_at=NOW,
        )

    ctrl = _make_controller(db)
    await ctrl.reconcile(uid)

    # The deleted replica doesn't count; 1 new one should be created
    assert await _count_replicas(db, "dep-del") == 2


# ---------------------------------------------------------------------------
# drain + scale-up in a single reconcile
# ---------------------------------------------------------------------------


async def test_drain_terminal_and_scale_up_in_one_pass(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Terminal replicas are drained AND replacement replicas are created."""
    async with db.begin() as sess:
        await _seed_parents(sess)
        uid = await _insert_deployment(sess, "dep-combo", desired_replicas=2)
        job_name = await _insert_pilot_job(sess, "job-combo")
        r_error = await _insert_replica(
            sess, "dep-combo", state=ReplicaState.error, pilot_job_name=job_name
        )
        r_ready = await _insert_replica(
            sess,
            "dep-combo",
            state=ReplicaState.ready,
            pilot_job_name=job_name,
            started_at=NOW,
        )

    ctrl = _make_controller(db)
    await ctrl.reconcile(uid)

    assert (await _get_replica(db, r_error)).scheduled_deletion_at is not None
    assert (await _get_replica(db, r_ready)).scheduled_deletion_at is None

    # 1 ready + 1 new pending = 2 live, plus the drained error = 3 total rows
    assert await _count_replicas(db, "dep-combo") == 3
