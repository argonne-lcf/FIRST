"""Tests for ReplicaPlacer: scheduling pending replicas onto pilot jobs."""

import itertools
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

_replica_counter = itertools.count()

from first_common.schema.base_scheduler import SchedulerJobState
from first_common.schema.types import ReplicaState
from first_gateway.controllers.controller import StaleReconcile
from first_gateway.controllers.workers.replica_placement import (
    AT_CAPACITY,
    ReplicaPlacer,
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

# A valid, importable scheduler_adapter so PilotConfig.model_validate passes.
PILOT_SYSTEM: dict[str, Any] = {
    "scheduler_adapter": (
        "first_gateway.platforms.schedulers.globus_compute_pbs.GlobusComputePBSAdapter"
    ),
    "scheduler_config": {},
    "job_walltime_min": 60,
    "pilot_max_idle_time_min": 60,
    "pilot_max_unhealthy_time_min": 5,
    "max_concurrent_jobs": 3,
    "max_num_nodes": 8,
    "gpus_per_node": 4,
    "queue": "debug",
    "account": "TestAcct",
    "scheduler_flags": "",
    "workdir": "/tmp/pilot_workdir",
    "external_port": 8443,
    "nginx_path": "/usr/sbin/nginx",
    "ip_allowlist": ["10.0.0.0/8"],
    "node_file_env": "PBS_NODEFILE",
    "submit_script_preamble": "#!/bin/bash",
    "pilot_path": "/test/first-pilot",
}


def _make_controller(db: async_sessionmaker[AsyncSession]) -> ReplicaPlacer:
    cs = MagicMock()
    cs.db_sessionmaker = db
    cs.redis_pubsub.publish = AsyncMock()
    return ReplicaPlacer("replica-placer", cs, MagicMock())


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
    num_nodes: int = 1,
    gpus_per_node: int = 2,
) -> str:
    dep = PilotDeployment(
        name=name,
        cluster_name="polaris",
        model_name="llama",
        router_params={},
        prometheus_scrape_interval_sec=30,
        min_replicas=0,
        max_replicas=10,
        launch_spec={"num_nodes": num_nodes, "gpus_per_node": gpus_per_node},
        desired_replicas=0,
    )
    sess.add(dep)
    await sess.flush()
    return name


async def _insert_pilot_job(
    sess: AsyncSession,
    name: str,
    *,
    scheduler_state: SchedulerJobState = SchedulerJobState.running,
    scheduled_deletion_at: datetime | None = None,
    num_nodes: int = 1,
    gpus_per_node: int = 4,
    claimed_gpu_ids: list[tuple[int, int]] | None = None,
) -> str:
    job = PilotJob(
        name=name,
        cluster_name="polaris",
        scheduler_state=scheduler_state.value,
        scheduled_deletion_at=scheduled_deletion_at,
        walltime_min=60,
        num_nodes=num_nodes,
        gpus_per_node=gpus_per_node,
        claimed_gpu_ids=claimed_gpu_ids or [],
    )
    sess.add(job)
    await sess.flush()
    return name


async def _insert_replica(
    sess: AsyncSession,
    deployment_name: str,
    *,
    state: ReplicaState = ReplicaState.pending,
    created_at: datetime | None = None,
    scheduled_deletion_at: datetime | None = None,
) -> int:
    r = PilotReplica(
        name=f"{deployment_name}/replica/{next(_replica_counter)}",
        pilot_deployment_name=deployment_name,
        state=state.value,
        scheduled_deletion_at=scheduled_deletion_at,
    )
    if created_at is not None:
        r.created_at = created_at
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
        j = await sess.scalar(sa.select(PilotJob).where(PilotJob.name == name))
    assert j is not None
    return j


async def _count_jobs(db: async_sessionmaker[AsyncSession]) -> int:
    async with db() as sess:
        return (
            await sess.scalar(sa.select(sa.func.count()).select_from(PilotJob))
        ) or 0


# ---------------------------------------------------------------------------
# list_actionable
# ---------------------------------------------------------------------------


async def test_list_actionable_only_pending_not_draining(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess, "dep")
        pending = await _insert_replica(sess, "dep", state=ReplicaState.pending)
        await _insert_replica(sess, "dep", state=ReplicaState.placed)
        await _insert_replica(sess, "dep", state=ReplicaState.ready)
        await _insert_replica(
            sess, "dep", state=ReplicaState.pending, scheduled_deletion_at=NOW
        )

    ctrl = _make_controller(db)
    async with db() as sess:
        actionable = await ctrl.list_actionable(sess)

    assert actionable == [pending]


async def test_list_actionable_orders_by_effective_submit_time(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Larger replica created slightly later still sorts first (BETA head start)."""
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess, "small", gpus_per_node=1)
        await _insert_deployment(sess, "big", gpus_per_node=4)
        # small created 10 min earlier than big
        small = await _insert_replica(
            sess, "small", created_at=NOW - timedelta(minutes=10)
        )
        big = await _insert_replica(sess, "big", created_at=NOW)

    ctrl = _make_controller(db)
    async with db() as sess:
        actionable = await ctrl.list_actionable(sess)

    # big: t_eff = NOW - 5*4 = NOW - 20min; small: t_eff = NOW - 10min. big first.
    assert actionable == [big, small]


# ---------------------------------------------------------------------------
# reconcile: placement onto existing jobs
# ---------------------------------------------------------------------------


async def test_place_on_existing_job(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess, "dep", num_nodes=1, gpus_per_node=2)
        await _insert_pilot_job(sess, "job-1", num_nodes=1, gpus_per_node=4)
        uid = await _insert_replica(sess, "dep")

    ctrl = _make_controller(db)
    await ctrl.reconcile(uid)

    replica = await _get_replica(db, uid)
    assert replica.state == ReplicaState.placed.value
    assert replica.pilot_job_name == "job-1"
    # Fills from lowest free GPU indexes.
    assert set(replica.claimed_gpu_ids) == {(0, 0), (0, 1)}

    job = await _get_job(db, "job-1")
    assert set(job.claimed_gpu_ids) == {(0, 0), (0, 1)}
    # No new job created.
    assert await _count_jobs(db) == 1

    # Placement wakes the Launch controller.
    from first_gateway.database.redis.pubsub import Channel

    publish: AsyncMock = ctrl.client_state.redis_pubsub.publish  # type: ignore[assignment]
    publish.assert_awaited_once_with(Channel.replica_placed, replica.name)


async def test_place_fills_lowest_free_gpus(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess, "dep", num_nodes=1, gpus_per_node=2)
        # GPU 0 already claimed; free = {1, 2, 3}
        await _insert_pilot_job(
            sess, "job-1", num_nodes=1, gpus_per_node=4, claimed_gpu_ids=[(0, 0)]
        )
        uid = await _insert_replica(sess, "dep")

    ctrl = _make_controller(db)
    await ctrl.reconcile(uid)

    replica = await _get_replica(db, uid)
    assert set(replica.claimed_gpu_ids) == {(0, 1), (0, 2)}


async def test_best_fit_prefers_fullest_fitting_node(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """A 2-GPU replica lands on the job with the fewest free GPUs that fits."""
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess, "dep", num_nodes=1, gpus_per_node=2)
        # empty: 4 free
        await _insert_pilot_job(sess, "job-empty", num_nodes=1, gpus_per_node=4)
        # 2 free (exact fit) -> best fit
        await _insert_pilot_job(
            sess,
            "job-tight",
            num_nodes=1,
            gpus_per_node=4,
            claimed_gpu_ids=[(0, 0), (0, 1)],
        )
        uid = await _insert_replica(sess, "dep")

    ctrl = _make_controller(db)
    await ctrl.reconcile(uid)

    replica = await _get_replica(db, uid)
    assert replica.pilot_job_name == "job-tight"
    assert set(replica.claimed_gpu_ids) == {(0, 2), (0, 3)}


async def test_skips_job_without_enough_free_gpus(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess, "dep", num_nodes=1, gpus_per_node=3)
        # only 1 free GPU -> cannot fit; must create a new job
        await _insert_pilot_job(
            sess,
            "job-full",
            num_nodes=1,
            gpus_per_node=4,
            claimed_gpu_ids=[(0, 0), (0, 1), (0, 2)],
        )
        uid = await _insert_replica(sess, "dep")

    ctrl = _make_controller(db)
    await ctrl.reconcile(uid)

    replica = await _get_replica(db, uid)
    assert replica.state == ReplicaState.placed.value
    assert replica.pilot_job_name != "job-full"
    assert await _count_jobs(db) == 2


async def test_ignores_draining_and_terminal_jobs(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess, "dep", num_nodes=1, gpus_per_node=2)
        await _insert_pilot_job(
            sess, "job-gone", scheduler_state=SchedulerJobState.gone
        )
        await _insert_pilot_job(sess, "job-draining", scheduled_deletion_at=NOW)
        uid = await _insert_replica(sess, "dep")

    ctrl = _make_controller(db)
    await ctrl.reconcile(uid)

    replica = await _get_replica(db, uid)
    # Neither existing job is eligible -> a new job is created.
    assert replica.state == ReplicaState.placed.value
    assert replica.pilot_job_name not in {"job-gone", "job-draining"}


async def test_place_on_pending_submit_job(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Replicas can be placed onto not-yet-running jobs."""
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess, "dep", num_nodes=1, gpus_per_node=2)
        await _insert_pilot_job(
            sess,
            "job-pending",
            scheduler_state=SchedulerJobState.pending_submit,
            num_nodes=1,
            gpus_per_node=4,
        )
        uid = await _insert_replica(sess, "dep")

    ctrl = _make_controller(db)
    await ctrl.reconcile(uid)

    replica = await _get_replica(db, uid)
    assert replica.pilot_job_name == "job-pending"
    assert await _count_jobs(db) == 1


async def test_stale_selected_job_cannot_be_assigned_after_idle_delete_mark(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Locked placement rechecks a job marked deleting after candidate read."""
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess, "dep", num_nodes=1, gpus_per_node=2)
        await _insert_pilot_job(sess, "job-selected", num_nodes=1, gpus_per_node=4)
        replica_uid = await _insert_replica(sess, "dep")

    # ReplicaPlacer selected this detached candidate while it was placeable.
    selected = await _get_job(db, "job-selected")

    # PilotJobController wins the inter-controller gap and marks it deleting.
    async with db.begin() as sess:
        job = await sess.get(PilotJob, selected.uid)
        assert job is not None
        job.scheduled_deletion_at = NOW

    ctrl = _make_controller(db)
    with pytest.raises(StaleReconcile, match="lost race for GPUs"):
        await ctrl._place(
            replica_uid,
            "dep/replica/stale-selection",
            selected.uid,
            selected.name,
            {(0, 0), (0, 1)},
        )

    replica = await _get_replica(db, replica_uid)
    assert replica.state == ReplicaState.pending.value
    assert replica.pilot_job_name is None
    assert replica.claimed_gpu_ids == []
    job = await _get_job(db, "job-selected")
    assert job.claimed_gpu_ids == []


# ---------------------------------------------------------------------------
# reconcile: multi-node replicas
# ---------------------------------------------------------------------------


async def test_multinode_requires_dedicated_empty_job(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess, "dep", num_nodes=2, gpus_per_node=4)
        # A single-node job cannot host a multi-node replica.
        await _insert_pilot_job(sess, "job-single", num_nodes=1, gpus_per_node=4)
        uid = await _insert_replica(sess, "dep")

    ctrl = _make_controller(db)
    await ctrl.reconcile(uid)

    replica = await _get_replica(db, uid)
    assert replica.state == ReplicaState.placed.value
    new_job = await _get_job(db, replica.pilot_job_name or "")
    assert new_job.num_nodes == 2
    assert set(new_job.claimed_gpu_ids) == {
        (0, 0),
        (0, 1),
        (0, 2),
        (0, 3),
        (1, 0),
        (1, 1),
        (1, 2),
        (1, 3),
    }


async def test_multinode_skips_partially_used_job(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess, "dep", num_nodes=2, gpus_per_node=4)
        # Right shape but not empty -> not eligible for a multi-node replica.
        await _insert_pilot_job(
            sess,
            "job-used",
            num_nodes=2,
            gpus_per_node=4,
            claimed_gpu_ids=[(0, 0)],
        )
        uid = await _insert_replica(sess, "dep")

    ctrl = _make_controller(db)
    await ctrl.reconcile(uid)

    replica = await _get_replica(db, uid)
    assert replica.pilot_job_name != "job-used"
    assert await _count_jobs(db) == 2


# ---------------------------------------------------------------------------
# reconcile: new job creation + capacity limits
# ---------------------------------------------------------------------------


async def test_creates_new_job_when_none_fit(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess, "dep", num_nodes=1, gpus_per_node=2)
        uid = await _insert_replica(sess, "dep")

    ctrl = _make_controller(db)
    await ctrl.reconcile(uid)

    assert await _count_jobs(db) == 1
    replica = await _get_replica(db, uid)
    assert replica.state == ReplicaState.placed.value
    new_job = await _get_job(db, replica.pilot_job_name or "")
    # gpus_per_node from cluster PilotConfig, not the launch spec.
    assert new_job.gpus_per_node == 4
    assert new_job.num_nodes == 1
    assert set(replica.claimed_gpu_ids) == {(0, 0), (0, 1)}


async def test_at_capacity_by_job_count(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)  # max_concurrent_jobs = 3
        await _insert_deployment(sess, "dep", num_nodes=1, gpus_per_node=3)
        # Three full single-node jobs: no room to place, no room to add.
        for i in range(3):
            await _insert_pilot_job(
                sess,
                f"job-{i}",
                num_nodes=1,
                gpus_per_node=4,
                claimed_gpu_ids=[(0, 0), (0, 1), (0, 2), (0, 3)],
            )
        uid = await _insert_replica(sess, "dep")

    ctrl = _make_controller(db)
    await ctrl.reconcile(uid)

    replica = await _get_replica(db, uid)
    assert replica.state == ReplicaState.pending.value
    assert replica.state_message == AT_CAPACITY
    assert await _count_jobs(db) == 3


async def test_at_capacity_by_node_count(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)  # max_num_nodes = 8
        await _insert_deployment(sess, "dep", num_nodes=2, gpus_per_node=4)
        # 7 nodes in flight across 2 jobs; a 2-node replica would make 9 > 8.
        await _insert_pilot_job(
            sess, "job-a", num_nodes=4, gpus_per_node=4, claimed_gpu_ids=[(0, 0)]
        )
        await _insert_pilot_job(
            sess, "job-b", num_nodes=3, gpus_per_node=4, claimed_gpu_ids=[(0, 0)]
        )
        uid = await _insert_replica(sess, "dep")

    ctrl = _make_controller(db)
    await ctrl.reconcile(uid)

    replica = await _get_replica(db, uid)
    assert replica.state == ReplicaState.pending.value
    assert replica.state_message == AT_CAPACITY
    assert await _count_jobs(db) == 2


# ---------------------------------------------------------------------------
# reconcile: guards / no-ops
# ---------------------------------------------------------------------------


async def test_reconcile_missing_replica_is_noop(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)

    ctrl = _make_controller(db)
    await ctrl.reconcile(999_999)


async def test_reconcile_skips_already_placed(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess, "dep", num_nodes=1, gpus_per_node=2)
        uid = await _insert_replica(sess, "dep", state=ReplicaState.placed)

    ctrl = _make_controller(db)
    await ctrl.reconcile(uid)

    assert await _count_jobs(db) == 0
    replica = await _get_replica(db, uid)
    assert replica.pilot_job_name is None


async def test_reconcile_skips_draining_replica(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess, "dep", num_nodes=1, gpus_per_node=2)
        uid = await _insert_replica(
            sess, "dep", state=ReplicaState.pending, scheduled_deletion_at=NOW
        )

    ctrl = _make_controller(db)
    await ctrl.reconcile(uid)

    assert await _count_jobs(db) == 0


async def test_two_replicas_pack_onto_same_job(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Sequential reconciles bin-pack two 2-GPU replicas onto one 4-GPU job."""
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess, "dep", num_nodes=1, gpus_per_node=2)
        await _insert_pilot_job(sess, "job-1", num_nodes=1, gpus_per_node=4)
        r1 = await _insert_replica(sess, "dep")
        r2 = await _insert_replica(sess, "dep")

    ctrl = _make_controller(db)
    await ctrl.reconcile(r1)
    await ctrl.reconcile(r2)

    rep1 = await _get_replica(db, r1)
    rep2 = await _get_replica(db, r2)
    assert rep1.pilot_job_name == "job-1"
    assert rep2.pilot_job_name == "job-1"
    assert set(rep1.claimed_gpu_ids) == {(0, 0), (0, 1)}
    assert set(rep2.claimed_gpu_ids) == {(0, 2), (0, 3)}
    # Both packed; no extra job created.
    assert await _count_jobs(db) == 1
