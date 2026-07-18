"""Tests for PilotJob.assign_replica / unassign_replica GPU bookkeeping.

These cover the read-modify-write path that the ReplicaPlacer and Drainer rely
on, including two subtleties that are easy to regress:

- claimed_gpu_ids round-trips through JSONB as hashable tuples (not lists), so
  the set arithmetic in (un)assign works after a DB reload.
- The SELECT ... FOR UPDATE reads fresh row state even when the Session already
  cached the row, preventing lost updates / double GPU assignment.
"""

import itertools

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

_replica_counter = itertools.count()

from first_common.schema.base_scheduler import SchedulerJobState
from first_common.schema.types import ReplicaState
from first_gateway.database.models import (
    AccessGroup,
    Cluster,
    Model,
    PilotDeployment,
    PilotJob,
    PilotReplica,
)


async def _seed(sess: AsyncSession) -> None:
    sess.add(Cluster(name="polaris", health_check={}, pilot_system=None))
    sess.add(AccessGroup(name="ag", allowed_groups=[], allowed_domains=[]))
    await sess.flush()
    sess.add(Model(name="llama", access_group_name="ag", supported_endpoints=["chat"]))
    await sess.flush()
    sess.add(
        PilotDeployment(
            name="dep",
            cluster_name="polaris",
            model_name="llama",
            router_params={},
            prometheus_scrape_interval_sec=30,
            min_replicas=0,
            max_replicas=10,
            launch_spec={"num_nodes": 1, "gpus_per_node": 2},
            desired_replicas=0,
        )
    )
    await sess.flush()


async def _job(sess: AsyncSession, name: str = "polaris/job/1") -> int:
    j = PilotJob(
        name=name,
        cluster_name="polaris",
        scheduler_state=SchedulerJobState.running.value,
        walltime_min=60,
        num_nodes=1,
        gpus_per_node=4,
        claimed_gpu_ids=[],
    )
    sess.add(j)
    await sess.flush()
    return j.uid


async def _replica(sess: AsyncSession) -> int:
    r = PilotReplica(
        name=f"dep/replica/{next(_replica_counter)}",
        pilot_deployment_name="dep",
        state=ReplicaState.pending.value,
    )
    sess.add(r)
    await sess.flush()
    return r.uid


async def test_assign_replica_claims_gpus(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed(sess)
        job_uid = await _job(sess)
        rep_uid = await _replica(sess)

    async with db.begin() as sess:
        claimed = await PilotJob.assign_replica(
            sess, job_uid, rep_uid, {(0, 0), (0, 1)}
        )
        assert claimed is True

    async with db() as sess:
        job = await sess.get(PilotJob, job_uid)
        rep = await sess.get(PilotReplica, rep_uid)
    assert job is not None and rep is not None
    assert set(job.claimed_gpu_ids) == {(0, 0), (0, 1)}
    assert set(rep.claimed_gpu_ids) == {(0, 0), (0, 1)}
    assert rep.pilot_job_name == job.name
    # Reloaded elements are hashable tuples, not lists.
    assert all(isinstance(g, tuple) for g in job.claimed_gpu_ids)


async def test_assign_replica_rejects_overlap(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed(sess)
        job_uid = await _job(sess)
        r1 = await _replica(sess)
        r2 = await _replica(sess)

    async with db.begin() as sess:
        assert await PilotJob.assign_replica(sess, job_uid, r1, {(0, 0), (0, 1)})

    async with db.begin() as sess:
        # Overlapping request must be refused, not silently co-assigned.
        assert not await PilotJob.assign_replica(sess, job_uid, r2, {(0, 1), (0, 2)})

    async with db() as sess:
        job = await sess.get(PilotJob, job_uid)
    assert job is not None
    assert set(job.claimed_gpu_ids) == {(0, 0), (0, 1)}


async def test_assign_replica_rejects_unknown_gpus(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed(sess)
        job_uid = await _job(sess)  # gpus_per_node=4 -> valid gpu ids 0..3
        rep_uid = await _replica(sess)

    async with db.begin() as sess:
        with pytest.raises(ValueError):
            await PilotJob.assign_replica(sess, job_uid, rep_uid, {(0, 9)})


async def test_unassign_replica_frees_gpus(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed(sess)
        job_uid = await _job(sess)
        r1 = await _replica(sess)
        r2 = await _replica(sess)

    async with db.begin() as sess:
        await PilotJob.assign_replica(sess, job_uid, r1, {(0, 0), (0, 1)})
    async with db.begin() as sess:
        await PilotJob.assign_replica(sess, job_uid, r2, {(0, 2), (0, 3)})

    async with db.begin() as sess:
        await PilotJob.unassign_replica(sess, job_uid, r1)

    async with db() as sess:
        job = await sess.get(PilotJob, job_uid)
        rep1 = await sess.get(PilotReplica, r1)
    assert job is not None and rep1 is not None
    # Only r1's GPUs freed; r2's stay claimed.
    assert set(job.claimed_gpu_ids) == {(0, 2), (0, 3)}
    assert rep1.claimed_gpu_ids == []


async def test_assign_replica_reads_fresh_under_lock(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """
    Regression: a Session that already cached a job with an empty GPU set must
    not use that stale value when assigning under lock. If another transaction
    claimed a GPU in the meantime, the FOR UPDATE re-read (populate_existing)
    must see it, so an overlapping assignment is refused rather than clobbering
    the concurrent writer's claim.
    """
    async with db.begin() as sess:
        await _seed(sess)
        job_uid = await _job(sess)
        r1 = await _replica(sess)
        r2 = await _replica(sess)

    async with db() as warm:
        # Warm the identity map with claimed_gpu_ids == [] (autobegins a txn).
        cached = await warm.get(PilotJob, job_uid)
        assert cached is not None and cached.claimed_gpu_ids == []

        # A separate transaction claims (0, 0) for r1 and commits.
        async with db.begin() as other:
            assert await PilotJob.assign_replica(other, job_uid, r1, {(0, 0)})

        # Now assign r2 through the WARM session. Without a fresh read this would
        # see the cached [] and wrongly grant (0, 0) again. READ COMMITTED means
        # the FOR UPDATE re-read sees the committed claim.
        granted = await PilotJob.assign_replica(warm, job_uid, r2, {(0, 0)})
        assert granted is False
        await warm.commit()

    async with db() as sess:
        job = await sess.get(PilotJob, job_uid)
    assert job is not None
    # (0, 0) claimed exactly once.
    assert list(job.claimed_gpu_ids).count((0, 0)) == 1
