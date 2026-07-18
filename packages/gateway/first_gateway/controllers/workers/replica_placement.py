import logging
from datetime import timedelta

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from first_common.schema.base_scheduler import SchedulerJobState
from first_common.schema.types import PilotConfig, ReplicaState

from ...database.models import Cluster, PilotDeployment, PilotJob, PilotReplica
from ...database.redis.pubsub import Channel
from ..controller import Controller, StaleReconcile

logger = logging.getLogger(__name__)

# Effective-submit-time bonus per GPU-per-node: a larger replica is treated as
# if it were submitted this much earlier, so bin-packing favors it without
# starving small replicas forever
BETA = timedelta(minutes=5)

# Jobs in these states can host a replica (in-flight, not tearing down).
_PLACEABLE_JOB_STATES = [
    SchedulerJobState.pending_submit.value,
    SchedulerJobState.queued.value,
    SchedulerJobState.starting.value,
    SchedulerJobState.running.value,
]

AT_CAPACITY = "Waiting for free cluster capacity to place this replica."


def _free_gpus(job: PilotJob) -> set[tuple[int, int]]:
    """The (node, gpu) coordinates on ``job`` not currently claimed."""
    inventory = {
        (node, gpu) for node in range(job.num_nodes) for gpu in range(job.gpus_per_node)
    }
    return inventory - set(job.claimed_gpu_ids)


class ReplicaPlacer(Controller):
    """
    Schedules pending PilotReplicas onto PilotJobs, creating new PilotJobs to
    meet demand up to the cluster's capacity limits.

    Pure Postgres state management: no RPC or scheduler interaction.
    """

    resource_type = PilotReplica
    wakeup_channels = [Channel.replica_created]

    async def list_actionable(self, sess: AsyncSession) -> list[int]:
        stmt = (
            sa.select(
                PilotReplica.uid,
                PilotReplica.created_at,
                PilotDeployment.launch_spec,
            )
            .join(
                PilotDeployment,
                PilotReplica.pilot_deployment_name == PilotDeployment.name,
            )
            .where(
                PilotReplica.state == ReplicaState.pending.value,
                PilotReplica.scheduled_deletion_at.is_(None),
                sa.or_(
                    PilotReplica.reconcile_retry_at.is_(None),
                    PilotReplica.reconcile_retry_at < sa.func.now(),
                ),
            )
        )
        rows = (await sess.execute(stmt)).all()
        # Reconcile in ascending effective-submit-time order so larger replicas
        # get a bin-packing head start while waiting small ones don't starve.
        ordered = sorted(
            rows,
            key=lambda r: (
                r.created_at - BETA * int(r.launch_spec.get("gpus_per_node", 0))
            ),
        )
        return [r.uid for r in ordered]

    async def reconcile(self, uid: int) -> None:
        async with self.client_state.db_sessionmaker() as sess:
            replica = await sess.get(
                PilotReplica,
                uid,
                options=[selectinload(PilotReplica.pilot_deployment)],
            )

            if (
                replica is None
                or replica.state != ReplicaState.pending.value
                or replica.scheduled_deletion
                or replica.deleted_at is not None
            ):
                return

            deploy = replica.pilot_deployment
            cluster = await sess.scalar(
                sa.select(Cluster).where(Cluster.name == deploy.cluster_name)
            )

            if cluster is None or cluster.pilot_system is None:
                logger.warning(
                    "ReplicaPlacer: replica %s cluster %s missing or has no "
                    "pilot_system; cannot place",
                    replica.name,
                    deploy.cluster_name,
                )
                return

            jobs = list(
                await sess.scalars(
                    sa.select(PilotJob)
                    .where(
                        PilotJob.cluster_name == deploy.cluster_name,
                        PilotJob.scheduler_state.in_(_PLACEABLE_JOB_STATES),
                        PilotJob.scheduled_deletion_at.is_(None),
                        PilotJob.deleted_at.is_(None),
                    )
                    .order_by(PilotJob.uid)
                )
            )

        # First, prefer to place on an existing PilotJob:
        req_nodes = int(deploy.launch_spec["num_nodes"])
        req_gpus = int(deploy.launch_spec["gpus_per_node"])

        candidates = [j for j in jobs if j.num_nodes == req_nodes]

        if req_nodes == 1:
            placement = self._select_best_fit_single_node(candidates, req_gpus)
        else:
            placement = self._find_multi_node(candidates, req_nodes, req_gpus)

        if placement is not None:
            job, requested = placement
            await self._place(replica.uid, replica.name, job.uid, job.name, requested)
            return

        # If no existing job fits, add one, if the cluster has headroom.
        pilot_cfg = PilotConfig.model_validate(cluster.pilot_system)
        if self._has_headroom(jobs, pilot_cfg, req_nodes):
            await self._create_job_and_place(
                replica.uid,
                replica.name,
                deploy.cluster_name,
                pilot_cfg,
                req_nodes,
                req_gpus,
            )
        else:
            await self._mark_at_capacity(replica.uid)

    @staticmethod
    def _select_best_fit_single_node(
        jobs: list[PilotJob], required_gpus: int
    ) -> tuple[PilotJob, set[tuple[int, int]]] | None:
        best_key: tuple[int, int] | None = None
        best: tuple[PilotJob, set[tuple[int, int]]] | None = None

        for job in jobs:
            assert job.num_nodes == 1

            free_gpus = sorted(gpu for (_, gpu) in _free_gpus(job))
            if len(free_gpus) < required_gpus:
                continue

            key = (len(free_gpus), job.uid)

            if best_key is None or key < best_key:
                gpu_ids = {(0, gpu) for gpu in free_gpus[:required_gpus]}
                best_key, best = key, (job, gpu_ids)

        return best

    @staticmethod
    def _find_multi_node(
        jobs: list[PilotJob], required_nodes: int, required_gpus: int
    ) -> tuple[PilotJob, set[tuple[int, int]]] | None:
        for job in jobs:
            assert job.num_nodes == required_nodes

            if job.claimed_gpu_ids or job.gpus_per_node < required_gpus:
                continue

            return job, {
                (node, gpu)
                for node in range(required_nodes)
                for gpu in range(required_gpus)
            }

        return None

    @staticmethod
    def _has_headroom(
        jobs: list[PilotJob], config: PilotConfig, req_nodes: int
    ) -> bool:
        if len(jobs) + 1 > config.max_concurrent_jobs:
            return False
        if sum(j.num_nodes for j in jobs) + req_nodes > config.max_num_nodes:
            return False
        return True

    async def _place(
        self,
        replica_uid: int,
        replica_name: str,
        job_uid: int,
        job_name: str,
        requested_gpus: set[tuple[int, int]],
    ) -> None:
        async with self.client_state.db_sessionmaker.begin() as sess:
            claimed = await PilotJob.assign_replica(
                sess, job_uid, replica_uid, requested_gpus
            )
            if not claimed:
                raise StaleReconcile(
                    f"ReplicaPlacer: {replica_name} lost race for GPUs on {job_name}"
                )

            result = await sess.execute(
                sa.update(PilotReplica)
                .where(
                    PilotReplica.uid == replica_uid,
                    PilotReplica.state == ReplicaState.pending.value,
                    PilotReplica.scheduled_deletion_at.is_(None),
                )
                .values(
                    state=ReplicaState.placed.value,
                    state_message=f"Placed on {job_name}.",
                )
            )
            if result.rowcount == 0:  # type: ignore[attr-defined]
                # Replica stopped being pending under us; roll back the claim.
                raise StaleReconcile(
                    f"ReplicaPlacer: {replica_name} no longer pending at placement"
                )
        logger.info(
            "ReplicaPlacer: placed replica %s on job %s (%s)",
            replica_name,
            job_name,
            sorted(requested_gpus),
        )

    async def _create_job_and_place(
        self,
        replica_uid: int,
        replica_name: str,
        cluster_name: str,
        config: PilotConfig,
        req_nodes: int,
        req_gpus: int,
    ) -> None:
        async with self.client_state.db_sessionmaker.begin() as sess:
            job = PilotJob.create(
                cluster_name,
                walltime_min=config.job_walltime_min,
                num_nodes=req_nodes,
                gpus_per_node=config.gpus_per_node,
            )
            sess.add(job)
            await sess.flush()  # populate job.uid before assigning

            requested = {
                (node, gpu) for node in range(req_nodes) for gpu in range(req_gpus)
            }
            claimed = await PilotJob.assign_replica(
                sess, job.uid, replica_uid, requested
            )
            if not claimed:
                raise StaleReconcile(
                    f"ReplicaPlacer: {replica_name} could not claim GPUs on new job"
                )
            result = await sess.execute(
                sa.update(PilotReplica)
                .where(
                    PilotReplica.uid == replica_uid,
                    PilotReplica.state == ReplicaState.pending.value,
                    PilotReplica.scheduled_deletion_at.is_(None),
                )
                .values(
                    state=ReplicaState.placed.value,
                    state_message=f"Placed on {job.name}.",
                )
            )
            if result.rowcount == 0:  # type: ignore[attr-defined]
                raise StaleReconcile(
                    f"ReplicaPlacer: {replica_name} no longer pending at placement"
                )
            job_name = job.name
        logger.info(
            "ReplicaPlacer: created job %s and placed replica %s",
            job_name,
            replica_name,
        )

    async def _mark_at_capacity(self, replica_uid: int) -> None:
        async with self.client_state.db_sessionmaker.begin() as sess:
            await sess.execute(
                sa.update(PilotReplica)
                .where(
                    PilotReplica.uid == replica_uid,
                    PilotReplica.state == ReplicaState.pending.value,
                    PilotReplica.state_message.is_distinct_from(AT_CAPACITY),
                )
                .values(state_message=AT_CAPACITY)
            )
