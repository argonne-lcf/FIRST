import logging
from datetime import datetime, timezone

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from first_common.schema.base_scheduler import JobSubmitResult, SchedulerJobState
from first_common.schema.types import HealthCheckResult, PilotConfig

from ...database.models import Cluster, PilotJob, PilotReplica
from ...database.redis.pubsub import Channel
from ...platforms.schedulers import (
    SCHEDULER_RPC_TIMEOUT,
    build_scheduler,
    scheduler_rpc,
)
from ...services.pilot_submitter import PilotSubmitter
from ..controller import Controller, StaleReconcile

logger = logging.getLogger(__name__)

_RPC_TIMEOUT = SCHEDULER_RPC_TIMEOUT
_DYING_STATES = frozenset({SchedulerJobState.exiting.value})
_GONE_STATES = frozenset({SchedulerJobState.gone.value})
_SUBMISSION_CAPACITY_STATES = [
    SchedulerJobState.queued.value,
    SchedulerJobState.starting.value,
    SchedulerJobState.running.value,
]


class PilotJobController(Controller):
    resource_type = PilotJob
    wakeup_channels = [Channel.pilot_job_created]

    async def list_actionable(self, sess: AsyncSession) -> list[int]:
        stmt = sa.select(PilotJob.uid).where(
            sa.or_(
                PilotJob.reconcile_retry_at.is_(None),
                PilotJob.reconcile_retry_at < sa.func.now(),
            ),
            PilotJob.deleted_at.is_(None),
            sa.or_(
                PilotJob.scheduled_deletion_at.is_not(None),
                PilotJob.scheduler_state.not_in(
                    [
                        SchedulerJobState.queued.value,
                        SchedulerJobState.starting.value,
                        SchedulerJobState.running.value,
                    ]
                ),
                sa.and_(
                    PilotJob.idle_since.is_not(None),
                    self._no_live_assigned_replicas(),
                ),
                PilotJob.manager_health == HealthCheckResult.unhealthy.value,
            ),
        )
        return list(await sess.scalars(stmt))

    async def reconcile(self, uid: int) -> None:
        async with self.client_state.db_sessionmaker() as sess:
            job = await sess.get(PilotJob, uid)
            if job is None:
                return
            cluster = await self._get_pilot_cluster(sess, job.cluster_name)

        if cluster is None:
            return
        pilot_config = PilotConfig.model_validate(cluster.pilot_system)
        maintenance_notice = cluster.maintenance_notice

        if job.scheduled_deletion:
            await self._terminate_and_delete(job, cluster, pilot_config)
            return

        if job.scheduler_state in _DYING_STATES | _GONE_STATES:
            logger.info(
                "PilotJob %s in dying/gone state %s, scheduling deletion",
                job.name,
                job.scheduler_state,
            )
            await self._mark_scheduled_deletion(
                job,
                PilotJob.scheduler_state.in_(list(_DYING_STATES | _GONE_STATES)),
            )
            return

        if job.idle_since is not None:
            idle_min = (
                datetime.now(timezone.utc) - job.idle_since
            ).total_seconds() / 60
            if idle_min >= pilot_config.pilot_max_idle_time_min:
                logger.info(
                    "PilotJob %s idle %.1f min (limit %d), scheduling deletion",
                    job.name,
                    idle_min,
                    pilot_config.pilot_max_idle_time_min,
                )
                await self._mark_idle_deletion(job)
                return

        if (
            job.manager_health == HealthCheckResult.unhealthy.value
            and job.manager_unhealthy_since is not None
        ):
            unhealthy_min = (
                datetime.now(timezone.utc) - job.manager_unhealthy_since
            ).total_seconds() / 60
            if unhealthy_min >= pilot_config.pilot_max_unhealthy_time_min:
                logger.info(
                    "PilotJob %s unhealthy %.1f min (limit %d), scheduling deletion%s",
                    job.name,
                    unhealthy_min,
                    pilot_config.pilot_max_unhealthy_time_min,
                    "; cluster under maintenance" if maintenance_notice else "",
                )
                await self._mark_scheduled_deletion(
                    job,
                    PilotJob.manager_health == HealthCheckResult.unhealthy.value,
                )
                return

        if job.scheduler_state == SchedulerJobState.pending_submit.value:
            # Held jobs stay queued and submit once the cause clears.
            if maintenance_notice:
                reason = "under maintenance"
            elif cluster.scheduler_unreachable:
                reason = "scheduler unreachable"
            elif cluster.pilot_submissions_halted(pilot_config):
                reason = "pilot submissions halted"
            else:
                await self._submit(job, pilot_config)
                return
            logger.debug(
                "PilotJob %s: will not submit; cluster %s %s",
                job.name,
                job.cluster_name,
                reason,
            )

    async def _get_pilot_cluster(
        self, sess: AsyncSession, cluster_name: str
    ) -> Cluster | None:
        cluster = await sess.scalar(
            sa.select(Cluster).where(Cluster.name == cluster_name)
        )
        if cluster is None or cluster.pilot_system is None:
            logger.warning(
                "PilotJobController: cluster %s missing or has no pilot_system",
                cluster_name,
            )
            return None
        return cluster

    async def _terminate_and_delete(
        self, job: PilotJob, cluster: Cluster, pilot_config: PilotConfig
    ) -> None:
        if job.scheduler_job_id is None or job.scheduler_state in _GONE_STATES:
            await self._finalize_deletion(job)
            return

        if cluster.scheduler_unreachable:
            if (
                job.manager_health == HealthCheckResult.unhealthy.value
                and cluster.maintenance_notice
            ):
                # Nothing can reach the pilot or the scheduler, and the outage
                # is expected. If the job survives, the observer reaps it as an
                # orphan once the scheduler is back.
                logger.info(
                    "PilotJob %s: manager and scheduler unreachable during "
                    "maintenance; marking gone",
                    job.name,
                )
                await self._finalize_deletion(
                    job, PilotJob.manager_health == HealthCheckResult.unhealthy.value
                )
                return
            logger.info(
                "PilotJob %s: waiting for scheduler to become reachable to terminate",
                job.name,
            )
            return

        if job.scheduler_state in _DYING_STATES:
            logger.info(
                "PilotJob %s: waiting for scheduler job %s to become gone",
                job.name,
                job.scheduler_job_id,
            )
            return

        adapter = await build_scheduler(pilot_config, self.client_state)
        await scheduler_rpc(adapter.terminate_job(job.scheduler_job_id), _RPC_TIMEOUT)
        logger.info(
            "PilotJob %s: requested termination of scheduler job %s",
            job.name,
            job.scheduler_job_id,
        )

        # Keep observing the scheduler after qdel acknowledgement, but release
        # this job's FIRST submission slot by moving it to the exiting state.
        async with self.client_state.db_sessionmaker.begin() as sess:
            result = await sess.execute(
                sa.update(PilotJob)
                .where(
                    PilotJob.uid == job.uid,
                    PilotJob.scheduled_deletion_at.is_not(None),
                    PilotJob.deleted_at.is_(None),
                    PilotJob.scheduler_state == job.scheduler_state,
                )
                .values(scheduler_state=SchedulerJobState.exiting.value)
            )
            if result.rowcount == 0:  # type: ignore[attr-defined]
                raise StaleReconcile(
                    f"PilotJob {job.name}: scheduler state changed after qdel"
                )

    async def _finalize_deletion(
        self, job: PilotJob, premise: sa.ColumnElement[bool] | None = None
    ) -> None:
        """Mark a job scheduled for deletion gone and deleted.

        `premise` guards the update; by default, that the scheduler no longer
        has the job.
        """
        if premise is None:
            premise = (
                PilotJob.scheduler_job_id.is_(None)
                if job.scheduler_job_id is None
                else PilotJob.scheduler_state.in_(list(_GONE_STATES))
            )
        async with self.client_state.db_sessionmaker.begin() as sess:
            result = await sess.execute(
                sa.update(PilotJob)
                .where(
                    PilotJob.uid == job.uid,
                    PilotJob.scheduled_deletion_at.is_not(None),
                    PilotJob.deleted_at.is_(None),
                    premise,
                )
                .values(
                    deleted_at=datetime.now(timezone.utc),
                    scheduler_state=SchedulerJobState.gone.value,
                )
            )
            if result.rowcount == 0:  # type: ignore[attr-defined]
                raise StaleReconcile(
                    f"PilotJob {job.name}: scheduler absence changed before deletion"
                )

    async def _mark_scheduled_deletion(
        self,
        job: PilotJob,
        *extra_premises: sa.ColumnElement[bool],
    ) -> None:
        async with self.client_state.db_sessionmaker.begin() as sess:
            result = await sess.execute(
                sa.update(PilotJob)
                .where(
                    PilotJob.uid == job.uid,
                    PilotJob.scheduled_deletion_at.is_(None),
                    *extra_premises,
                )
                .values(scheduled_deletion_at=datetime.now(timezone.utc))
            )
            if result.rowcount == 0:  # type: ignore[attr-defined]
                raise StaleReconcile(
                    f"PilotJob {job.name}: mark_scheduled_deletion stale"
                )

    @staticmethod
    def _no_live_assigned_replicas() -> sa.ColumnElement[bool]:
        """Return a correlated guard against deleting an assigned pilot."""
        return ~sa.exists().where(
            PilotReplica.pilot_job_name == PilotJob.name,
            PilotReplica.deleted_at.is_(None),
        )

    async def _mark_idle_deletion(self, job: PilotJob) -> None:
        # Placement and idle deletion run in separate controllers. Keep the
        # assignment check in the UPDATE so a detached idle read cannot mark a
        # pilot after a replica has been assigned to it.
        assert job.idle_since is not None
        await self._mark_scheduled_deletion(
            job,
            PilotJob.idle_since == job.idle_since,
            self._no_live_assigned_replicas(),
        )

    async def _submit(self, job: PilotJob, pilot_config: PilotConfig) -> None:
        async with self.client_state.db_sessionmaker() as sess:
            active_jobs = await sess.scalar(
                sa.select(sa.func.count())
                .select_from(PilotJob)
                .where(
                    PilotJob.cluster_name == job.cluster_name,
                    PilotJob.scheduler_state.in_(_SUBMISSION_CAPACITY_STATES),
                    PilotJob.deleted_at.is_(None),
                )
            )
            assert active_jobs is not None
            if active_jobs + 1 > pilot_config.max_concurrent_jobs:
                logger.info(
                    "PilotJob %s: will not submit; would exceed cluster %s max_concurrent_jobs (%d/%d)",
                    job.name,
                    job.cluster_name,
                    active_jobs,
                    pilot_config.max_concurrent_jobs,
                )
                return

            active_nodes = await sess.scalar(
                sa.select(sa.func.coalesce(sa.func.sum(PilotJob.num_nodes), 0)).where(
                    PilotJob.cluster_name == job.cluster_name,
                    PilotJob.scheduler_state.in_(_SUBMISSION_CAPACITY_STATES),
                    PilotJob.deleted_at.is_(None),
                )
            )
            assert active_nodes is not None
            if active_nodes + job.num_nodes > pilot_config.max_num_nodes:
                logger.info(
                    "PilotJob %s: will not submit; would exceed cluster %s max_num_nodes (%d/%d)",
                    job.name,
                    job.cluster_name,
                    active_nodes,
                    pilot_config.max_num_nodes,
                )
                return

        adapter = await build_scheduler(pilot_config, self.client_state)
        submitter = PilotSubmitter(pilot_config, adapter)

        # Check if already queued to prevent duplicates on partial failure
        statuses = await scheduler_rpc(submitter.get_statuses(), _RPC_TIMEOUT)
        existing_job = next((j for j in statuses if j.name == job.name), None)

        if existing_job:
            logger.warning(
                f"Job with name {job.name!r} already exists in scheduler. "
                f"Linking scheduler_id={existing_job.id} instead of re-submitting."
            )
            submit_result = JobSubmitResult(job.name, scheduler_id=existing_job.id)
        else:
            submit_result = await scheduler_rpc(submitter.submit(job), _RPC_TIMEOUT)
            logger.info(
                "PilotJob %s: submitted as scheduler job %s",
                job.name,
                submit_result.scheduler_id,
            )

        async with self.client_state.db_sessionmaker.begin() as sess:
            await sess.execute(
                sa.update(PilotJob)
                .where(PilotJob.uid == job.uid)
                .values(
                    scheduler_job_id=submit_result.scheduler_id,
                    scheduler_state=SchedulerJobState.queued.value,
                )
            )
