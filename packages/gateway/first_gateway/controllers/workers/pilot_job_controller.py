import asyncio
import logging
from datetime import datetime, timezone

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from first_common.schema.base_scheduler import SchedulerJobState
from first_common.schema.types import HealthCheckResult, PilotConfig

from ...database.models import Cluster, PilotJob
from ...platforms.schedulers import build_scheduler
from ...services.pilot_submitter import PilotSubmitter
from ..controller import Controller, StaleReconcile

logger = logging.getLogger(__name__)

_RPC_TIMEOUT = 60.0
_TERMINAL_STATES = frozenset(
    {SchedulerJobState.exiting.value, SchedulerJobState.gone.value}
)
_ACTIVE_STATES = [
    SchedulerJobState.pending_submit.value,
    SchedulerJobState.queued.value,
    SchedulerJobState.starting.value,
    SchedulerJobState.running.value,
]


class PilotJobController(Controller):
    resource_type = PilotJob

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
                PilotJob.idle_since.is_not(None),
                PilotJob.manager_health == HealthCheckResult.unhealthy.value,
            ),
        )
        return list(await sess.scalars(stmt))

    async def reconcile(self, uid: int) -> None:
        async with self.client_state.db_sessionmaker() as sess:
            job = await sess.get(PilotJob, uid)
            if job is None:
                return
            pilot_config = await self._get_pilot_config(sess, job.cluster_name)

        if pilot_config is None:
            return

        if job.scheduled_deletion:
            await self._terminate_and_delete(job, pilot_config)
            return

        if job.scheduler_state in _TERMINAL_STATES:
            logger.info(
                "PilotJob %s in terminal state %s, scheduling deletion",
                job.name,
                job.scheduler_state,
            )
            await self._mark_scheduled_deletion(
                job, PilotJob.scheduler_state.in_(list(_TERMINAL_STATES))
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
                await self._mark_scheduled_deletion(
                    job, PilotJob.idle_since.is_not(None)
                )
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
                    "PilotJob %s unhealthy %.1f min (limit %d), scheduling deletion",
                    job.name,
                    unhealthy_min,
                    pilot_config.pilot_max_unhealthy_time_min,
                )
                await self._mark_scheduled_deletion(
                    job,
                    PilotJob.manager_health == HealthCheckResult.unhealthy.value,
                )
                return

        if job.scheduler_state == SchedulerJobState.pending_submit.value:
            await self._submit(job, pilot_config)

    async def _get_pilot_config(
        self, sess: AsyncSession, cluster_name: str
    ) -> PilotConfig | None:
        cluster = await sess.scalar(
            sa.select(Cluster).where(Cluster.name == cluster_name)
        )
        if cluster is None or cluster.pilot_system is None:
            logger.warning(
                "PilotJobController: cluster %s missing or has no pilot_system",
                cluster_name,
            )
            return None
        return PilotConfig.model_validate(cluster.pilot_system)

    async def _terminate_and_delete(
        self, job: PilotJob, pilot_config: PilotConfig
    ) -> None:
        if (
            job.scheduler_job_id is not None
            and job.scheduler_state not in _TERMINAL_STATES
        ):
            adapter = await build_scheduler(pilot_config, self.client_state)
            await asyncio.wait_for(
                adapter.terminate_job(job.scheduler_job_id), timeout=_RPC_TIMEOUT
            )
            logger.info(
                "PilotJob %s: terminated scheduler job %s",
                job.name,
                job.scheduler_job_id,
            )

        async with self.client_state.db_sessionmaker.begin() as sess:
            result = await sess.execute(
                sa.update(PilotJob)
                .where(
                    PilotJob.uid == job.uid,
                    PilotJob.scheduled_deletion_at.is_not(None),
                    PilotJob.deleted_at.is_(None),
                )
                .values(
                    deleted_at=datetime.now(timezone.utc),
                    scheduler_state=SchedulerJobState.gone.value,
                )
            )
            if result.rowcount == 0:  # type: ignore[attr-defined]
                raise StaleReconcile(f"PilotJob {job.name}: terminate_and_delete stale")

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

    async def _submit(self, job: PilotJob, pilot_config: PilotConfig) -> None:
        async with self.client_state.db_sessionmaker() as sess:
            active_jobs = await sess.scalar(
                sa.select(sa.func.count())
                .select_from(PilotJob)
                .where(
                    PilotJob.cluster_name == job.cluster_name,
                    PilotJob.scheduler_state.in_(_ACTIVE_STATES),
                    PilotJob.deleted_at.is_(None),
                )
            )
            assert active_jobs is not None
            if active_jobs >= pilot_config.max_concurrent_jobs:
                logger.info(
                    "PilotJob %s: cluster %s at max_concurrent_jobs (%d/%d)",
                    job.name,
                    job.cluster_name,
                    active_jobs,
                    pilot_config.max_concurrent_jobs,
                )
                return

            active_nodes = await sess.scalar(
                sa.select(sa.func.coalesce(sa.func.sum(PilotJob.num_nodes), 0)).where(
                    PilotJob.cluster_name == job.cluster_name,
                    PilotJob.scheduler_state.in_(_ACTIVE_STATES),
                    PilotJob.deleted_at.is_(None),
                )
            )
            assert active_nodes is not None
            if active_nodes >= pilot_config.max_num_nodes:
                logger.info(
                    "PilotJob %s: cluster %s at max_num_nodes (%d/%d)",
                    job.name,
                    job.cluster_name,
                    active_nodes,
                    pilot_config.max_num_nodes,
                )
                return

        adapter = await build_scheduler(pilot_config, self.client_state)
        settings = self.client_state.settings
        submitter = PilotSubmitter(
            pilot_config,
            adapter,
            settings.pilot_ca_crt,
            settings.pilot_ca_key.get_secret_value(),
        )
        submit_result = await asyncio.wait_for(
            submitter.submit(job),
            timeout=_RPC_TIMEOUT,
        )
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
