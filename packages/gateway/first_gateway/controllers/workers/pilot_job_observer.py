import logging
from datetime import datetime, timezone

import sqlalchemy as sa

from first_common.schema.base_scheduler import JobStatusInfo, SchedulerJobState
from first_common.schema.types import PilotConfig

from ...database.models import Cluster, PilotJob
from ...platforms.schedulers import build_scheduler
from ...services.pilot_submitter import PilotSubmitter
from ..worker import Worker

logger = logging.getLogger(__name__)


class PilotJobObserver(Worker):
    """
    Polls each cluster's HPC scheduler for pilot job statuses and discovers
    manager endpoints via readyfiles.

    Writes scheduler_state, time_started, and manager_url to Postgres.
    Reaps orphaned scheduler jobs that have no matching PilotJob row.
    """

    poll_interval = 10.0

    async def run(self) -> None:
        hb = self.register_heartbeat("poll")
        while True:
            hb.beat()
            try:
                await self._poll_all_clusters()
            except Exception:
                logger.exception("%s: poll failed", self.name)
            await self.wait_for_wake()

    async def _poll_all_clusters(self) -> None:
        async with self.client_state.db_sessionmaker() as sess:
            clusters = await Cluster.list(sess)

        for cluster in clusters:
            if cluster.pilot_system is None:
                continue
            pilot_config = PilotConfig.model_validate(cluster.pilot_system)
            adapter = await build_scheduler(pilot_config, self.client_state)

            settings = self.client_state.settings
            submitter = PilotSubmitter(
                pilot_config,
                adapter,
                settings.pilot_ca_crt,
                settings.pilot_ca_key.get_secret_value(),
            )
            try:
                await self._poll_cluster(submitter, cluster.name)
            except Exception:
                logger.exception(
                    "%s: poll failed for cluster %s", self.name, cluster.name
                )

    async def _poll_cluster(
        self,
        submitter: PilotSubmitter,
        cluster_name: str,
    ) -> None:
        async with self.client_state.db_sessionmaker() as sess:
            db_jobs = await sess.scalars(
                sa.select(PilotJob).where(
                    PilotJob.cluster_name == cluster_name,
                    PilotJob.scheduler_job_id.is_not(None),
                    PilotJob.scheduler_state.not_in([SchedulerJobState.gone.value]),
                )
            )

        statuses = {s.id: s for s in await submitter.get_statuses()}

        for job in db_jobs:
            assert job.scheduler_job_id is not None
            status = statuses.get(job.scheduler_job_id)
            await self._update_job(job, status)

        orphan_job_ids = set(statuses) - set(
            db_job.scheduler_job_id for db_job in db_jobs
        )
        for orphan_id in orphan_job_ids:
            logger.warning("Reaping orphan scheduler job id=%s", orphan_id)
            try:
                await submitter.adapter.terminate_job(orphan_id)
            except Exception:
                logger.exception("Failed to terminate orphan job %s", orphan_id)

        await self._discover_endpoints(submitter, cluster_name)

    async def _update_job(self, db_job: PilotJob, status: JobStatusInfo | None) -> None:
        if status is None:
            async with self.client_state.db_sessionmaker.begin() as sess:
                await sess.execute(
                    sa.update(PilotJob)
                    .where(
                        PilotJob.uid == db_job.uid,
                        PilotJob.scheduler_state.is_distinct_from(
                            SchedulerJobState.gone.value
                        ),
                    )
                    .values(
                        scheduler_state=SchedulerJobState.gone.value,
                        scheduled_deletion_at=datetime.now(timezone.utc),
                        deleted_at=datetime.now(timezone.utc),
                    )
                )
        elif (
            status.state != db_job.scheduler_state
            or status.started_at != db_job.time_started
        ):
            async with self.client_state.db_sessionmaker.begin() as sess:
                await sess.execute(
                    sa.update(PilotJob)
                    .where(
                        PilotJob.uid == db_job.uid,
                        sa.or_(
                            PilotJob.scheduler_state.is_distinct_from(
                                status.state.value
                            ),
                            PilotJob.time_started.is_distinct_from(status.started_at),
                        ),
                    )
                    .values(
                        scheduler_state=status.state.value,
                        time_started=status.started_at,
                    )
                )

    async def _discover_endpoints(
        self, submitter: PilotSubmitter, cluster_name: str
    ) -> None:
        async with self.client_state.db_sessionmaker() as sess:
            actionable = (
                await sess.execute(
                    sa.select(PilotJob.uid, PilotJob.name).where(
                        PilotJob.cluster_name == cluster_name,
                        PilotJob.scheduler_state == SchedulerJobState.running.value,
                        PilotJob.manager_url.is_(None),
                    )
                )
            ).all()

        if not actionable:
            return

        actionable_by_name: dict[str, int] = {name: uid for uid, name in actionable}
        ready_names = await submitter.list_ready_endpoints()
        ready_jobs = set(ready_names) & set(actionable_by_name)

        for job_name in sorted(ready_jobs):
            try:
                addr = await submitter.get_endpoint(job_name)
            except Exception:
                logger.exception("Failed to read endpoint for job %s", job_name)
                continue
            logger.info(
                "Discovered manager endpoint for job %s: %s",
                job_name,
                addr.control_url,
            )
            async with self.client_state.db_sessionmaker.begin() as sess:
                await sess.execute(
                    sa.update(PilotJob)
                    .where(
                        PilotJob.uid == actionable_by_name[job_name],
                        PilotJob.manager_url.is_(None),
                    )
                    .values(manager_url=addr.control_url)
                )
