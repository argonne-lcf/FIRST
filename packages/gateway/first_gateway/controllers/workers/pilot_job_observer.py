import logging
from datetime import datetime, timezone

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from first_common.schema.base_scheduler import JobStatusInfo, SchedulerJobState
from first_common.schema.types import HealthCheckResult, PilotConfig, ReplicaState

from ...database.models import Cluster, PilotDeployment, PilotJob, PilotReplica
from ...database.redis.pubsub import Channel
from ...platforms.schedulers import build_scheduler
from ...platforms.schedulers.graphql_pbs import GraphQLPBSAdapter
from ...services.pilot_control import PilotControlClient
from ...services.pilot_submitter import PilotSubmitter
from ...settings import ClientState
from ..wakeup import WakeupDispatcher
from ..worker import Worker

logger = logging.getLogger(__name__)

_TERMINAL_STATES = frozenset({SchedulerJobState.gone.value})


class PilotJobObserver(Worker):
    """
    Polls each cluster's HPC scheduler for pilot job statuses and discovers
    manager endpoints via readyfiles.

    Writes scheduler_state, time_started, and manager_url to Postgres.
    Reaps orphaned scheduler jobs that have no matching PilotJob row.
    """

    poll_interval = 10.0

    def __init__(
        self,
        name: str,
        client_state: ClientState,
        wakeup_dispatcher: WakeupDispatcher,
        *,
        restart_backoff: float = 1.0,
        max_backoff: float = 30.0,
        heartbeat_timeout: float = 120.0,
    ) -> None:
        super().__init__(
            name,
            client_state,
            wakeup_dispatcher,
            restart_backoff=restart_backoff,
            max_backoff=max_backoff,
            heartbeat_timeout=heartbeat_timeout,
        )
        self.client = PilotControlClient(client_state, cn="pilot-job-observer")

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
            except Exception as e:
                logger.exception(
                    "%s: poll failed for cluster %s", self.name, cluster.name
                )
                async with self.client_state.db_sessionmaker.begin() as sess:
                    await Cluster.record_failure(sess, cluster.uid, e)
                    await self._mark_cluster_health(
                        sess, cluster, HealthCheckResult.unhealthy
                    )
            else:
                if cluster.health != HealthCheckResult.healthy:
                    logger.info(
                        "%s: cluster %r poll succeeded; recovering",
                        self.name,
                        cluster.name,
                    )
                    async with self.client_state.db_sessionmaker.begin() as sess:
                        await Cluster.reset_reconcile_state(sess, cluster.uid)
                        await self._mark_cluster_health(
                            sess, cluster, HealthCheckResult.healthy
                        )

    async def _poll_cluster(
        self,
        submitter: PilotSubmitter,
        cluster_name: str,
    ) -> None:
        async with self.client_state.db_sessionmaker() as sess:
            db_jobs = (
                await sess.scalars(
                    sa.select(PilotJob).where(
                        PilotJob.cluster_name == cluster_name,
                        PilotJob.scheduler_job_id.is_not(None),
                        PilotJob.scheduler_state.not_in([SchedulerJobState.gone.value]),
                    )
                )
            ).all()

        statuses = {s.id: s for s in await submitter.get_statuses()}

        for job in db_jobs:
            assert job.scheduler_job_id is not None
            status = statuses.get(job.scheduler_job_id)
            if status is None:
                # Bulk scheduler listings can be truncated or active-only.
                logger.info(
                    f"{job.scheduler_job_id} is missing in job status list; querying by exact job ID"
                )
                status = await submitter.adapter.get_exact_job_status(
                    job.scheduler_job_id
                )
            await self._update_job(job, status)

        now = datetime.now(timezone.utc)
        queued_steady = set(
            jobid
            for jobid, status in statuses.items()
            if (now - status.created_at).total_seconds() > 180
            and status.state
            in (
                SchedulerJobState.queued,
                SchedulerJobState.starting,
                SchedulerJobState.running,
                SchedulerJobState.exiting,
            )
        )
        orphan_job_ids = queued_steady - {db_job.scheduler_job_id for db_job in db_jobs}

        for orphan_id in orphan_job_ids:
            logger.warning("Reaping orphan scheduler job id=%s", orphan_id)
            try:
                await submitter.adapter.terminate_job(orphan_id)
            except Exception:
                logger.exception("Failed to terminate orphan job %s", orphan_id)

        await self._discover_endpoints(submitter, cluster_name)

    async def _update_job(self, db_job: PilotJob, status: JobStatusInfo | None) -> None:
        if status is not None and (
            status.state.value == db_job.scheduler_state
            and status.started_at == db_job.time_started
        ):
            return

        target_state = (
            SchedulerJobState.gone.value if status is None else status.state.value
        )
        now = datetime.now(timezone.utc)

        # Lock the row so a terminal transition and its launch-failure accounting
        # are one atomic, exactly-once operation.  Multiple gateway observers (or
        # an exiting -> gone transition) must not charge the same failed pilot
        # allocation more than once.
        async with self.client_state.db_sessionmaker.begin() as sess:
            job = await sess.scalar(
                sa.select(PilotJob)
                .where(PilotJob.uid == db_job.uid)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
            if job is None:
                return

            previous_state = job.scheduler_state
            pre_manager_failure = (
                target_state in _TERMINAL_STATES
                and previous_state not in _TERMINAL_STATES
                and job.manager_url is None
                and job.scheduled_deletion_at is None
            )

            job.scheduler_state = target_state
            if status is None:
                job.scheduled_deletion_at = now
                job.deleted_at = now
            else:
                job.time_started = status.started_at

            if pre_manager_failure:
                await self._record_pre_manager_launch_failure(sess, job)

        if status is None:
            logger.warning(
                f"PilotJob {db_job.name}: {db_job.scheduler_job_id!r} is no longer in the scheduler. "
                "Assuming gone; marking terminated."
            )
        elif (
            target_state != db_job.scheduler_state
            or status.started_at != db_job.time_started
        ):
            logger.info(
                f"PilotJob {db_job.name}: {db_job.scheduler_state} -> {target_state}"
            )

    @staticmethod
    async def _record_pre_manager_launch_failure(
        sess: AsyncSession, job: PilotJob
    ) -> None:
        """Charge each actively assigned deployment once for a dead pilot.

        A PilotJob that reaches a scheduler terminal state before publishing its
        manager endpoint never had an opportunity to launch its placed replicas.
        Count the failed allocation against each distinct affected deployment so
        ``max_consecutive_launch_failures`` bounds automatic replacements.  Rows
        already draining are excluded: their allocation is being intentionally
        retired, rather than replaced after a launch failure.
        """
        affected_deployments = (
            sa.select(PilotReplica.pilot_deployment_name)
            .where(
                PilotReplica.pilot_job_name == job.name,
                PilotReplica.state == ReplicaState.placed.value,
                PilotReplica.scheduled_deletion_at.is_(None),
                PilotReplica.deleted_at.is_(None),
            )
            .distinct()
        )
        result = await sess.execute(
            sa.update(PilotDeployment)
            .where(PilotDeployment.name.in_(affected_deployments))
            .values(
                consecutive_launch_failures=(
                    PilotDeployment.consecutive_launch_failures + 1
                )
            )
        )
        affected = result.rowcount  # type: ignore[attr-defined]
        if affected:
            logger.warning(
                "PilotJob %s terminated before manager readiness; charged one "
                "launch failure to %d assigned deployment(s)",
                job.name,
                affected,
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
            if isinstance(submitter.adapter, GraphQLPBSAdapter):
                # GraphQL exposes the allocated head-node IP as soon as PBS says
                # the job is running.  It is only a candidate address: the pilot
                # may still be validating its all-node GPU inventory (or may have
                # failed before binding).  Do not wake the ReplicaLauncher until
                # the mTLS control API has answered at least one /status request.
                try:
                    await self.client.get_status(addr.control_url)
                except Exception as exc:
                    logger.info(
                        "GraphQL candidate endpoint for job %s is not ready at %s: %r",
                        job_name,
                        addr.control_url,
                        exc,
                    )
                    continue
            logger.info(
                "Discovered ready manager endpoint for job %s: %s",
                job_name,
                addr.control_url,
            )
            async with self.client_state.db_sessionmaker.begin() as sess:
                result = await sess.execute(
                    sa.update(PilotJob)
                    .where(
                        PilotJob.uid == actionable_by_name[job_name],
                        PilotJob.scheduler_state == SchedulerJobState.running.value,
                        PilotJob.manager_url.is_(None),
                    )
                    .values(manager_url=addr.control_url)
                )

            if result.rowcount == 0:  # type: ignore[attr-defined]
                continue

            await self.client_state.redis_pubsub.publish(
                Channel.pilot_job_ready, job_name
            )

    async def _mark_cluster_health(
        self, sess: AsyncSession, cluster: Cluster, health: HealthCheckResult
    ) -> None:
        await sess.execute(
            sa.update(Cluster)
            .where(
                Cluster.uid == cluster.uid,
                Cluster.health.is_distinct_from(health.value),
            )
            .values(health=health.value)
        )
