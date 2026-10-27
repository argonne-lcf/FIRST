import logging
from collections.abc import Awaitable
from datetime import datetime, timezone
from typing import TypeVar

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from first_common.schema.base_scheduler import JobStatusInfo, SchedulerJobState
from first_common.schema.types import HealthCheckResult, PilotConfig

from ...database.models import Cluster, PilotJob
from ...database.redis.pubsub import Channel
from ...platforms.schedulers import (
    SCHEDULER_RPC_TIMEOUT,
    build_scheduler,
    scheduler_rpc,
)
from ...services.pilot_control import PilotControlClient
from ...services.pilot_submitter import PilotSubmitter
from ...settings import ClientState
from ..wakeup import WakeupDispatcher
from ..worker import Worker

logger = logging.getLogger(__name__)

_RPC_TIMEOUT = SCHEDULER_RPC_TIMEOUT

# Reconciler/controller cleanup begins at exiting. Account for a failed pilot
# start in that same first terminal transition.
_TERMINAL_STATES = frozenset(
    {SchedulerJobState.exiting.value, SchedulerJobState.gone.value}
)

_T = TypeVar("_T")


class PilotJobObserver(Worker):
    """
    Polls each cluster's HPC scheduler for pilot job statuses and discovers
    manager endpoints via readyfiles.

    Writes scheduler_state, time_started, and manager_url to Postgres, and
    idle_since while a running job has no manager_url.
    Reaps orphaned scheduler jobs that have no matching PilotJob row.

    Counts pilots that end before their manager comes up in
    Cluster.pilot_start_failures (with PilotJob.failure_reason), and resets the
    count when a manager comes up, unless the latch has already tripped.

    Clusters under maintenance are still polled, so jobs that survive it keep
    being tracked; poll failures there are expected and logged quietly.
    """

    poll_interval = 10.0
    failure_site_passthrough = frozenset({"_rpc"})

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
        self.client = PilotControlClient(client_state)

    async def run(self) -> None:
        self.hb = self.register_heartbeat("poll")
        while True:
            self.hb.beat()
            try:
                await self._poll_all_clusters()
            except Exception:
                logger.exception("%s: poll failed", self.name)
            await self.wait_for_wake()

    async def _rpc(self, awaitable: Awaitable[_T]) -> _T:
        """Await a scheduler RPC with a hard timeout, beating the heartbeat.

        Bounds each remote call so an unresponsive adapter can't stall the sweep
        past the heartbeat deadline, and resets the heartbeat around every call
        so a legitimately slow (but progressing) sweep is never mistaken for a
        wedged worker.
        """
        self.hb.beat()
        try:
            return await scheduler_rpc(awaitable, _RPC_TIMEOUT)
        finally:
            self.hb.beat()

    async def _poll_all_clusters(self) -> None:
        async with self.client_state.db_sessionmaker() as sess:
            clusters = await Cluster.list(sess)

        now = datetime.now(timezone.utc)
        for cluster in clusters:
            if cluster.pilot_system is None:
                continue

            # Honor the reconcile backoff written by record_failure() so a cluster
            # with an unreachable scheduler isn't polled every 10s indefinitely.
            if (
                cluster.reconcile_retry_at is not None
                and cluster.reconcile_retry_at > now
            ):
                continue

            pilot_config = PilotConfig.model_validate(cluster.pilot_system)
            adapter = await build_scheduler(pilot_config, self.client_state)

            submitter = PilotSubmitter(pilot_config, adapter)
            try:
                await self._poll_cluster(
                    submitter,
                    cluster.name,
                    in_maintenance=bool(cluster.maintenance_notice),
                )
            except Exception as e:
                if cluster.maintenance_notice:
                    # Expected while the cluster is down for maintenance.
                    logger.info(
                        "%s: poll failed for cluster %s under maintenance (%s): %r",
                        self.name,
                        cluster.name,
                        cluster.maintenance_notice,
                        e,
                    )
                else:
                    logger.exception(
                        "%s: poll failed for cluster %s", self.name, cluster.name
                    )
                async with self.client_state.db_sessionmaker.begin() as sess:
                    await Cluster.record_failure(
                        sess, cluster.uid, e, self.failure_site(e)
                    )
                    await self._mark_cluster_health(
                        sess,
                        cluster,
                        HealthCheckResult.unhealthy,
                        f"Scheduler poll failed: {str(e) or type(e).__name__}",
                    )
            else:
                if (
                    cluster.health != HealthCheckResult.healthy
                    or cluster.reconcile_failures
                ):
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
        *,
        in_maintenance: bool = False,
    ) -> None:
        async with self.client_state.db_sessionmaker() as sess:
            db_jobs = (
                await sess.scalars(
                    sa.select(PilotJob).where(
                        PilotJob.cluster_name == cluster_name,
                        PilotJob.scheduler_job_id.is_not(None),
                        PilotJob.scheduler_state.not_in([SchedulerJobState.gone.value]),
                        PilotJob.deleted_at.is_(None),
                    )
                )
            ).all()

        statuses = {s.id: s for s in await self._rpc(submitter.get_statuses())}

        for job in db_jobs:
            assert job.scheduler_job_id is not None
            status = statuses.get(job.scheduler_job_id)
            if status is None:
                # Bulk scheduler listings can be truncated or active-only.
                logger.info(
                    f"{job.scheduler_job_id} is missing in job status list; querying by exact job ID"
                )
                status = await self._rpc(
                    submitter.adapter.get_exact_job_status(job.scheduler_job_id)
                )
            await self._update_job(job, status, in_maintenance=in_maintenance)

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
                await self._rpc(submitter.adapter.terminate_job(orphan_id))
            except Exception:
                logger.exception("Failed to terminate orphan job %s", orphan_id)

        await self._discover_endpoints(submitter, cluster_name)

    async def _update_job(
        self,
        db_job: PilotJob,
        status: JobStatusInfo | None,
        *,
        in_maintenance: bool = False,
    ) -> None:
        """Record the scheduler's view of a job.

        `in_maintenance`: a job lost from the scheduler is expected, so logged
        quietly and not counted as a pilot start failure.
        """
        async with self.client_state.db_sessionmaker.begin() as sess:
            # Serialize competing observations and intentional job retirement;
            # the first terminal transition and its accounting commit together.
            current = await sess.get(PilotJob, db_job.uid, with_for_update=True)
            if current is None:
                return

            prev_state = current.scheduler_state

            if status is None:
                target_state = SchedulerJobState.gone.value
                new_started = current.time_started
            else:
                target_state = status.state.value
                new_started = status.started_at

            # FIRST retires an allocation at exiting; a delayed scheduler
            # response must not resurrect it or count it again.
            # Nonterminal PBS transitions (including requeue) remain allowed.
            if (
                prev_state in _TERMINAL_STATES and target_state not in _TERMINAL_STATES
            ) or (
                prev_state == SchedulerJobState.gone.value
                and target_state != SchedulerJobState.gone.value
            ):
                return

            if prev_state == target_state and current.time_started == new_started:
                return

            # A job lost during maintenance says nothing about pilot health.
            start_failure = (
                not in_maintenance
                and target_state in _TERMINAL_STATES
                and prev_state not in _TERMINAL_STATES
                and current.manager_url is None
                and current.scheduled_deletion_at is None
            )

            current.scheduler_state = target_state
            current.time_started = new_started
            if status is None:
                now = datetime.now(timezone.utc)
                current.scheduled_deletion_at = now
                current.deleted_at = now
                logger.log(
                    logging.INFO if in_maintenance else logging.WARNING,
                    f"PilotJob {current.name}: {current.scheduler_job_id!r} is no longer "
                    "in the scheduler. Assuming gone; marking terminated."
                    + ("; cluster under maintenance" if in_maintenance else ""),
                )
            else:
                logger.info(f"PilotJob {current.name}: {prev_state} -> {target_state}")

            if start_failure:
                await self._record_pilot_start_failure(sess, current, prev_state)

    @staticmethod
    async def _record_pilot_start_failure(
        sess: AsyncSession, job: PilotJob, prev_state: str
    ) -> None:
        """Count a pilot that ended before publishing its manager endpoint.

        Blames the cluster: a pilot that never came up usually points at the
        cluster or its scheduler. At `max_consecutive_pilot_start_failures`, new
        submissions on the cluster halt until an admin resets it.
        """
        job.failure_reason = (
            f"Pilot job {job.scheduler_job_id} died ({job.scheduler_state}) "
            f"before its manager came up; last scheduler state {prev_state}"
        )
        row = (
            await sess.execute(
                sa.update(Cluster)
                .where(Cluster.name == job.cluster_name)
                .values(pilot_start_failures=Cluster.pilot_start_failures + 1)
                .returning(Cluster.pilot_start_failures, Cluster.pilot_system)
            )
        ).one_or_none()
        if row is None:
            return
        failures, pilot_system = row
        threshold = (
            PilotConfig.model_validate(
                pilot_system
            ).max_consecutive_pilot_start_failures
            if pilot_system is not None
            else None
        )

        logger.warning(
            "PilotJob %s: pilot start failure %d%s on cluster %s: %s",
            job.name,
            failures,
            f"/{threshold}" if threshold is not None else "",
            job.cluster_name,
            job.failure_reason,
        )
        if failures == threshold:
            logger.warning(
                "Cluster %s: halting pilot submissions after %d consecutive pilot "
                "start failures (last: %s). Reset with `first-admin "
                "reconcile-reset Cluster.%s` or by re-applying the cluster.",
                job.cluster_name,
                failures,
                job.failure_reason,
                job.cluster_name,
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

        # A running pilot without a manager cannot host replicas, so it is idle.
        # Start the idle clock now so the controller can reap one that never
        # publishes its manager. This does not race with PilotReplicaObserver:
        # that observer writes idle_since only after manager_url is set.
        async with self.client_state.db_sessionmaker.begin() as sess:
            await sess.execute(
                sa.update(PilotJob)
                .where(
                    PilotJob.cluster_name == cluster_name,
                    PilotJob.scheduler_state == SchedulerJobState.running.value,
                    PilotJob.manager_url.is_(None),
                    PilotJob.idle_since.is_(None),
                )
                .values(idle_since=datetime.now(timezone.utc))
            )

        actionable_by_name: dict[str, int] = {name: uid for uid, name in actionable}
        ready_names = await self._rpc(submitter.list_ready_endpoints())
        ready_jobs = set(ready_names) & set(actionable_by_name)

        for job_name in sorted(ready_jobs):
            try:
                addr = await self._rpc(submitter.get_endpoint(job_name))
            except Exception:
                logger.exception("Failed to read endpoint for job %s", job_name)
                continue

            # Do not wake ReplicaLauncher until control API is live
            try:
                await self.client.get_status(
                    addr.control_url, cluster_name=cluster_name
                )
            except Exception as exc:
                logger.info(
                    "Job %s pilot manager is not ready at %s: %r",
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
                    # Clear the pre-manager idle clock in the same statement, so
                    # the controller's premised idle reap cannot match a
                    # reachable pilot.
                    .values(manager_url=addr.control_url, idle_since=None)
                )
                if result.rowcount == 0:  # type: ignore[attr-defined]
                    continue
                # A pilot came up: the failure streak is over, unless the latch
                # has tripped. Only an admin reset clears a tripped latch.
                threshold = submitter.pilot_config.max_consecutive_pilot_start_failures
                await sess.execute(
                    sa.update(Cluster)
                    .where(
                        Cluster.name == cluster_name,
                        Cluster.pilot_start_failures > 0,
                        Cluster.pilot_start_failures < threshold,
                    )
                    .values(pilot_start_failures=0)
                )

            await self.client_state.redis_pubsub.publish(
                Channel.pilot_job_ready, job_name
            )

    async def _mark_cluster_health(
        self,
        sess: AsyncSession,
        cluster: Cluster,
        health: HealthCheckResult,
        detail: str | None = None,
    ) -> None:
        await sess.execute(
            sa.update(Cluster)
            .where(
                Cluster.uid == cluster.uid,
                sa.or_(
                    Cluster.health.is_distinct_from(health.value),
                    Cluster.health_detail.is_distinct_from(detail),
                ),
            )
            .values(health=health.value, health_detail=detail)
        )
