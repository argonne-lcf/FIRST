import asyncio
import logging
from collections.abc import Awaitable
from datetime import datetime, timezone
from typing import TypeVar

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from first_common.schema.base_scheduler import JobStatusInfo, SchedulerJobState
from first_common.schema.types import HealthCheckResult, PilotConfig, ReplicaState

from ...database.models import Cluster, PilotDeployment, PilotJob, PilotReplica
from ...database.redis.pubsub import Channel
from ...platforms.schedulers import build_scheduler
from ...services.pilot_control import PilotControlClient
from ...services.pilot_submitter import PilotSubmitter
from ...settings import ClientState
from ..wakeup import WakeupDispatcher
from ..worker import Worker

logger = logging.getLogger(__name__)

_RPC_TIMEOUT = 60.0

_TERMINAL_STATES = frozenset({SchedulerJobState.gone.value})

_T = TypeVar("_T")


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
        self.hb = self.register_heartbeat("poll")
        # Per-cluster throttle for the "skipping (maintenance)" log line.
        self._maintenance_logged_at: dict[int, datetime] = {}

    async def run(self) -> None:
        # Re-register: supervise() clears the heartbeat list on every restart,
        # so the __init__ registration only survives the first run.
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
            return await asyncio.wait_for(awaitable, timeout=_RPC_TIMEOUT)
        except TimeoutError as e:
            # asyncio.wait_for raises a bare TimeoutError with an empty message;
            # name the RPC so record_failure() stores something actionable.
            name = getattr(awaitable, "__qualname__", None) or repr(awaitable)
            raise TimeoutError(
                f"Scheduler {name} RPC timed out after {_RPC_TIMEOUT:g}s"
            ) from e
        finally:
            self.hb.beat()

    async def _poll_all_clusters(self) -> None:
        async with self.client_state.db_sessionmaker() as sess:
            clusters = await Cluster.list(sess)

        now = datetime.now(timezone.utc)
        for cluster in clusters:
            if cluster.pilot_system is None:
                continue

            if cluster.maintenance_notice:
                self._log_maintenance_skip(cluster, now)
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

    def _log_maintenance_skip(self, cluster: Cluster, now: datetime) -> None:
        """Log that a cluster in maintenance is being skipped, at most once/min."""
        last = self._maintenance_logged_at.get(cluster.uid)
        if last is not None and (now - last).total_seconds() < 60:
            return
        self._maintenance_logged_at[cluster.uid] = now
        logger.info(
            "%s: skipping cluster %r in maintenance mode: %s",
            self.name,
            cluster.name,
            cluster.maintenance_notice,
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
                await self._rpc(submitter.adapter.terminate_job(orphan_id))
            except Exception:
                logger.exception("Failed to terminate orphan job %s", orphan_id)

        await self._discover_endpoints(submitter, cluster_name)

    async def _update_job(self, db_job: PilotJob, status: JobStatusInfo | None) -> None:
        async with self.client_state.db_sessionmaker.begin() as sess:
            current = await sess.get(PilotJob, db_job.uid)
            if current is None:
                return

            prev_state = current.scheduler_state

            if status is None:
                target_state = SchedulerJobState.gone.value
                new_started = current.time_started
            else:
                target_state = status.state.value
                new_started = status.started_at

            if prev_state == target_state and current.time_started == new_started:
                return

            charge = (
                target_state in _TERMINAL_STATES
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
                logger.warning(
                    f"PilotJob {current.name}: {current.scheduler_job_id!r} is no longer "
                    "in the scheduler. Assuming gone; marking terminated."
                )
            else:
                logger.info(f"PilotJob {current.name}: {prev_state} -> {target_state}")

            if charge:
                await self._record_pre_manager_launch_failure(sess, db_job)

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
                await self.client.get_status(addr.control_url)
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
