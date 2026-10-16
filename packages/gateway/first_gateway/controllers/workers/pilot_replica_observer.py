import logging
from datetime import datetime, timezone
from time import monotonic
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from first_common.schema.pilot import PilotJobStatus, ReplicaInfo
from first_common.schema.resources.runtime import PilotJobRuntime
from first_common.schema.types import (
    HealthCheckResult,
    PilotDeploymentState,
    ReplicaState,
)

from ...database.models import PilotDeployment, PilotJob, PilotReplica
from ...database.redis.pubsub import Channel
from ...services.pilot_control import PilotControlClient
from ...settings import ClientState
from ..wakeup import WakeupDispatcher
from ..worker import Worker

logger = logging.getLogger(__name__)


def aggregate_state(dep: PilotDeployment) -> PilotDeploymentState:
    """
    Calculate an aggregated PilotDeployment-level state based on its Replicas'
    runtime state and the intentions of the Controller.
    """
    LIVE = {
        ReplicaState.pending,
        ReplicaState.placed,
        ReplicaState.launching,
        ReplicaState.ready,
        ReplicaState.unhealthy,
        ReplicaState.terminating,
    }
    IN_FLIGHT = {ReplicaState.pending, ReplicaState.placed, ReplicaState.launching}

    live = [r for r in dep.replicas if r.state in LIVE]
    draining = [r for r in live if r.is_draining]
    retained = [r for r in live if not r.is_draining]

    serving = [r for r in retained if r.state == ReplicaState.ready]
    incoming = [r for r in retained if r.state in IN_FLIGHT]
    unhealthy = [r for r in retained if r.state == ReplicaState.unhealthy]

    # 1. Routable capacity exists
    if serving:
        return (
            PilotDeploymentState.healthy
            if len(serving) >= dep.desired_replicas
            else PilotDeploymentState.degraded
        )

    # 2. Nothing is serving, but capacity is on the way (and we actually want it).
    if incoming and dep.desired_replicas > 0:
        return PilotDeploymentState.starting

    # 3. Nothing is coming or serving, and teardown is underway.
    if draining or (live and dep.desired_replicas == 0):
        return PilotDeploymentState.stopping

    # 4. We want capacity; nothing serving/coming/draining; fresh evidence of failure.
    if dep.desired_replicas > 0 and (unhealthy or dep.consecutive_launch_failures > 0):
        return PilotDeploymentState.failed

    # 5. Desired > 0 but we haven't created any replicas yet (maybe because the cluster is full)
    if dep.desired_replicas > 0:
        return PilotDeploymentState.awaiting_capacity

    # 6. Intentionally at zero
    return PilotDeploymentState.offline


class PilotReplicaObserver(Worker):
    """
    Polls GET /status on each running pilot manager and syncs replica state back
    to Postgres.

    Writes:
    - PilotJob.{resources, manager_health, manager_unhealthy_since, idle_since}
    - PilotReplica.{state, state_message, resources, model_url, observed_served_name, started_at}
    - PilotDeployment.{consecutive_launch_failures, last_startup_sec, state}

    Reaps orphan replicas reported by a pilot manager that have no matching
    PilotReplica row (or a row pointing at a different PilotJob).
    """

    poll_interval = 10.0

    # Per-job cooldown for managers that fail /status check
    status_backoff_base = poll_interval
    status_backoff_max = 120.0

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
        self._status_cooldown: dict[int, tuple[float, int]] = {}

    async def run(self) -> None:
        hb = self.register_heartbeat("poll")
        while True:
            hb.beat()
            try:
                await self.poll()
            except Exception:
                logger.exception("%s: poll failed", self.name)
            await self.wait_for_wake()

    async def poll(self) -> None:
        async with self.client_state.db_sessionmaker() as sess:
            jobs = list(
                await sess.scalars(
                    sa.select(PilotJob).where(
                        PilotJob.scheduler_state == "running",
                        PilotJob.manager_url.is_not(None),
                    )
                )
            )

        live_uids = {job.uid for job in jobs}
        self._status_cooldown = {
            uid: v for uid, v in self._status_cooldown.items() if uid in live_uids
        }

        now = monotonic()
        for job in jobs:
            cooldown = self._status_cooldown.get(job.uid)
            if cooldown is not None and now < cooldown[0]:
                continue

            try:
                assert job.manager_url is not None
                status = await self.client.get_status(job.manager_url)
            except Exception:
                await self._record_status_failure(job, cooldown)
                continue

            self._status_cooldown.pop(job.uid, None)
            await self.update_job_status(job, status)
            await self.sync_replicas(job, status.replicas)
            await self.reap_orphans(job, status.replicas)

        await self.update_deployments()

    async def _record_status_failure(
        self, job: PilotJob, cooldown: tuple[float, int] | None
    ) -> None:
        """Mark the job's manager unhealthy and backoff on status polling."""
        failures = (cooldown[1] if cooldown else 0) + 1
        delay = min(
            self.status_backoff_base * 2.0 ** (failures - 1),
            self.status_backoff_max,
        )

        self._status_cooldown[job.uid] = (monotonic() + delay, failures)

        logger.warning(
            "%s: failed to fetch /status from job %s at %s "
            "(failure #%d, backing off %.0fs)",
            self.name,
            job.name,
            job.manager_url,
            failures,
            delay,
            # Full traceback on the first failure only; later ones are expected.
            exc_info=(failures == 1),
        )
        await self.record_unhealthy(job)

    async def record_unhealthy(self, job: PilotJob) -> None:
        now = datetime.now(timezone.utc)
        async with self.client_state.db_sessionmaker.begin() as sess:
            await sess.execute(
                sa.update(PilotJob)
                .where(
                    PilotJob.uid == job.uid,
                    PilotJob.manager_health.is_distinct_from(
                        HealthCheckResult.unhealthy.value
                    ),
                )
                .values(
                    manager_health=HealthCheckResult.unhealthy.value,
                    manager_unhealthy_since=now,
                )
            )

        logger.info(f"Recorded PilotJob {job.name!r} manager_health=unhealthy")

    async def update_job_status(self, job: PilotJob, status: PilotJobStatus) -> None:
        id_clause = PilotJob.uid == job.uid

        # If we have manager status response, it's healthy:
        if job.manager_health != HealthCheckResult.healthy.value:
            async with self.client_state.db_sessionmaker.begin() as sess:
                await sess.execute(
                    sa.update(PilotJob)
                    .where(
                        id_clause,
                        PilotJob.manager_health.is_distinct_from(
                            HealthCheckResult.healthy.value
                        ),
                    )
                    .values(
                        manager_health=HealthCheckResult.healthy.value,
                        manager_unhealthy_since=None,
                    )
                )
                logger.info(f"PilotJob {job.name!r} manager is healthy")

        # Update Resources in DB on startup only (default empty dict):
        resources_dict = status.resources.model_dump(mode="json")
        if not job.resources:
            async with self.client_state.db_sessionmaker.begin() as sess:
                await sess.execute(
                    sa.update(PilotJob)
                    .where(id_clause)
                    .values(resources=resources_dict)
                )
                logger.info(f"Recorded PilotJob {job.name!r} resources on startup")

        # Update in Redis unconditionally (tracking GPU metrics continuously)
        await self.client_state.redis_repo.set_pilot_job_runtime(
            job.uid, PilotJobRuntime(resources=status.resources)
        )

        has_running = any(
            replica.state
            in (
                ReplicaState.placed,
                ReplicaState.launching,
                ReplicaState.ready,
                ReplicaState.unhealthy,
            )
            for replica in status.replicas
        )

        # Mark or clear idle_since timestamp
        if has_running and job.idle_since is not None:
            async with self.client_state.db_sessionmaker.begin() as sess:
                await sess.execute(
                    sa.update(PilotJob)
                    .where(id_clause, PilotJob.idle_since.is_not(None))
                    .values(idle_since=None)
                )
            logger.info(f"PilotJob {job.name!r} has running replicas; no longer idle")
        elif not has_running and job.idle_since is None:
            async with self.client_state.db_sessionmaker.begin() as sess:
                await sess.execute(
                    sa.update(PilotJob)
                    .where(id_clause, PilotJob.idle_since.is_(None))
                    .values(idle_since=datetime.now(timezone.utc))
                )
            logger.info(f"PilotJob {job.name!r} has zero replicas; marking idle")

    async def sync_replicas(
        self, job: PilotJob, remote_replicas: list[ReplicaInfo]
    ) -> None:
        """Sync observed replica state to Postgres"""
        if not remote_replicas:
            return

        async with self.client_state.db_sessionmaker() as sess:
            rows = await sess.scalars(
                sa.select(PilotReplica).where(
                    PilotReplica.pilot_job_name == job.name,
                    PilotReplica.name.in_([ri.name for ri in remote_replicas]),
                )
            )
            db_replicas: dict[str, PilotReplica] = {row.name: row for row in rows}

        any_started = False
        for ri in remote_replicas:
            db_replica = db_replicas.get(ri.name)
            if db_replica is None:
                continue
            try:
                applied = await self._update_replica(db_replica, ri)
            except Exception:
                logger.exception(
                    "%s: failed to sync replica %s on job %s",
                    self.name,
                    ri.name,
                    job.name,
                )
                continue
            if applied and ri.state == ReplicaState.ready:
                any_started = True

        if any_started:
            await self.client_state.redis_pubsub.publish(Channel.replica_started, "")

    async def _update_replica(self, db_replica: PilotReplica, ri: ReplicaInfo) -> bool:
        """
        Write observed changes for one replica. A state transition and its
        effect on the parent deployment commit together, and only if the
        replica was still in the state we read. Returns True if a transition
        was committed.
        """
        values: dict[str, Any] = {}
        transition = ri.state.value != db_replica.state

        if transition:
            values["state"] = ri.state.value
            if ri.state in (
                ReplicaState.error,
                ReplicaState.start_timeout,
                ReplicaState.terminated,
            ):
                values["stopped_at"] = datetime.now(timezone.utc)

        if ri.state_message != db_replica.state_message:
            values["state_message"] = ri.state_message

        if ri.url != db_replica.model_url:
            values["model_url"] = ri.url

        if ri.served_model_name != db_replica.observed_served_name:
            values["observed_served_name"] = ri.served_model_name

        if ri.started_at != db_replica.started_at:
            values["started_at"] = ri.started_at

        if ri.resources and not db_replica.resources:
            values["resources"] = [r.model_dump(mode="json") for r in ri.resources]

        if ri.log_path and not db_replica.log_path:
            values["log_path"] = ri.log_path.as_posix()

        if not values:
            return False

        async with self.client_state.db_sessionmaker.begin() as sess:
            result = await sess.execute(
                sa.update(PilotReplica)
                .where(
                    PilotReplica.uid == db_replica.uid,
                    PilotReplica.deleted_at.is_(None),
                    PilotReplica.state == db_replica.state,
                )
                .values(**values)
            )
            if result.rowcount == 0:  # type: ignore[attr-defined]
                # Replica changed or was deleted under us; re-observe next tick.
                return False
            if transition:
                await self._record_transition(sess, db_replica, ri.state)

        if transition:
            logger.info(
                f"Replica {db_replica.name!r}: {db_replica.state} -> {ri.state.value} [{ri.state_message}]"
            )
        return transition

    @staticmethod
    async def _record_transition(
        sess: AsyncSession, db_replica: PilotReplica, new_state: ReplicaState
    ) -> None:
        """Apply a replica transition's effect on its parent PilotDeployment."""
        dep_clause = PilotDeployment.name == db_replica.pilot_deployment_name

        if new_state == ReplicaState.ready:
            values: dict[str, Any] = {"consecutive_launch_failures": 0}
            # Only a clean `launching -> ready` cold start is a startup
            # measurement; `unhealthy -> ready` recovery flaps are not.
            if (
                db_replica.state == ReplicaState.launching
                and db_replica.placed_at is not None
            ):
                values["last_startup_sec"] = (
                    datetime.now(timezone.utc) - db_replica.placed_at
                ).total_seconds()
            await sess.execute(
                sa.update(PilotDeployment).where(dep_clause).values(**values)
            )
        elif new_state in (ReplicaState.error, ReplicaState.start_timeout):
            await sess.execute(
                sa.update(PilotDeployment)
                .where(dep_clause)
                .values(
                    consecutive_launch_failures=(
                        PilotDeployment.consecutive_launch_failures + 1
                    )
                )
            )

    async def reap_orphans(
        self, job: PilotJob, remote_replicas: list[ReplicaInfo]
    ) -> None:
        assert job.manager_url is not None
        if not remote_replicas:
            return

        # Match on BOTH name and pilot_job_name (even though name is
        # unique itself), because if same Replica is assigned to a different
        # PilotJob, this one is still an orphan (resource leak).
        async with self.client_state.db_sessionmaker() as sess:
            known = set(
                await sess.scalars(
                    sa.select(PilotReplica.name).where(
                        PilotReplica.pilot_job_name == job.name,
                        PilotReplica.name.in_([ri.name for ri in remote_replicas]),
                        PilotReplica.deleted_at.is_(None),
                    )
                )
            )

        for ri in remote_replicas:
            if ri.name in known:
                continue

            logger.warning("Reaping orphan replica %s on job %s", ri.name, job.name)
            try:
                resp = await self.client.stop_replica(job.manager_url, ri.name)
                resp.raise_for_status()
            except Exception:
                logger.exception(
                    "Failed to stop orphan replica %s on job %s",
                    ri.name,
                    job.name,
                )

    async def update_deployments(self) -> None:
        """Recompute each PilotDeployment's aggregate state; write only changes."""
        async with self.client_state.db_sessionmaker.begin() as sess:
            deploys = await sess.scalars(
                sa.select(PilotDeployment).options(
                    selectinload(PilotDeployment.replicas)
                )
            )
            changed = [
                {"uid": dep.uid, "state": state}
                for dep in deploys
                if (state := aggregate_state(dep).value) != dep.state
            ]
            if changed:
                await sess.execute(sa.update(PilotDeployment), changed)
