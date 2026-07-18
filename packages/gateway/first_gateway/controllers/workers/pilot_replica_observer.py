import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable

import sqlalchemy as sa
from sqlalchemy.orm import selectinload

from first_common.schema.pilot import PilotJobStatus, ReplicaInfo
from first_common.schema.resources.runtime import PilotJobRuntime
from first_common.schema.types import (
    HealthCheckResult,
    PilotDeploymentState,
    ReplicaState,
)

from ...database.models import PilotDeployment, PilotJob, PilotReplica
from ...services.pilot_control import PilotControlClient
from ...settings import ClientState
from ..wakeup import WakeupDispatcher
from ..worker import Worker

logger = logging.getLogger(__name__)


@dataclass
class ReplicaStateCounts:
    state_counts: dict[ReplicaState, int] = field(
        default_factory=lambda: defaultdict(int)
    )
    launch_successes: int = 0
    launch_failures: int = 0


class DeploymentCounter:
    def __init__(self) -> None:
        self.deployments: dict[str, ReplicaStateCounts] = defaultdict(
            ReplicaStateCounts
        )

    def record(self, db_row: PilotReplica, new_info: ReplicaInfo) -> None:
        name = db_row.pilot_deployment_name
        self.deployments[name].state_counts[new_info.state] += 1

        # Only on transition, track launch successes/failures:
        if new_info.state != db_row.state:
            if new_info.state == ReplicaState.ready:
                self.deployments[name].launch_successes += 1
            elif new_info.state in (ReplicaState.error, ReplicaState.start_timeout):
                self.deployments[name].launch_failures += 1

    def items(self) -> Iterable[tuple[str, ReplicaStateCounts]]:
        return self.deployments.items()


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
    - PilotDeployment.{consecutive_launch_failures, state}

    Reaps orphan replicas reported by a pilot manager that have no matching
    PilotReplica row (or a row pointing at a different PilotJob).
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
        self.client = PilotControlClient(client_state, cn="pilot-replica-observer")

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

        deploy_counter = DeploymentCounter()

        for job in jobs:
            try:
                assert job.manager_url is not None
                status = await self.client.get_status(job.manager_url)
            except Exception:
                logger.exception(
                    "%s: failed to fetch /status from job %s at %s",
                    self.name,
                    job.name,
                    job.manager_url,
                )
                await self.record_unhealthy(job)
                continue

            await self.update_job_status(job, status)
            await self.sync_replicas(job, status.replicas, deploy_counter)
            await self.reap_orphans(job, status.replicas)

        await self.update_deployments(deploy_counter)

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

        # Update Resources in DB on startup only (default empty dict):
        resources_dict = status.resources.model_dump(mode="json")
        if not job.resources:
            async with self.client_state.db_sessionmaker.begin() as sess:
                await sess.execute(
                    sa.update(PilotJob)
                    .where(id_clause)
                    .values(resources=resources_dict)
                )

        # Update in Redis unconditionally (tracking GPU memory usage # continuously)
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
        elif not has_running and job.idle_since is None:
            async with self.client_state.db_sessionmaker.begin() as sess:
                await sess.execute(
                    sa.update(PilotJob)
                    .where(id_clause, PilotJob.idle_since.is_(None))
                    .values(idle_since=datetime.now(timezone.utc))
                )

    async def sync_replicas(
        self,
        job: PilotJob,
        remote_replicas: list[ReplicaInfo],
        deploy_counter: DeploymentCounter,
    ) -> None:
        """
        Update PilotReplica DB state and return (success_deployments, fail_counts).
        """
        for ri in remote_replicas:
            async with self.client_state.db_sessionmaker() as sess:
                db_replica = await sess.scalar(
                    sa.select(PilotReplica).where(
                        PilotReplica.name == ri.name,
                        PilotReplica.pilot_job_name == job.name,
                    )
                )

            if db_replica is None:
                continue

            deploy_counter.record(db_replica, ri)

            values: dict[str, Any] = {}

            if ri.state.value != db_replica.state:
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

            if not values:
                continue

            async with self.client_state.db_sessionmaker.begin() as sess:
                await sess.execute(
                    sa.update(PilotReplica)
                    .where(PilotReplica.uid == db_replica.uid)
                    .values(**values)
                )

    async def reap_orphans(
        self, job: PilotJob, remote_replicas: list[ReplicaInfo]
    ) -> None:
        assert job.manager_url is not None
        for ri in remote_replicas:
            # Match on BOTH name and pilot_job_name (even though name is
            # unique), because if same Replica is assigned to a different
            # PilotJob, this one is still an orphan (resource leak).
            async with self.client_state.db_sessionmaker() as sess:
                exists = await sess.scalar(
                    sa.select(
                        sa.exists().where(
                            PilotReplica.name == ri.name,
                            PilotReplica.pilot_job_name == job.name,
                        )
                    )
                )

            if exists:
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

    async def update_deployments(self, counter: DeploymentCounter) -> None:
        successful = [
            name for name, counts in counter.items() if counts.launch_successes > 0
        ]
        fail_counts = {
            name: counts.launch_failures
            for name, counts in counter.items()
            if counts.launch_successes == 0 and counts.launch_failures
        }

        async with self.client_state.db_sessionmaker.begin() as sess:
            await sess.execute(
                sa.update(PilotDeployment)
                .where(
                    PilotDeployment.name.in_(successful),
                    PilotDeployment.consecutive_launch_failures != 0,
                )
                .values(consecutive_launch_failures=0)
            )

        for name, fail_count in fail_counts.items():
            async with self.client_state.db_sessionmaker.begin() as sess:
                await sess.execute(
                    sa.update(PilotDeployment)
                    .where(PilotDeployment.name == name)
                    .values(
                        consecutive_launch_failures=(
                            PilotDeployment.consecutive_launch_failures + fail_count
                        )
                    )
                )

        async with self.client_state.db_sessionmaker.begin() as sess:
            deploys = await sess.scalars(
                sa.select(PilotDeployment).options(
                    selectinload(PilotDeployment.replicas)
                )
            )
            await sess.execute(
                sa.update(PilotDeployment),
                [
                    {"uid": dep.uid, "state": aggregate_state(dep).value}
                    for dep in deploys
                ],
            )
