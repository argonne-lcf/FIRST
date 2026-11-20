import logging
from datetime import datetime, timezone

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from first_common.schema.base_scheduler import SchedulerJobState
from first_common.schema.types import ReplicaState

from ...database.models import PilotDeployment, PilotReplica
from ...database.redis.pubsub import Channel
from ..controller import Controller

logger = logging.getLogger(__name__)

# Replica states that provide, or will provide, serving capacity and therefore
# count toward desired_replicas (when not draining).
_LIVE_STATES = frozenset(
    {
        ReplicaState.pending.value,
        ReplicaState.placed.value,
        ReplicaState.launching.value,
        ReplicaState.ready.value,
        ReplicaState.unhealthy.value,
    }
)

# Replica states where the process has stopped but on-node/DB resources are
# still held and must be freed by the Drainer.
_TERMINAL_REPLICA_STATES = frozenset(
    {
        ReplicaState.error.value,
        ReplicaState.start_timeout.value,
        ReplicaState.terminated.value,
    }
)

_TERMINAL_JOB_STATES = frozenset(
    {SchedulerJobState.exiting.value, SchedulerJobState.gone.value}
)

# Drain-order priority: cheapest-to-lose first. Lower rank drains first.
_DRAIN_RANK = {
    ReplicaState.pending.value: 0,
    ReplicaState.placed.value: 1,
    ReplicaState.unhealthy.value: 2,
    ReplicaState.launching.value: 3,
    ReplicaState.ready.value: 4,
}


class ReplicaReconciler(Controller):
    """
    Drives each PilotDeployment's live replica count toward desired_replicas and
    is the sole writer of PilotReplica.scheduled_deletion_at (plus the only
    inserter of new PilotReplica rows).

    All drain conditions funnel through here: excess capacity, a dying parent
    PilotJob, and terminal replicas whose resources still need freeing. The
    Placement controller and Drainer consume the rows/flags this controller
    produces.

    No replicas are added while the deployment's cluster is under maintenance;
    drains still proceed.

    Keyed on PilotDeployment rather than pilot_replica: the core decision
    (scale a deployment up or down) is per-deployment, and every replica is
    reachable from its parent in one reconcile.
    """

    resource_type = PilotDeployment
    wakeup_channels = [Channel.desired_replicas_changed]

    async def list_actionable(self, sess: AsyncSession) -> list[int]:
        stmt = sa.select(PilotDeployment.uid).where(
            sa.or_(
                PilotDeployment.reconcile_retry_at.is_(None),
                PilotDeployment.reconcile_retry_at < sa.func.now(),
            )
        )
        return list(await sess.scalars(stmt))

    async def reconcile(self, uid: int) -> None:
        async with self.client_state.db_sessionmaker() as sess:
            dep = await sess.get(
                PilotDeployment,
                uid,
                options=[
                    selectinload(PilotDeployment.replicas).selectinload(
                        PilotReplica.pilot_job
                    ),
                    selectinload(PilotDeployment.cluster),
                ],
            )
            if dep is None:
                return
            replicas = list(dep.replicas)
            desired = dep.desired_replicas
            maintenance_notice = dep.cluster.maintenance_notice

        # 1. Drain-by-predicate: terminal replicas and replicas whose parent job
        #    is going away. Only consider rows not already flagged or deleted.
        drains: dict[int, str] = {}
        for r in replicas:
            if r.scheduled_deletion or r.deleted_at is not None:
                continue
            reason = self._predicate_drain_reason(r)
            if reason is not None:
                drains[r.uid] = reason

        # 2. Count reconcile: replicas that count toward desired are live,
        #    not draining, not soft-deleted, and not already flagged above.
        counting = [
            r
            for r in replicas
            if r.deleted_at is None
            and r.scheduled_deletion_at is None
            and r.uid not in drains
            and r.state in _LIVE_STATES
        ]
        live = len(counting)

        n_new = 0
        if live < desired:
            n_new = desired - live
        elif live > desired:
            counting.sort(key=self._drain_sort_key)
            for r in counting[: live - desired]:
                drains[r.uid] = (
                    f"Scaled down: {live} live replicas exceeded "
                    f"desired_replicas={desired}."
                )

        await self._apply_drains(dep, drains)
        if n_new and maintenance_notice:
            logger.debug(
                "ReplicaReconciler: deployment %s not adding %d replica(s); "
                "cluster %s under maintenance",
                dep.name,
                n_new,
                dep.cluster_name,
            )
        elif n_new:
            await self._insert_replicas(dep, n_new)

    @staticmethod
    def _predicate_drain_reason(r: PilotReplica) -> str | None:
        """Why `r` must drain regardless of desired count, or None."""
        if r.state in _TERMINAL_REPLICA_STATES:
            return f"Freeing resources of replica that ended in state {r.state}."
        job = r.pilot_job
        if job is None:
            return None
        if job.scheduled_deletion:
            why = job.deletion_reason or "no reason recorded"
            return f"Parent PilotJob {job.name} is being torn down: {why}"
        if job.scheduler_state in _TERMINAL_JOB_STATES:
            return f"Parent PilotJob {job.name} scheduler job is {job.scheduler_state}."
        return None

    @staticmethod
    def _drain_sort_key(r: PilotReplica) -> tuple[int, datetime]:
        rank = _DRAIN_RANK.get(r.state, 99)
        # Oldest first within a rank; pending rows have no started_at.
        ts = r.started_at or r.created_at
        return (rank, ts)

    @staticmethod
    def _counts_toward_desired(replica: PilotReplica) -> bool:
        job = replica.pilot_job
        return (
            replica.deleted_at is None
            and replica.scheduled_deletion_at is None
            and replica.state in _LIVE_STATES
            and not (
                job is not None
                and (
                    job.scheduler_state in _TERMINAL_JOB_STATES
                    or job.scheduled_deletion
                )
            )
        )

    async def _apply_drains(self, dep: PilotDeployment, drains: dict[int, str]) -> None:
        """Flag replicas for deletion, recording each one's drain reason."""
        if not drains:
            return
        now = datetime.now(timezone.utc)
        flagged = 0
        # Sort target ids to keep multi-row lock ordering consistent.
        async with self.client_state.db_sessionmaker.begin() as sess:
            for uid in sorted(drains):
                result = await sess.execute(
                    sa.update(PilotReplica)
                    .where(
                        PilotReplica.uid == uid,
                        PilotReplica.scheduled_deletion_at.is_(None),
                    )
                    .values(**PilotReplica.schedule_deletion_values(drains[uid], now))
                )
                flagged += result.rowcount  # type: ignore[attr-defined]
        logger.info(
            "ReplicaReconciler: deployment %s flagged %d replica(s) for drain",
            dep.name,
            flagged,
        )
        if flagged:
            # Wake the Drainer now that freshly-flagged replicas need teardown.
            await self.client_state.redis_pubsub.publish(
                Channel.replica_drain, dep.name
            )

    async def _insert_replicas(self, dep: PilotDeployment, n_new: int) -> None:
        if n_new < 1:
            return
        async with self.client_state.db_sessionmaker.begin() as sess:
            # The initial reconcile snapshot can outlive a scale-down, exhausted
            # failure budget, or another inserter. Serialize insertion on the
            # exact deployment UID, then read capacity again under that lock.
            current = await sess.scalar(
                sa.select(PilotDeployment)
                .where(PilotDeployment.uid == dep.uid)
                .with_for_update()
                .options(
                    selectinload(PilotDeployment.replicas).selectinload(
                        PilotReplica.pilot_job
                    )
                )
            )
            if current is None or (
                current.consecutive_launch_failures
                > current.max_consecutive_launch_failures
            ):
                return
            # Preserve immediate replacement of terminal/draining replicas;
            # this is not a new no-overlap or teardown-wait policy. Equality at
            # the failure limit still permits the existing final retry.
            live = sum(self._counts_toward_desired(r) for r in current.replicas)
            n_new = current.desired_replicas - live
            if n_new < 1:
                return
            sess.add_all(PilotReplica.create(current.name) for _ in range(n_new))
        logger.info(
            "ReplicaReconciler: deployment %s created %d pending replica(s) "
            "(desired=%d)",
            current.name,
            n_new,
            current.desired_replicas,
        )
        # Wake the Placement controller now that pending replicas exist
        await self.client_state.redis_pubsub.publish(
            Channel.replica_created, current.name
        )
