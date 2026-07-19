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
                    )
                ],
            )
            if dep is None:
                return
            replicas = list(dep.replicas)
            desired = dep.desired_replicas

        # 1. Drain-by-predicate: terminal replicas and replicas whose parent job
        #    is going away. Only consider rows not already flagged or deleted.
        drain_uids: set[int] = set()
        for r in replicas:
            if r.scheduled_deletion or r.deleted_at is not None:
                continue
            job = r.pilot_job
            parent_dying = job is not None and (
                job.scheduler_state in _TERMINAL_JOB_STATES or job.scheduled_deletion
            )
            if r.state in _TERMINAL_REPLICA_STATES or parent_dying:
                drain_uids.add(r.uid)

        # 2. Count reconcile: replicas that count toward desired are live,
        #    not draining, not soft-deleted, and not already flagged above.
        counting = [
            r
            for r in replicas
            if r.deleted_at is None
            and r.scheduled_deletion_at is None
            and r.uid not in drain_uids
            and r.state in _LIVE_STATES
        ]
        live = len(counting)

        n_new = 0
        if live < desired:
            n_new = desired - live
        elif live > desired:
            counting.sort(key=self._drain_sort_key)
            for r in counting[: live - desired]:
                drain_uids.add(r.uid)

        await self._apply_drains(dep, drain_uids)
        if n_new:
            await self._insert_replicas(dep, n_new)

    @staticmethod
    def _drain_sort_key(r: PilotReplica) -> tuple[int, datetime]:
        rank = _DRAIN_RANK.get(r.state, 99)
        # Oldest first within a rank; pending rows have no started_at.
        ts = r.started_at or r.created_at
        return (rank, ts)

    async def _apply_drains(self, dep: PilotDeployment, drain_uids: set[int]) -> None:
        if not drain_uids:
            return
        now = datetime.now(timezone.utc)
        # Sort target ids to keep multi-row lock ordering consistent.
        async with self.client_state.db_sessionmaker.begin() as sess:
            result = await sess.execute(
                sa.update(PilotReplica)
                .where(
                    PilotReplica.uid.in_(sorted(drain_uids)),
                    PilotReplica.scheduled_deletion_at.is_(None),
                )
                .values(scheduled_deletion_at=now)
            )
        flagged = result.rowcount  # type: ignore[attr-defined]
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
            sess.add_all(PilotReplica.create(dep.name) for _ in range(n_new))
        logger.info(
            "ReplicaReconciler: deployment %s created %d pending replica(s) "
            "(desired=%d)",
            dep.name,
            n_new,
            dep.desired_replicas,
        )
        # Wake the Placement controller now that pending replicas exist
        await self.client_state.redis_pubsub.publish(Channel.replica_created, dep.name)
