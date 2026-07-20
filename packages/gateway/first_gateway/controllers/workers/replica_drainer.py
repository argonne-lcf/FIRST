import logging
from datetime import datetime, timezone

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from first_common.schema.base_scheduler import SchedulerJobState
from first_common.schema.types import ReplicaState

from ...database.models import PilotJob, PilotReplica
from ...database.redis.pubsub import Channel
from ...services.pilot_control import PilotControlClient
from ...settings import ClientState
from ..controller import Controller, StaleReconcile
from ..wakeup import WakeupDispatcher

logger = logging.getLogger(__name__)

# Terminal states we must not overwrite with `terminated`: the replica already
# stopped for a reason worth preserving in the operational history.
_PRESERVE_STATES = frozenset(
    {
        ReplicaState.error.value,
        ReplicaState.start_timeout.value,
        ReplicaState.terminated.value,
    }
)

# Give the router config controller a moment to pull the replica out of rotation
# before we stop it, so we don't 500 an in-flight request.
_MIN_DRAIN_WAIT_SEC = 20.0
# Hard cap: after this long we drain regardless of remaining in-flight work.
_MAX_DRAIN_WAIT_SEC = 300.0


class ReplicaDrainer(Controller):
    """
    Tears down replicas flagged for deletion and frees their resources.

    Consumes `scheduled_deletion_at` (written solely by the ReplicaReconciler);
    it never writes that field. For each flagged replica it stops the process on
    the pilot manager, releases the GPUs claimed on the parent PilotJob, and sets
    `deleted_at` so the retention sweeper can eventually reap the row.
    """

    resource_type = PilotReplica
    wakeup_channels = [Channel.replica_drain]

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
        self.client = PilotControlClient(client_state, cn="replica-drainer")

    async def list_actionable(self, sess: AsyncSession) -> list[int]:
        stmt = sa.select(PilotReplica.uid).where(
            PilotReplica.scheduled_deletion_at.is_not(None),
            PilotReplica.deleted_at.is_(None),
            sa.or_(
                PilotReplica.reconcile_retry_at.is_(None),
                PilotReplica.reconcile_retry_at < sa.func.now(),
            ),
        )
        return list(await sess.scalars(stmt))

    async def reconcile(self, uid: int) -> None:
        async with self.client_state.db_sessionmaker() as sess:
            replica = await sess.get(
                PilotReplica,
                uid,
                options=[
                    selectinload(PilotReplica.pilot_job),
                    selectinload(PilotReplica.pilot_deployment),
                ],
            )

        if (
            replica is None
            or replica.deleted_at is not None
            or replica.scheduled_deletion_at is None
        ):
            return

        # ready replicas serve traffic; wait for eligibility to yank them:
        if replica.state == ReplicaState.ready and not await self._ready_eligible(
            replica
        ):
            return

        # Stop the process on the pilot manager. The manager holds on-node
        # resources even for error/start_timeout replicas until told to stop:
        if (
            replica.pilot_job
            and replica.pilot_job.scheduler_state == SchedulerJobState.running
            and replica.pilot_job.manager_url
        ):
            await self._stop_on_manager(replica.pilot_job.manager_url, replica.name)

        await self._finalize_deletion(replica)

    async def _ready_eligible(self, replica: PilotReplica) -> bool:
        assert replica.scheduled_deletion_at
        elapsed = (
            datetime.now(timezone.utc) - replica.scheduled_deletion_at
        ).total_seconds()
        if elapsed < _MIN_DRAIN_WAIT_SEC:
            return False
        if elapsed >= _MAX_DRAIN_WAIT_SEC:
            return True
        runtime = await self.client_state.redis_repo.get_backend_runtime(
            replica.pilot_deployment.model_name, replica.backend_id
        )
        return runtime.inflight == 0

    async def _stop_on_manager(self, manager_url: str, replica_name: str) -> None:
        resp = await self.client.stop_replica(manager_url, replica_name)
        if resp.status_code == 404:
            # Already gone on the manager (double-delete); nothing to free there.
            logger.info(
                "ReplicaDrainer: replica %s already absent on manager (404)",
                replica_name,
            )
            return
        # Any other non-2xx (or lingering transport error from the helper) is
        # transient: raise so the reconcile cooldown/retry kicks in.
        resp.raise_for_status()
        logger.info("Stopped replica %s", replica_name)

    async def _finalize_deletion(self, replica: PilotReplica) -> None:
        now = datetime.now(timezone.utc)
        new_state = (
            replica.state
            if replica.state in _PRESERVE_STATES
            else ReplicaState.terminated.value
        )
        async with self.client_state.db_sessionmaker.begin() as sess:
            if replica.pilot_job:
                await PilotJob.unassign_replica(
                    sess, replica.pilot_job.uid, replica.uid
                )

            result = await sess.execute(
                sa.update(PilotReplica)
                .where(
                    PilotReplica.uid == replica.uid,
                    PilotReplica.deleted_at.is_(None),
                )
                .values(
                    state=new_state,
                    stopped_at=sa.func.coalesce(PilotReplica.stopped_at, now),
                    deleted_at=now,
                )
            )
            if result.rowcount == 0:  # type: ignore[attr-defined]
                raise StaleReconcile(
                    f"ReplicaDrainer: {replica.name} already deleted under us"
                )
        logger.info(
            "ReplicaDrainer: drained replica %s (state -> %s)", replica.name, new_state
        )
