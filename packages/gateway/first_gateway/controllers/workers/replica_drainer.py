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

# After exactly two controller-recorded manager-stop failures, stop retrying
# the same live allocation.  The next action schedules the parent PilotJob for
# authoritative scheduler termination and retains the replica's GPU claims
# until that exact allocation is observed gone.
_STOP_FAILURE_LIMIT = 2
_STOP_FAILURE_MARKER = "replica cleanup verification failed"
_FORCED_CLEANUP_MESSAGE = (
    "Replica cleanup verification FAILED after bounded manager stop retries; "
    "parent scheduler allocation termination requested."
)


class ReplicaCleanupUnverified(RuntimeError):
    """The manager stop RPC did not prove model and post-stop cleanup."""


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

        job = replica.pilot_job

        # Once any controller has requested scheduler deletion, allocation
        # absence is the only safe release proof.  In particular, the forced
        # cleanup path must not unassign claims while qdel is still running.
        if job and job.scheduled_deletion_at is not None:
            if (
                job.scheduler_state != SchedulerJobState.gone.value
                or job.deleted_at is None
            ):
                return
            await self._finalize_deletion(replica)
            return

        # Exiting/suspended is allocation-active but the manager is no longer a
        # safe cleanup authority. Request exact parent qdel and retain claims
        # even when the drainer wins the race with PilotJobController.
        if job and job.scheduler_state == SchedulerJobState.exiting.value:
            await self._request_parent_termination(replica, cleanup_unverified=False)
            return

        # Stop the process on the pilot manager. The manager holds on-node
        # resources even for error/start_timeout replicas until told to stop:
        if job and job.scheduler_state == SchedulerJobState.running and job.manager_url:
            if self._bounded_stop_failures(replica) >= _STOP_FAILURE_LIMIT:
                await self._request_parent_termination(replica, cleanup_unverified=True)
                return
            try:
                await self._stop_on_manager(job.manager_url, replica.name)
            except Exception as exc:
                raise ReplicaCleanupUnverified(
                    f"{_STOP_FAILURE_MARKER}: {type(exc).__name__}: {exc}"
                ) from exc

        await self._finalize_deletion(replica)

    @staticmethod
    def _bounded_stop_failures(replica: PilotReplica) -> int:
        error = replica.reconcile_last_error or ""
        if _STOP_FAILURE_MARKER not in error:
            return 0
        return replica.reconcile_failures

    async def _request_parent_termination(
        self, replica: PilotReplica, *, cleanup_unverified: bool
    ) -> None:
        """Atomically retain failure evidence and request parent allocation qdel."""
        if replica.pilot_job is None:
            raise StaleReconcile(
                f"ReplicaDrainer: {replica.name} lost parent before forced cleanup"
            )

        now = datetime.now(timezone.utc)
        async with self.client_state.db_sessionmaker.begin() as sess:
            locked_replica = await sess.scalar(
                sa.select(PilotReplica)
                .where(
                    PilotReplica.uid == replica.uid,
                    PilotReplica.deleted_at.is_(None),
                    PilotReplica.scheduled_deletion_at.is_not(None),
                    PilotReplica.pilot_job_name == replica.pilot_job.name,
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )
            if locked_replica is None:
                raise StaleReconcile(
                    f"ReplicaDrainer: {replica.name} disappeared during escalation"
                )
            locked_job = await sess.scalar(
                sa.select(PilotJob)
                .where(
                    PilotJob.uid == replica.pilot_job.uid,
                    PilotJob.name == replica.pilot_job.name,
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )
            if locked_job is None:
                raise StaleReconcile(
                    f"ReplicaDrainer: parent for {replica.name} disappeared"
                )
            if locked_replica.pilot_job_name != locked_job.name:
                raise StaleReconcile(
                    f"ReplicaDrainer: {replica.name} changed parent during escalation"
                )

            if cleanup_unverified:
                locked_replica.state = ReplicaState.error.value
                locked_replica.state_message = _FORCED_CLEANUP_MESSAGE
            if (
                locked_job.deleted_at is None
                and locked_job.scheduler_state != SchedulerJobState.gone.value
                and locked_job.scheduled_deletion_at is None
            ):
                locked_job.scheduled_deletion_at = now

        if cleanup_unverified:
            logger.error(
                "ReplicaDrainer: %s; PilotJob %s must prove scheduler absence",
                _FORCED_CLEANUP_MESSAGE,
                replica.pilot_job.name,
            )
        else:
            logger.warning(
                "ReplicaDrainer: PilotJob %s is exiting; requested scheduler "
                "termination and retained replica %s claims",
                replica.pilot_job.name,
                replica.name,
            )

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

        if replica.state in _PRESERVE_STATES:
            new_state = replica.state
            new_msg = replica.state_message
        else:
            new_state = ReplicaState.terminated.value
            new_msg = "Replica terminated by drainer."

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
                    state_message=new_msg,
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
