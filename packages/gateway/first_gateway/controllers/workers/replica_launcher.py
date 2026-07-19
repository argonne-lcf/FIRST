import logging

import httpx
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from first_common.schema.base_scheduler import SchedulerJobState
from first_common.schema.pilot import ReplicaStartRequest
from first_common.schema.types import PilotLaunchSpec, ReplicaState

from ...database.models import PilotDeployment, PilotJob, PilotReplica
from ...database.redis.pubsub import Channel
from ...services.pilot_control import PilotControlClient
from ...settings import ClientState
from ..controller import Controller, StaleReconcile
from ..wakeup import WakeupDispatcher

logger = logging.getLogger(__name__)


class ReplicaLauncher(Controller):
    """
    Launches placed replicas onto their PilotJobs once the job is running and its
    manager endpoint is reachable.

    This controller only drives the `placed -> launching` transition.
    """

    resource_type = PilotReplica
    wakeup_channels = [Channel.replica_placed, Channel.pilot_job_ready]

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
        self.client = PilotControlClient(client_state, cn="replica-launcher")

    async def list_actionable(self, sess: AsyncSession) -> list[int]:
        stmt = (
            sa.select(PilotReplica.uid)
            .join(PilotJob, PilotReplica.pilot_job_name == PilotJob.name)
            .where(
                PilotReplica.state == ReplicaState.placed.value,
                PilotReplica.scheduled_deletion_at.is_(None),
                PilotReplica.deleted_at.is_(None),
                sa.or_(
                    PilotReplica.reconcile_retry_at.is_(None),
                    PilotReplica.reconcile_retry_at < sa.func.now(),
                ),
                PilotJob.scheduler_state == SchedulerJobState.running.value,
                PilotJob.manager_url.is_not(None),
            )
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
                or replica.state != ReplicaState.placed.value
                or replica.scheduled_deletion
                or replica.deleted_at is not None
            ):
                return

            job = replica.pilot_job
            if (
                job is None
                or job.scheduler_state != SchedulerJobState.running.value
                or job.manager_url is None
            ):
                # Job not ready yet; a pilot_job_ready wake will bring us back.
                return

            manager_url = job.manager_url
            deploy = replica.pilot_deployment
            request = ReplicaStartRequest(
                name=replica.name,
                deployment_name=deploy.name,
                launch_spec=PilotLaunchSpec.model_validate(deploy.launch_spec),
                gpu_indices=list(replica.claimed_gpu_ids),
            )

        resp = await self.client.start_replica(manager_url, request)

        if resp.is_success:
            await self._mark_launching(replica.uid, replica.name)
            return

        if resp.status_code == 409:
            # A previous attempt's request likely succeeded on the backend even
            # though we never saw the response. Confirm the replica is really
            # registered before declaring victory.
            await self._handle_conflict(replica.uid, replica.name, manager_url)
            return

        if resp.status_code == 400:
            # The manager refused to start this replica (bad request / start
            # failure). Fault the deployment and mark the replica error so the
            # Reconciler drains it and frees its resources for a fresh attempt.
            await self._mark_error(replica.uid, replica.name, deploy.name, resp)
            return

        # Anything else (5xx, unexpected) is transient from our perspective:
        # let it raise so the standard reconcile cooldown/retry kicks in without
        # penalizing the deployment or draining the replica.
        resp.raise_for_status()

    async def _mark_launching(self, replica_uid: int, replica_name: str) -> None:
        async with self.client_state.db_sessionmaker.begin() as sess:
            result = await sess.execute(
                sa.update(PilotReplica)
                .where(
                    PilotReplica.uid == replica_uid,
                    PilotReplica.state == ReplicaState.placed.value,
                    PilotReplica.scheduled_deletion_at.is_(None),
                )
                .values(
                    state=ReplicaState.launching.value,
                    state_message="Launch requested on pilot manager.",
                )
            )
            if result.rowcount == 0:  # type: ignore[attr-defined]
                raise StaleReconcile(
                    f"ReplicaLauncher: {replica_name} no longer placed at launch"
                )
        logger.info("ReplicaLauncher: launched replica %s", replica_name)

    async def _handle_conflict(
        self, replica_uid: int, replica_name: str, manager_url: str
    ) -> None:
        status = await self.client.get_status(manager_url)
        if not any(r.name == replica_name for r in status.replicas):
            # 409 but the replica isn't actually there — inconsistent. Raise so
            # the next reconcile re-attempts a clean start.
            raise RuntimeError(
                f"ReplicaLauncher: {replica_name} got 409 but is absent from "
                f"pilot manager /status; will retry"
            )
        logger.info(
            "ReplicaLauncher: replica %s already registered (409); treating as "
            "launched",
            replica_name,
        )
        await self._mark_launching(replica_uid, replica_name)

    async def _mark_error(
        self,
        replica_uid: int,
        replica_name: str,
        deployment_name: str,
        resp: httpx.Response,
    ) -> None:
        message = _error_message(resp)
        async with self.client_state.db_sessionmaker.begin() as sess:
            result = await sess.execute(
                sa.update(PilotReplica)
                .where(
                    PilotReplica.uid == replica_uid,
                    PilotReplica.state == ReplicaState.placed.value,
                    PilotReplica.scheduled_deletion_at.is_(None),
                    PilotReplica.deleted_at.is_(None),
                )
                .values(
                    state=ReplicaState.error.value,
                    state_message=f"Launch rejected: {message}",
                )
            )
            if result.rowcount == 0:  # type: ignore[attr-defined]
                # Replica moved on under us; don't count a failure against a
                # placement that no longer exists.
                raise StaleReconcile(
                    f"ReplicaLauncher: {replica_name} no longer placed at error"
                )
            await sess.execute(
                sa.update(PilotDeployment)
                .where(PilotDeployment.name == deployment_name)
                .values(
                    consecutive_launch_failures=(
                        PilotDeployment.consecutive_launch_failures + 1
                    )
                )
            )
        logger.warning(
            "ReplicaLauncher: replica %s failed to launch: %s", replica_name, message
        )


def _error_message(resp: httpx.Response) -> str:
    """Best-effort extraction of the pilot error message from a response body."""
    try:
        return str(resp.json()["error"]["message"])
    except Exception:
        return resp.text or "unknown error"
