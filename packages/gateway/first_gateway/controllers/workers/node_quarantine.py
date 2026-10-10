"""Optional controller worker; node isolation never pauses model controllers."""

import logging

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from first_common.schema.types import PilotConfig

from ...database.models import Cluster, NodeQuarantine
from ...services.quarantine import QuarantineService
from ..controller import Controller
from ..worker import Heartbeat

logger = logging.getLogger(__name__)


class NodeQuarantineController(Controller):
    resource_type = NodeQuarantine

    async def _tick(self, hb: Heartbeat) -> None:
        async with self.client_state.db_sessionmaker() as sess:
            clusters = await Cluster.list(sess)
        service = QuarantineService(self.client_state)
        for cluster in clusters:
            hb.beat()
            if cluster.pilot_system is None:
                continue
            pilot = PilotConfig.model_validate(cluster.pilot_system)
            if pilot.quarantine is None:
                continue
            try:
                await service.ingest_cluster(cluster, pilot, hb.beat)
            except Exception as error:
                # Auxiliary parsing/transport failures do not block ordinary
                # scheduler polling, placement, termination or replica retries.
                logger.exception(
                    "node quarantine report ingestion failed for %s", cluster.name
                )
                await service.ingestion_failure(cluster, pilot, error)
        await super()._tick(hb)

    async def list_actionable(self, sess: AsyncSession) -> list[int]:
        return list(
            await sess.scalars(
                sa.select(NodeQuarantine.uid).where(
                    NodeQuarantine.state != "released",
                    sa.or_(
                        NodeQuarantine.reconcile_retry_at.is_(None),
                        NodeQuarantine.reconcile_retry_at < sa.func.now(),
                    ),
                )
            )
        )

    async def reconcile(self, uid: int) -> None:
        await QuarantineService(self.client_state).reconcile(uid)
