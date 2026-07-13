import asyncio
import logging

from ...database.redis.admission import AdmissionController
from ..worker import Worker

logger = logging.getLogger(__name__)


class InflightObserver(Worker):
    """
    Periodic backstop that invokes AdmissionController.repair_orphaned_zsets().
    to correct any drift in the derived Redis counts.

    Under correct operation this is always a no-op.  Drift indicates an
    operational incident (Redis data loss, stale backup restore, bug in TTL
    handling).
    """

    poll_interval: float = 600.0  # 10 minutes

    async def run(self) -> None:
        hb = self.register_heartbeat("poll")
        while True:
            hb.beat()
            try:
                await self._reconcile()
            except Exception:
                logger.exception("%s: reconcile failed", self.name)
            await asyncio.sleep(self.poll_interval)

    async def _reconcile(self) -> None:
        redis = self.client_state.redis
        ac = AdmissionController(redis)
        await ac.repair_orphaned_zsets()
