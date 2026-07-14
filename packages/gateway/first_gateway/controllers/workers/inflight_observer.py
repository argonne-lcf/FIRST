import asyncio
import logging

from ...database.redis.admission import AdmissionController
from ..worker import Worker

logger = logging.getLogger(__name__)


class InflightObserver(Worker):
    """
    Periodic backstop that invokes AdmissionController .sweep() and
    .repair_orphaned_zsets() to clean expired reservations (crashed workers) and
    correct any drift in the derived Redis counts.

    Under correct operation this is always a no-op.
    """

    poll_interval: float = 30.0

    async def run(self) -> None:
        hb = self.register_heartbeat("poll")
        iteration = 0
        while True:
            hb.beat()
            try:
                await self._reconcile(iteration)
            except Exception:
                logger.exception("%s: reconcile failed", self.name)
            await asyncio.sleep(self.poll_interval)
            iteration += 1

    async def _reconcile(self, iteration: int) -> None:
        redis = self.client_state.redis
        ac = AdmissionController(redis)

        # Sweep expired reservations every 30sec:
        await ac.sweep(batch=200)

        # On startup and once every 20 minutes:
        if iteration % 40 == 0:
            removed = await ac.repair_orphaned_zsets()
            if removed:
                logger.warning(
                    f"Removed {removed} orphaned reservations from inflight sets. "
                    "This means the AdmissionController settled one or more reservations without "
                    "cleaning up the matching inflight sets. I handled it, but "
                    "please verify the root cause."
                )
