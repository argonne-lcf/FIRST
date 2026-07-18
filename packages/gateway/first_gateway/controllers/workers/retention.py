import logging

from ...database.models import PilotJob, PilotReplica
from ..worker import Worker

logger = logging.getLogger(__name__)

_SWEEPABLE = (PilotJob, PilotReplica)


class RetentionSweeper(Worker):
    """Hard-deletes soft-deleted rows whose retention window has elapsed.

    Runs sweep_expired() on every SoftDeletable table every poll_interval seconds.
    """

    poll_interval = 60.0

    async def run(self) -> None:
        hb = self.register_heartbeat("sweep")
        while True:
            hb.beat()
            await self._sweep_all()
            await self.wait_for_wake()

    async def _sweep_all(self) -> None:
        for cls in _SWEEPABLE:
            try:
                async with self.client_state.db_sessionmaker.begin() as sess:
                    count = await cls.sweep_expired(sess)
                if count:
                    logger.info(
                        "retention sweep: deleted %d expired row(s) from %s",
                        count,
                        cls.__name__,
                    )
            except Exception:
                logger.exception("retention sweep: failed to sweep %s", cls.__name__)
