import logging

from ..worker import Worker

logger = logging.getLogger(__name__)


class HealthAlerter(Worker):
    """Emits Slack health alerts"""

    poll_interval = 60.0

    async def run(self) -> None:
        hb = self.register_heartbeat("poll")
        while True:
            hb.beat()
            await self.poll()
            await self.wait_for_wake()

    async def poll(self) -> None:
        return
