import asyncio
import logging

from first.controllers.worker import Worker

logger = logging.getLogger(__name__)


class ClusterHealthController(Worker):
    async def run(self) -> None:
        while True:
            await asyncio.sleep(10)
            logger.info("Checking cluster statuses")
