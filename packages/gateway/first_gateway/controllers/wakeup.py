import asyncio
import logging
from collections import defaultdict

from ..database.redis.pubsub import Channel
from ..settings import ClientState

logger = logging.getLogger(__name__)


class WakeupDispatcher:
    """
    Single RedisPubSub subscriber connection in the manager.
    Fans out to Events to wake up every interested Controller.
    """

    def __init__(self, client_state: ClientState) -> None:
        self._subscribers: dict[Channel, list[asyncio.Event]] = defaultdict(list)
        self._pubsub = client_state.redis_pubsub

    def subscribe(self, event: asyncio.Event, channels: list[Channel]) -> None:
        for channel in channels:
            self._subscribers[channel].append(event)

    def wakeup(self, channel: Channel) -> None:
        for event in self._subscribers[channel]:
            event.set()

    async def run(self) -> None:
        while True:
            try:
                async for channel, _msg in self._pubsub.subscribe_all():
                    self.wakeup(channel)
            except Exception:
                logger.exception("error in WakeupDispatcher subscribe")
                await asyncio.sleep(5)
