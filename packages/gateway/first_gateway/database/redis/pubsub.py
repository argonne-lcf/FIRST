from enum import Enum
from typing import AsyncIterator

from redis.asyncio import Redis


class Channel(str, Enum):
    router_cfg_updated = "router_cfg_updated"
    desired_replicas_changed = "desired_replicas_changed"
    replica_created = "replica_created"


class RedisPubSub:
    def __init__(self, redis: Redis) -> None:
        self.client = redis

    @staticmethod
    def to_str(v: str | bytes) -> str:
        return v.decode() if isinstance(v, bytes) else v

    async def publish(self, channel: Channel, message: str) -> None:
        await self.client.publish(channel.value, message)

    async def subscribe(self, *channels: Channel) -> AsyncIterator[tuple[Channel, str]]:
        pubsub = self.client.pubsub()
        await pubsub.subscribe(*(s.value for s in channels))
        try:
            async for event in pubsub.listen():
                if event.get("type") != "message":
                    continue
                yield Channel(event["channel"]), self.to_str(event["data"])
        finally:
            await pubsub.aclose()  # type: ignore[no-untyped-call]

    async def subscribe_all(self) -> AsyncIterator[tuple[Channel, str]]:
        async for x in self.subscribe(*iter(Channel)):
            yield x
