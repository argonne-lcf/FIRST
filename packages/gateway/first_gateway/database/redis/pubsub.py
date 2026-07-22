from enum import Enum
from typing import AsyncIterator

from redis.asyncio import Redis


class Channel(str, Enum):
    router_cfg_updated = "router_cfg_updated"
    desired_replicas_changed = "desired_replicas_changed"
    replica_created = "replica_created"
    replica_placed = "replica_placed"
    replica_drain = "replica_drain"
    replica_started = "replica_started"
    pilot_job_created = "pilot_job_created"
    pilot_job_ready = "pilot_job_ready"


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
            # Poll with an explicit read timeout; listen() raises TimeoutError
            # with the default socket_timeout (5s) on a quiet connection.
            while True:
                event = await pubsub.get_message(timeout=30.0)
                if event is None or event.get("type") != "message":
                    continue
                yield Channel(event["channel"]), self.to_str(event["data"])
        finally:
            await pubsub.aclose()  # type: ignore[no-untyped-call]

    async def subscribe_all(self) -> AsyncIterator[tuple[Channel, str]]:
        async for x in self.subscribe(*iter(Channel)):
            yield x
