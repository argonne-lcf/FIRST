from typing import Any

from redis.asyncio import Redis

from first_gateway import Settings

_redis_client: Redis | None = None


async def get_redis_client() -> Redis:
    global _redis_client

    if _redis_client is None:
        _redis_client = Redis.from_url(Settings.load().redis_url)
        await _redis_client.ping()

    return _redis_client


async def should_throttle(*args: Any, ttl: int = 30) -> bool:
    """
    Returns True if called with the same *args less than `ttl` seconds ago.

    Uses underlying cache to store key of concatenated *args.
    """
    key = "".join(map(str, args))
    client = await get_redis_client()

    was_added = await client.set(key, "", nx=True, ex=ttl)
    return not was_added


async def get(key: str) -> Any:
    """
    Get cache value by key
    """
    client = await get_redis_client()
    return await client.get(key)


async def set(key: str, value: Any, ttl: int | None = 120) -> Any:
    """
    Set/overwrite cache entry
    """
    client = await get_redis_client()
    return await client.set(key, value, ex=ttl)
