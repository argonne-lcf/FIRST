"""
All caching is centralized in resource_server_async.cache
Caching uses Django cache (configured for Redis) with automatic fallback to in-memory cache
 - Endpoint caching: get_endpoint_from_cache(), cache_endpoint(), remove_endpoint_from_cache()
 - Streaming caching: All streaming functions use get_redis_client() for Redis-specific operations
 - Permission caching: In-memory TTLCache for performance-critical permission checks
"""

from logging import getLogger
from typing import TYPE_CHECKING, Any

import redis
from asgiref.sync import sync_to_async
from django.conf import settings
from django.core.cache import cache

if TYPE_CHECKING:
    from .clusters.cluster import BaseCluster
    from .endpoints.endpoint import BaseEndpoint


logger = getLogger(__name__)

_redis_client: redis.Redis | None = None
_redis_available: bool | None = None


def get_redis_client() -> redis.Redis | None:
    """Get Redis client for LIST and pipeline operations. Cached singleton."""
    global _redis_client, _redis_available

    if _redis_available is False:
        return None
    if _redis_client is not None:
        return _redis_client

    redis_url = next(
        (
            cache["LOCATION"]
            for cache in getattr(settings, "CACHES", {}).values()
            if cache.get("LOCATION", "").startswith("redis://")
        ),
        None,
    )

    if redis_url:
        try:
            _redis_client = redis.Redis.from_url(redis_url)
            _redis_client.ping()
        except Exception as e:
            logger.warning(f"Redis not available, falling back to Django cache: {e}")
        else:
            _redis_available = True
            logger.info("Redis client initialized successfully")
            return _redis_client

    _redis_available = False
    _redis_client = None
    return None


@sync_to_async
def should_throttle(*args: Any, ttl: int = 30) -> bool:
    """
    Returns True if called with the same *args less than `ttl` seconds ago.

    Uses underlying cache to store key of concatenated *args.
    """
    key = "".join(map(str, args))

    was_added = cache.add(key, "", ttl)
    return not was_added


def is_cached(key: str) -> bool:
    """Returns whether key exists in the cache."""
    return cache.has_key(key)


def get_item_from_cache(cache_key: str) -> Any:
    """Get item from cache or None if not found."""
    cached_item = cache.get(cache_key)
    if cached_item:
        logger.debug(f"Retrieved {cache_key} from cache.")
        return cached_item
    return None


get_item_from_cache_async = sync_to_async(get_item_from_cache)


def cache_item(cache_key: str, data: Any, ttl: int = 3600) -> None:
    """Cache item data (60 minutes TTL by default)."""
    cache.set(cache_key, data, ttl)
    logger.debug(f"Cached {cache_key}.")


cache_item_async = sync_to_async(cache_item)


def remove_item_from_cache(cache_key: str) -> None:
    """Remove item from cache"""
    cache.delete(cache_key)
    logger.debug(f"Removed {cache_key} from cache.")


def get_endpoint_from_cache(endpoint_slug: str) -> "BaseEndpoint | None":
    """Get endpoint adapter from cache or None if not found"""
    ep = get_item_from_cache(f"endpoint:{endpoint_slug}")
    assert isinstance(ep, BaseEndpoint) or ep is None
    return ep


def cache_endpoint(endpoint_slug: str, data: "BaseEndpoint") -> None:
    """Cache endpoint adapter"""
    cache_item(f"endpoint:{endpoint_slug}", data)


def remove_endpoint_from_cache(endpoint_slug: str) -> None:
    """Remove endpoint adapter from cache"""
    remove_item_from_cache(f"endpoint:{endpoint_slug}")


def get_cluster_from_cache(cluster_name: str) -> "BaseCluster | None":
    """Get cluster adapter from cache or None if not found"""
    obj: "BaseCluster | None" = get_item_from_cache(f"cluster:{cluster_name}")
    return obj


def cache_cluster(cluster_name: str, adapter: "BaseCluster") -> None:
    """Cache cluster adapter"""
    cache_item(f"cluster:{cluster_name}", adapter)


def remove_cluster_from_cache(cluster_name: str) -> None:
    """Remove cluster adapter from cache"""
    remove_item_from_cache(f"cluster:{cluster_name}")
