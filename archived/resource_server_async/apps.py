import logging

from django.apps import AppConfig

log = logging.getLogger(__name__)


class ResourceServerAsyncConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "resource_server_async"

    def ready(self) -> None:
        """Called when Django starts up - clear application caches"""
        try:
            # Clear application-specific caches on startup
            # This preserves Django sessions but clears our app caches
            cache_patterns = [
                "endpoint:*",
                "endpoint_status:*",
                "stream:*",
                "dashboard_health:*",
                "dashboard_token_validation:*",
                "globus_group_membership:*",
            ]

            # Try to use Redis client for pattern-based deletion
            from resource_server_async.cache import get_redis_client

            redis_client = get_redis_client()

            if redis_client:
                # Get the cache key prefix from Django settings
                from django.conf import settings

                assert isinstance(settings.CACHES, dict)
                assert isinstance(
                    redis_config := settings.CACHES.get("redis", {}), dict
                )
                prefix = redis_config.get("KEY_PREFIX", "")

                deleted_count = 0
                for pattern in cache_patterns:
                    full_pattern = f"{prefix}:{pattern}" if prefix else pattern
                    keys = redis_client.keys(full_pattern)
                    if keys:
                        deleted_count += redis_client.delete(*keys)  # type: ignore

                log.info(f"Cleared {deleted_count} application cache keys on startup")
            else:
                # Fallback: just clear all cache (will also clear sessions)
                # Uncomment only if you want aggressive cache clearing
                # cache.clear()
                log.warning("Redis client not available for selective cache clearing")

        except Exception as e:
            log.warning(f"Could not clear cache on startup: {e}")
