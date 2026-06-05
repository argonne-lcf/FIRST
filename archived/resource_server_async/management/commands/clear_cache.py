"""
Management command to clear application caches from Redis.

Usage:
    python manage.py clear_cache              # Clear app caches, preserve sessions
    python manage.py clear_cache --all        # Clear everything including sessions
    python manage.py clear_cache --pattern endpoint:*  # Clear specific pattern
"""

import logging

from django.conf import settings
from django.core.cache import cache
from django.core.management.base import BaseCommand

log = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Clear application caches from Redis"

    def add_arguments(self, parser):
        parser.add_argument(
            "--all",
            action="store_true",
            help="Clear ALL cache including Django sessions (use with caution)",
        )
        parser.add_argument(
            "--pattern",
            type=str,
            help='Clear specific cache pattern (e.g., "endpoint:*")',
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be deleted without actually deleting",
        )

    def handle(self, *args, **options):
        from resource_server_async.cache import get_redis_client

        dry_run = options.get("dry_run", False)
        clear_all = options.get("all", False)
        pattern = options.get("pattern")

        redis_client = get_redis_client()

        if not redis_client:
            self.stdout.write(
                self.style.ERROR(
                    "Redis client not available. Using Django cache.clear()"
                )
            )
            if not dry_run:
                cache.clear()
                self.stdout.write(self.style.SUCCESS("Cleared all Django cache"))
            else:
                self.stdout.write(
                    self.style.WARNING("[DRY RUN] Would clear all Django cache")
                )
            return

        # Get cache key prefix
        prefix = settings.CACHES.get("redis", {}).get("KEY_PREFIX", "")

        if clear_all:
            # Clear everything
            if not dry_run:
                cache.clear()
                self.stdout.write(
                    self.style.WARNING("Cleared ALL cache including Django sessions")
                )
            else:
                self.stdout.write(
                    self.style.WARNING(
                        "[DRY RUN] Would clear ALL cache including Django sessions"
                    )
                )
            return

        if pattern:
            # Clear specific pattern
            patterns = [pattern]
        else:
            # Default: clear app caches, preserve sessions
            patterns = [
                "endpoint:*",
                "endpoint_status:*",
                "stream:*",
                "dashboard_health:*",
                "dashboard_token_validation:*",
                "globus_group_membership:*",
            ]

        total_deleted = 0
        for pattern in patterns:
            # Add prefix if configured
            full_pattern = f"{prefix}:{pattern}" if prefix else pattern

            # Get matching keys
            keys = redis_client.keys(full_pattern)

            if keys:
                self.stdout.write(
                    self.style.NOTICE(f'Pattern "{pattern}": found {len(keys)} keys')
                )

                # Show first few keys as examples
                for key in keys[:5]:
                    key_str = key.decode("utf-8") if isinstance(key, bytes) else key
                    ttl = redis_client.ttl(key)
                    self.stdout.write(f"  - {key_str} (TTL: {ttl}s)")

                if len(keys) > 5:
                    self.stdout.write(f"  ... and {len(keys) - 5} more")

                if not dry_run:
                    # Delete the keys
                    deleted = redis_client.delete(*keys)
                    total_deleted += deleted
                    self.stdout.write(self.style.SUCCESS(f"  Deleted {deleted} keys"))
                else:
                    self.stdout.write(
                        self.style.WARNING(f"  [DRY RUN] Would delete {len(keys)} keys")
                    )
                    total_deleted += len(keys)
            else:
                self.stdout.write(
                    self.style.NOTICE(f'Pattern "{pattern}": no keys found')
                )

        if dry_run:
            self.stdout.write(
                self.style.SUCCESS(
                    f"\n[DRY RUN] Would delete {total_deleted} cache keys total"
                )
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(f"\nDeleted {total_deleted} cache keys total")
            )

        # Show cache stats
        try:
            info = redis_client.info("keyspace")
            self.stdout.write(f"\nRedis keyspace info: {info}")
        except Exception as e:
            self.stdout.write(self.style.WARNING(f"Could not get Redis stats: {e}"))
