"""Sidecar that mirrors V2-managed models into V1 Endpoint rows.

Runs as a long-lived systemd service. Every ``FIRST_V2_POLL_INTERVAL_SEC`` it
reads the V2 RouterConfig blob from Redis and reconciles the bridge-managed
``Endpoint`` rows (scoped to the FirstV2Endpoint adapter) so V1 can proxy
inference to the V2 backends.

Usage:
    python manage.py first_v2_bridge            # run forever
    python manage.py first_v2_bridge --once     # one reconcile tick, then exit
"""

import logging
import time
from typing import Any

from django.core.management.base import BaseCommand

from resource_server_async.first_v2_bridge.mapping import (
    ENDPOINT_ADAPTER,
    DesiredEndpoint,
    desired_endpoints,
)
from resource_server_async.first_v2_bridge.router_config import (
    RouterConfig,
    get_bridge_redis_client,
)
from resource_server_async.first_v2_bridge.settings import BridgeSettings
from resource_server_async.models import Endpoint

log = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Mirror V2-managed models into V1 Endpoint rows"

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument(
            "--once",
            action="store_true",
            help="Run a single reconcile tick and exit.",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        settings = BridgeSettings()

        if options.get("once", False):
            self.stdout.write("first_v2_bridge running once")
            return self._tick(settings)

        self.stdout.write(
            f"first_v2_bridge starting poll={settings.poll_interval_sec}s"
        )

        while True:
            try:
                self._tick(settings)
            except Exception as e:
                log.exception("first_v2_bridge tick failed: %s", e)
            time.sleep(settings.poll_interval_sec)

    def _tick(self, settings: BridgeSettings) -> None:
        redis_client = get_bridge_redis_client(settings.redis_url)
        cfg = RouterConfig.load(redis_client)
        if cfg is None:
            log.warning("No RouterConfig; skipping tick")
            return
        desired = desired_endpoints(cfg, settings)

        created, updated, deleted = self._reconcile_endpoints(desired)

        log.info(
            "first_v2_bridge tick: version=%s desired=%s created=%s updated=%s deleted=%s",
            cfg.version,
            len(desired),
            created,
            updated,
            deleted,
        )

    def _reconcile_endpoints(
        self, desired: list[DesiredEndpoint]
    ) -> tuple[int, int, int]:
        """Upsert desired rows and delete stale bridge-managed rows.

        Only rows using the FirstV2Endpoint adapter are ever touched.
        """
        desired_by_slug = {d.endpoint_slug: d for d in desired}

        existing_slugs = set(
            Endpoint.objects.filter(endpoint_adapter=ENDPOINT_ADAPTER).values_list(
                "endpoint_slug", flat=True
            )
        )

        created = updated = 0
        for slug, d in desired_by_slug.items():
            _, was_created = Endpoint.objects.update_or_create(
                endpoint_slug=slug,
                defaults={
                    "id": d.pk,
                    "cluster": d.cluster,
                    "framework": d.framework,
                    "model": d.model,
                    "endpoint_adapter": d.endpoint_adapter,
                    "allowed_globus_groups": d.allowed_globus_groups,
                    "allowed_domains": d.allowed_domains,
                    "config": repr(d.config),
                },
            )
            if was_created:
                created += 1
            else:
                updated += 1

        stale = existing_slugs - set(desired_by_slug)
        deleted = 0
        if stale:
            deleted, _ = Endpoint.objects.filter(
                endpoint_adapter=ENDPOINT_ADAPTER, endpoint_slug__in=stale
            ).delete()

        return created, updated, deleted
