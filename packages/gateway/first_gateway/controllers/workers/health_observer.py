import asyncio
import logging
from collections import defaultdict

import sqlalchemy as sa
from httpx import Client

from first_common.health import perform_health_check
from first_common.schema.types import HealthCheckParams, HealthCheckResult

from ...database.models import Cluster, StaticDeployment
from ...settings import ClientState
from ..wakeup import WakeupDispatcher
from ..worker import Worker

logger = logging.getLogger(__name__)


class HealthObserver(Worker):
    """
    Polls the configured health endpoint of Clusters and StaticDeployments.

    Writes the aggregated `health` to Postgres only on transition.
    Healthy->Unhealthy transitions are debounced to mitigate intermittent
    failures.
    """

    poll_interval = 30.0

    def __init__(
        self,
        name: str,
        client_state: ClientState,
        wakeup_dispatcher: WakeupDispatcher,
        *,
        restart_backoff: float = 1.0,
        max_backoff: float = 30.0,
        heartbeat_timeout: float = 120.0,
    ) -> None:
        super().__init__(
            name,
            client_state,
            wakeup_dispatcher,
            restart_backoff=restart_backoff,
            max_backoff=max_backoff,
            heartbeat_timeout=heartbeat_timeout,
        )
        self.fail_counts: dict[tuple[str, int], int] = defaultdict(int)
        self.health_client = Client()

    async def run(self) -> None:
        hb = self.register_heartbeat("poll")
        while True:
            hb.beat()
            await self._poll()
            await self.wait_for_wake()

    async def _poll(self) -> None:
        async with self.client_state.db_sessionmaker() as sess:
            clusters = await Cluster.list(sess)
            deployments = await StaticDeployment.list(sess)

        # Skip the broader health check on clusters with reconcile failures:
        # those take precedence
        cluster_checks = [
            self._check(c)
            for c in clusters
            if c.reconcile_failures == 0 and not c.maintenance_notice
        ]
        sd_checks = [self._check(d) for d in deployments]
        checks = cluster_checks + sd_checks
        results = [r for r in await asyncio.gather(*checks) if r is not None]

        by_health: dict[str, dict[HealthCheckResult, list[int]]] = {
            "Cluster": defaultdict(list),
            "StaticDeployment": defaultdict(list),
        }

        for kind, uid, health in results:
            by_health[kind][health].append(uid)

        async with self.client_state.db_sessionmaker.begin() as sess:
            for ResourceCls in (Cluster, StaticDeployment):
                kind = ResourceCls.__name__

                for health in sorted(by_health[kind]):
                    uids = sorted(by_health[kind][health])

                    await sess.execute(
                        sa.update(ResourceCls)
                        .where(
                            ResourceCls.uid.in_(uids),
                            ResourceCls.health.is_distinct_from(health.value),
                        )
                        .values(health=health.value)
                    )

    async def _check(
        self, resource: Cluster | StaticDeployment
    ) -> tuple[str, int, HealthCheckResult] | None:
        """Run one health check.

        Returns `(kind, uid, health)` for the transition batch. Returns ``None``
        when no healthcheck URL is configured (health is owned by
        PilotJobObserver in that case) or when a first failure is being
        debounced.
        """
        params = None
        try:
            params = HealthCheckParams.model_validate(resource.health_check)
            if not params.url:
                return None
            result = await perform_health_check(self.health_client, params)
        except Exception as exc:
            # A bad secret/configuration belongs to this resource, not the
            # entire poll. Never log exception text or config: either can carry
            # credentials. CancelledError remains uncaught for worker shutdown.
            logger.warning(
                "Health check failed for %s %s (uid=%s, error=%s)",
                resource.kind,
                resource.name,
                resource.uid,
                type(exc).__name__,
            )
            result = HealthCheckResult.unhealthy

        key = (resource.kind, resource.uid)

        if result == HealthCheckResult.unhealthy:
            self.fail_counts[key] += 1
            debounce = (
                params.debounce
                if params is not None
                else HealthCheckParams(url="").debounce
            )
            if self.fail_counts[key] < debounce:
                return None
        else:
            self.fail_counts.pop(key, None)

        return *key, result
