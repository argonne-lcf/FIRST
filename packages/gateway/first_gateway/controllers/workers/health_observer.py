import asyncio
import logging
from collections import defaultdict

import sqlalchemy as sa
from httpx import Client

from first_common.health import perform_health_check
from first_common.schema.types import HealthCheckParams, HealthCheckResult

from ...database.models import Cluster, StaticDeployment
from ...settings import ClientState
from ..worker import Worker

logger = logging.getLogger(__name__)


class HealthObserver(Worker):
    """
    Polls the configured health endpoint of Clusters and StaticDeployments.

    Writes the aggregated `health` to Postgres only on transition.
    Healthy->Unhealthy transitions are debounced to mitigate intermittent
    failures.
    """

    poll_interval: float = 30.0

    def __init__(
        self,
        name: str,
        client_state: ClientState,
        *,
        restart_backoff: float = 1.0,
        max_backoff: float = 30.0,
        heartbeat_timeout: float = 120.0,
    ) -> None:
        super().__init__(
            name,
            client_state,
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
            await asyncio.sleep(self.poll_interval)

    async def _poll(self) -> None:
        async with self.client_state.db_sessionmaker() as sess:
            clusters = await Cluster.list(sess)
            deployments = await StaticDeployment.list(sess)

        checks = [self._check(c) for c in clusters] + [
            self._check(d) for d in deployments
        ]
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

        Returns `(kind, uid, health)` for the transition batch, or ``None`` when
        the first failure is being debounced.
        """
        params = HealthCheckParams.model_validate(resource.health_check)
        result = await perform_health_check(self.health_client, params)

        key = (resource.kind, resource.uid)

        if result == HealthCheckResult.unhealthy:
            self.fail_counts[key] += 1
            if self.fail_counts[key] < params.debounce:
                return None
        else:
            self.fail_counts.pop(key, None)

        return *key, result
