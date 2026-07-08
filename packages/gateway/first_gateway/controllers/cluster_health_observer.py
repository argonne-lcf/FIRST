import asyncio
import logging
from collections import defaultdict

import sqlalchemy as sa

from first_common.health import perform_health_check
from first_common.schema.types import HealthCheckParams, HealthCheckResult

from ..database.models import Cluster
from .worker import Worker

logger = logging.getLogger(__name__)


class ClusterHealthObserver(Worker):
    """Polls each Cluster's configured health endpoint.

    Writes the aggregated ``Cluster.health`` to Postgres only on transition
    """

    poll_interval: float = 30.0

    async def run(self) -> None:
        hb = self.register_heartbeat("poll")
        while True:
            hb.beat()
            try:
                await self._poll()
            except Exception:
                logger.exception("cluster health observer: poll failed")
            await asyncio.sleep(self.poll_interval)

    async def _poll(self) -> None:
        async with self.client_state.db_sessionmaker() as sess:
            clusters = list(await sess.scalars(sa.select(Cluster)))

        results = await asyncio.gather(*(self._check(c) for c in clusters))
        observed = [r for r in results if r is not None]
        if not observed:
            return

        # Group the observed uids by health so each premised UPDATE writes one value.
        by_health: dict[HealthCheckResult, list[int]] = defaultdict(list)
        for uid, health in observed:
            by_health[health].append(uid)

        async with self.client_state.db_sessionmaker.begin() as sess:
            for health in sorted(by_health):
                uids = sorted(by_health[health])
                await sess.execute(
                    sa.update(Cluster)
                    .where(
                        Cluster.uid.in_(uids),
                        Cluster.health.is_distinct_from(health.value),
                    )
                    .values(health=health.value)
                )

    async def _check(self, cluster: Cluster) -> tuple[int, HealthCheckResult]:
        """Run one cluster's health check. Returns (uid, health) for the
        transition batch, or None if the check failed."""

        params = HealthCheckParams.model_validate(cluster.health_check)
        health = await perform_health_check(
            client=self.client_state.httpx_client, **params.model_dump()
        )
        return cluster.uid, health
