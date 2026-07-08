import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone

import sqlalchemy as sa

from first_common.schema.resources.spec import StaticDeploymentSpec
from first_common.schema.resources.status import DeploymentStatus
from first_common.schema.types import DeploymentHealth, HealthEndpointStatus

from ..database.models import StaticDeployment
from ..database.status_store import StaticDeploymentStatusStore
from .worker import Worker

logger = logging.getLogger(__name__)

_HEALTH_MAP: dict[HealthEndpointStatus, DeploymentHealth] = {
    HealthEndpointStatus.healthy: DeploymentHealth.healthy,
    HealthEndpointStatus.unhealthy: DeploymentHealth.unhealthy,
    HealthEndpointStatus.unknown: DeploymentHealth.offline,
}


class StaticDeploymentHealthObserver(Worker):
    """Polls each StaticDeployment's configured health endpoint.

    Writes the aggregated ``StaticDeployment.health`` to Postgres only on
    transition (premised ``IS DISTINCT FROM`` bulk update) and the high-churn
    ``last_health_check`` timestamp to Redis via ``StaticDeploymentStatusStore``.
    """

    poll_interval: float = 30.0

    async def run(self) -> None:
        hb = self.register_heartbeat("poll")
        store = StaticDeploymentStatusStore(self.client_state.redis)
        while True:
            hb.beat()
            try:
                await self._poll(store)
            except Exception:
                logger.exception("static health observer: poll failed")
            await asyncio.sleep(self.poll_interval)

    async def _poll(self, store: StaticDeploymentStatusStore) -> None:
        async with self.client_state.db_sessionmaker() as sess:
            deployments = list(await sess.scalars(sa.select(StaticDeployment)))

        results = await asyncio.gather(*(self._check(store, d) for d in deployments))
        observed = [r for r in results if r is not None]
        if not observed:
            return

        # Group the observed uids by target health so each premised UPDATE
        # writes one value.  The set of health values is tiny, so this is a
        # handful of round trips at most.  Sort uids for deadlock-free locking.
        by_health: dict[DeploymentHealth, list[int]] = defaultdict(list)
        for uid, health in observed:
            by_health[health].append(uid)

        async with self.client_state.db_sessionmaker.begin() as sess:
            for health in sorted(by_health):
                uids = sorted(by_health[health])
                await sess.execute(
                    sa.update(StaticDeployment)
                    .where(
                        StaticDeployment.uid.in_(uids),
                        StaticDeployment.health.is_distinct_from(health.value),
                    )
                    .values(health=health.value)
                )

    async def _check(
        self, store: StaticDeploymentStatusStore, dep: StaticDeployment
    ) -> tuple[int, DeploymentHealth] | None:
        """Run one deployment's health check. Returns (uid, health) for the
        transition batch, or None if the check failed."""
        spec = StaticDeploymentSpec.model_validate(dep)
        try:
            status: HealthEndpointStatus = await spec.health_check_method(
                client=self.client_state.httpx_client,
                base_url=dep.api_url,
                **spec.health_check_kwargs,
            )
        except Exception:
            logger.exception("static health observer: check failed for %s", dep.name)
            return None

        await store.update(
            dep.name,
            DeploymentStatus(last_health_check=datetime.now(timezone.utc)),
        )
        return dep.uid, _HEALTH_MAP[status]
