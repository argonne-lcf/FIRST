import asyncio
import logging
from collections import defaultdict, deque

import sqlalchemy as sa

from first_common.schema.resources.status import DeploymentStatus

from ..database.inflight import AsyncInflightCounter
from ..database.models import PilotDeployment, StaticDeployment
from ..database.status_store import (
    PilotDeploymentStatusStore,
    StaticDeploymentStatusStore,
)
from .worker import Worker

logger = logging.getLogger(__name__)

PILOT_DEPLOYMENT_PREFIX = "pilot_deployment"
STATIC_DEPLOYMENT_PREFIX = "static_deployment"

_SAMPLES_1M = 6
_SAMPLES_5M = 30


class DeploymentLoadObserver(Worker):
    """Samples in-flight request counts for all deployments and writes
    smoothed 1m/5m load averages to Redis via the deployment StatusStores.
    """

    poll_interval: float = 10.0

    async def run(self) -> None:
        hb = self.register_heartbeat("poll")
        redis = self.client_state.redis
        counter = AsyncInflightCounter(redis)
        pilot_store = PilotDeploymentStatusStore(redis)
        static_store = StaticDeploymentStatusStore(redis)

        samples: dict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=_SAMPLES_5M)
        )

        while True:
            hb.beat()
            try:
                await self._poll(counter, pilot_store, static_store, samples)
            except Exception:
                logger.exception("load observer: poll failed")
            await asyncio.sleep(self.poll_interval)

    async def _poll(
        self,
        counter: AsyncInflightCounter,
        pilot_store: PilotDeploymentStatusStore,
        static_store: StaticDeploymentStatusStore,
        samples: dict[str, deque[float]],
    ) -> None:
        async with self.client_state.db_sessionmaker() as sess:
            pilot_names = list(await sess.scalars(sa.select(PilotDeployment.name)))
            static_names = list(await sess.scalars(sa.select(StaticDeployment.name)))

        for name in pilot_names:
            inflight_key = f"{PILOT_DEPLOYMENT_PREFIX}:{name}"
            count = await counter.count(inflight_key)
            buf = samples[inflight_key]
            buf.append(float(count))
            await pilot_store.update(name, _compute_status(buf))

        for name in static_names:
            inflight_key = f"{STATIC_DEPLOYMENT_PREFIX}:{name}"
            count = await counter.count(inflight_key)
            buf = samples[inflight_key]
            buf.append(float(count))
            await static_store.update(name, _compute_status(buf))

        stale = (
            set(samples)
            - {f"{PILOT_DEPLOYMENT_PREFIX}:{n}" for n in pilot_names}
            - {f"{STATIC_DEPLOYMENT_PREFIX}:{n}" for n in static_names}
        )
        for key in stale:
            del samples[key]


def _compute_status(buf: deque[float]) -> DeploymentStatus:
    recent_1m = list(buf)[-_SAMPLES_1M:]
    recent_5m = list(buf)[-_SAMPLES_5M:]
    return DeploymentStatus(
        load_avg_1m=sum(recent_1m) / len(recent_1m),
        load_avg_5m=sum(recent_5m) / len(recent_5m),
        load_max_1m=max(recent_1m),
        load_max_5m=max(recent_5m),
    )
