import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone

import sqlalchemy as sa

from first_common.schema.resources.spec import ClusterSpec
from first_common.schema.resources.status import ClusterStatusInfo
from first_common.schema.types import ClusterStatus

from ..database.models import Cluster
from ..database.status_store import ClusterStatusStore
from .worker import Worker

logger = logging.getLogger(__name__)


class ClusterStatusObserver(Worker):
    """Polls each Cluster's configured status endpoint.

    Writes the aggregated ``Cluster.status`` to Postgres only on transition
    (premised ``IS DISTINCT FROM`` bulk update) and the high-churn
    ``last_status_check`` timestamp to Redis via ``ClusterStatusStore``.
    """

    poll_interval: float = 30.0

    async def run(self) -> None:
        hb = self.register_heartbeat("poll")
        store = ClusterStatusStore(self.client_state.redis)
        while True:
            hb.beat()
            try:
                await self._poll(store)
            except Exception:
                logger.exception("cluster status observer: poll failed")
            await asyncio.sleep(self.poll_interval)

    async def _poll(self, store: ClusterStatusStore) -> None:
        async with self.client_state.db_sessionmaker() as sess:
            clusters = list(await sess.scalars(sa.select(Cluster)))

        results = await asyncio.gather(*(self._check(store, c) for c in clusters))
        observed = [r for r in results if r is not None]
        if not observed:
            return

        # Group the observed uids by target status so each premised UPDATE
        # writes one value.  The set of statuses is tiny (<= 5), so this is a
        # handful of round trips at most.  Sort uids for deadlock-free locking.
        by_status: dict[ClusterStatus, list[int]] = defaultdict(list)
        for uid, status in observed:
            by_status[status].append(uid)

        async with self.client_state.db_sessionmaker.begin() as sess:
            for status in sorted(by_status):
                uids = sorted(by_status[status])
                await sess.execute(
                    sa.update(Cluster)
                    .where(
                        Cluster.uid.in_(uids),
                        Cluster.status.is_distinct_from(status.value),
                    )
                    .values(status=status.value)
                )

    async def _check(
        self, store: ClusterStatusStore, cluster: Cluster
    ) -> tuple[int, ClusterStatus] | None:
        """Run one cluster's status check. Returns (uid, status) for the
        transition batch, or None if the check failed."""
        spec = ClusterSpec.model_validate(cluster)
        try:
            status: ClusterStatus = await spec.status_method(
                client=self.client_state.httpx_client, **spec.status_kwargs
            )
        except Exception:
            logger.exception(
                "cluster status observer: check failed for %s", cluster.name
            )
            return None

        await store.update(
            cluster.name,
            ClusterStatusInfo(last_status_check=datetime.now(timezone.utc)),
        )
        return cluster.uid, status
