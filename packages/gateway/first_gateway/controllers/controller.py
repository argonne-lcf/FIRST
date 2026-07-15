import asyncio
import logging
from abc import abstractmethod
from time import monotonic
from typing import TYPE_CHECKING, Any, ClassVar

from prometheus_client import Counter, Gauge, Histogram
from sqlalchemy.ext.asyncio import AsyncSession

from ..database.models import ResourceRow
from ..settings import ClientState
from .worker import Heartbeat, Worker

if TYPE_CHECKING:
    from .manager import WakeupDispatcher

logger = logging.getLogger(__name__)

# -- Prometheus metrics --

RECONCILE_TOTAL = Counter(
    "controller_reconcile_total",
    "Total reconcile attempts by outcome",
    ["controller", "outcome"],
)
RECONCILE_DURATION = Histogram(
    "controller_reconcile_duration_seconds",
    "Duration of individual reconcile calls",
    ["controller"],
)
RESYNC_USED_FRACTION = Gauge(
    "controller_resync_interval_used_fraction",
    "Fraction of resync interval spent reconciling",
    ["controller"],
)
ACTIONABLE_ROWS = Gauge(
    "controller_actionable_rows",
    "Number of actionable rows found by list_actionable",
    ["controller"],
)
SECONDS_SINCE_RESYNC = Gauge(
    "controller_seconds_since_last_resync",
    "Seconds since the last full resync completed",
    ["controller"],
)


class StaleReconcile(Exception):
    """Raised by reconcile() when a premised update matches zero rows.

    The framework counts this as a normal (non-failing) outcome and
    increments the ``stale`` counter without recording a failure.
    """


class Controller(Worker):
    """Worker subclass that polls Postgres for actionable rows, calls
    reconcile() on each, and sleeps until either the resync interval
    elapses or a LISTEN/NOTIFY notification arrives.

    Subclasses set ``resource_type`` and implement ``list_actionable``
    and ``reconcile``.
    """

    resource_type: ClassVar[type[ResourceRow]]
    resync_interval: ClassVar[float] = 30.0
    max_backoff_seconds: ClassVar[float] = 3600.0

    def __init__(
        self,
        name: str,
        client_state: ClientState,
        *,
        dispatcher: "WakeupDispatcher | None" = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(name, client_state, **kwargs)
        self._dispatcher = dispatcher

    @property
    def table_name(self) -> str:
        tn: str = self.resource_type.__tablename__
        return tn

    @abstractmethod
    async def reconcile(self, uid: int) -> None: ...

    @abstractmethod
    async def list_actionable(self, sess: AsyncSession) -> list[int]: ...

    # -- reconcile loop --

    async def run(self) -> None:
        hb = self.register_heartbeat("reconcile")
        wake = self._dispatcher.event_for(self.table_name) if self._dispatcher else None
        last_tick_end = monotonic()

        while True:
            SECONDS_SINCE_RESYNC.labels(self.name).set(monotonic() - last_tick_end)
            hb.beat()

            tick_start = monotonic()
            await self._tick(hb)
            last_tick_end = monotonic()

            RESYNC_USED_FRACTION.labels(self.name).set(
                (last_tick_end - tick_start) / self.resync_interval
            )

            if wake:
                try:
                    await asyncio.wait_for(wake.wait(), timeout=self.resync_interval)
                except asyncio.TimeoutError:
                    pass
                finally:
                    wake.clear()
            else:
                await asyncio.sleep(self.resync_interval)

    async def _tick(self, hb: Heartbeat) -> None:
        async with self.client_state.db_sessionmaker() as sess:
            ids = await self.list_actionable(sess)

        ACTIONABLE_ROWS.labels(self.name).set(len(ids))

        for uid in ids:
            hb.beat()
            await self._reconcile_one(uid)

    async def _reconcile_one(self, uid: int) -> None:
        t0 = monotonic()
        try:
            await self.reconcile(uid)
        except StaleReconcile:
            outcome = "stale"
            logger.warning("%s: reconcile uid=%d stale", self.name, uid)
        except Exception as exc:
            outcome = "failure"
            logger.exception("%s: reconcile uid=%d failed", self.name, uid)
            await self._record_failure(uid, exc)
        else:
            outcome = "success"
            await self._record_success(uid)
        finally:
            RECONCILE_TOTAL.labels(self.name, outcome).inc()
            RECONCILE_DURATION.labels(self.name).observe(monotonic() - t0)

    async def _record_success(self, uid: int) -> None:
        async with self.client_state.db_sessionmaker.begin() as sess:
            await self.resource_type.reset_reconcile_state(sess, uid)

    async def _record_failure(self, uid: int, exc: Exception) -> None:
        async with self.client_state.db_sessionmaker.begin() as sess:
            await self.resource_type.record_failure(sess, uid, exc)
