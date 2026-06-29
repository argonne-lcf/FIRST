import asyncio
import logging
from abc import ABC, abstractmethod
from time import monotonic
from typing import NamedTuple

from ..settings import ClientState

logger = logging.getLogger(__name__)


class Heartbeat:
    def __init__(self, name: str, timeout: float) -> None:
        self.name = name
        self.timeout = timeout
        self._last_beat = monotonic()

    def beat(self) -> None:
        self._last_beat = monotonic()

    def timed_out(self) -> bool:
        return monotonic() - self._last_beat >= self.timeout

    @property
    def since_last(self) -> float:
        return monotonic() - self._last_beat


class HeartbeatStatus(NamedTuple):
    timed_out: bool
    stale: list[Heartbeat]


class Worker(ABC):
    def __init__(
        self,
        name: str,
        client_state: ClientState,
        *,
        restart_backoff: float = 1.0,
        max_backoff: float = 30.0,
        heartbeat_timeout: float = 120.0,
    ) -> None:
        self.name = name
        self.client_state = client_state
        self._restart_backoff = restart_backoff
        self._max_backoff = max_backoff
        self._heartbeat_timeout = heartbeat_timeout

        self._heartbeats: list[Heartbeat] = []
        self.run_task: asyncio.Task[None] | None = None

    def register_heartbeat(self, name: str) -> Heartbeat:
        hb = Heartbeat(name=f"{self.name}.{name}", timeout=self._heartbeat_timeout)
        self._heartbeats.append(hb)
        return hb

    def check_heartbeat(self) -> HeartbeatStatus:
        stale = [h for h in self._heartbeats if h.timed_out()]
        return HeartbeatStatus(timed_out=bool(stale), stale=stale)

    async def supervise(self, shutdown: asyncio.Event) -> None:
        logger.info(f"Starting worker {self.name!r}")
        backoff = self._restart_backoff

        while not shutdown.is_set():
            self._heartbeats = []
            self.run_task = asyncio.create_task(self.run())

            try:
                await self.run_task
                logger.warning("worker %s exited cleanly; restarting", self.name)
                backoff = self._restart_backoff
            except asyncio.CancelledError as exc:
                if shutdown.is_set():
                    logger.info(f"Worker {self.name!r} shutting down")
                    raise
                logger.warning(
                    f"Restarting Worker {self.name!r} because task cancelled without shutdown [{exc}]"
                )
                backoff = self._restart_backoff
            except Exception:
                logger.exception(
                    f"worker {self.name!r} crashed; restarting in {backoff:.1f}s"
                )
                try:
                    await asyncio.wait_for(shutdown.wait(), timeout=backoff)
                    return
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 2, self._max_backoff)

    @abstractmethod
    async def run(self) -> None:
        raise NotImplementedError("All Worker subclasses must implement run()")
