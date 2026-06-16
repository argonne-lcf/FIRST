import asyncio
import logging
from abc import ABC, abstractmethod
from time import monotonic
from typing import NamedTuple

from ..settings import ClientState

logger = logging.getLogger(__name__)


class HeartbeatStatus(NamedTuple):
    timed_out: bool
    since_last: float


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

        self._last_heartbeat: float | None = None
        self.run_task: asyncio.Task[None] | None = None

    def update_heartbeat(self) -> None:
        self._last_heartbeat = monotonic()

    def check_heartbeat(self) -> HeartbeatStatus:
        if self._last_heartbeat is None:
            return HeartbeatStatus(False, since_last=0.0)

        since_last = monotonic() - self._last_heartbeat
        return HeartbeatStatus(since_last >= self._heartbeat_timeout, since_last)

    async def supervise(self, shutdown: asyncio.Event) -> None:
        logger.info(f"Starting worker {self.name!r}")
        backoff = self._restart_backoff

        while not shutdown.is_set():
            self.update_heartbeat()
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
        """
        Implement worker loop here. Worker must periodically call
        self.update_heartbeat() when making progress.
        """
        raise NotImplementedError("All Worker subclasses must implement run()")
