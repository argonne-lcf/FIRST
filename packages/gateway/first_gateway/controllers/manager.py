import asyncio
import json
import logging
import signal

import psycopg
import uvloop
from sqlalchemy.engine import make_url

from first_gateway.log_config import config_logging

from ..settings import ClientState, Settings
from .cluster.health import ClusterHealthController
from .lease import ManagerLease
from .worker import Worker

logger = logging.getLogger("first_gateway.controllers.manager")


class WakeupDispatcher:
    """Single LISTEN connection in the manager; fans out per-table wakes."""

    def __init__(self) -> None:
        self._events: dict[str, asyncio.Event] = {}

    def event_for(self, table: str) -> asyncio.Event:
        return self._events.setdefault(table, asyncio.Event())

    async def run(self, conninfo: str) -> None:
        aconn = await psycopg.AsyncConnection.connect(conninfo, autocommit=True)
        try:
            await aconn.execute("LISTEN resource_changes")
            async for notify in aconn.notifies():
                try:
                    payload = json.loads(notify.payload)
                    ev = self._events.get(payload["table"])
                    if ev is not None:
                        ev.set()
                except (json.JSONDecodeError, KeyError):
                    logger.warning("bad notify payload: %r", notify.payload)
        finally:
            await aconn.close()


class ControllerManager:
    def __init__(self, client_state: ClientState) -> None:
        self.client_state = client_state
        self.lease = ManagerLease(client_state.db_sessionmaker)
        self.dispatcher = WakeupDispatcher()
        self._shutdown = asyncio.Event()

    def _build_workers(self) -> list[Worker]:
        return [
            ClusterHealthController(
                "cluster-health", self.client_state, heartbeat_timeout=20
            )
        ]

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._shutdown.set)

        if not await self.lease.acquire():
            logger.error(
                "Could not acquire manager lease; another instance holds it. Exiting."
            )
            return

        logger.info("Manager lease acquired (holder_id=%s)", self.lease.holder_id)

        workers = self._build_workers()

        conninfo = (
            make_url(self.client_state.settings.db_url.get_secret_value())
            .set(drivername="postgresql")
            .render_as_string(hide_password=False)
        )

        tasks: list[asyncio.Task[None]] = [
            asyncio.create_task(w.supervise(self._shutdown), name=w.name)
            for w in workers
        ]
        tasks.append(
            asyncio.create_task(
                self._heartbeat_monitor(workers), name="heartbeat-monitor"
            )
        )
        tasks.append(
            asyncio.create_task(self.lease.run_renewal(), name="lease-renewal")
        )
        tasks.append(
            asyncio.create_task(self.dispatcher.run(conninfo), name="wakeup-dispatcher")
        )

        await self._shutdown.wait()
        logger.info("shutdown requested; cancelling tasks")

        for t in tasks:
            t.cancel()

        try:
            results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True), timeout=10
            )
        except asyncio.TimeoutError:
            logger.warning("tasks did not exit within 10s; forcing")
        else:
            for task, result in zip(tasks, results):
                if isinstance(result, Exception) and not isinstance(
                    result, asyncio.CancelledError
                ):
                    logger.error(
                        "task %s raised during shutdown: %r",
                        task.get_name(),
                        result,
                    )

        try:
            await self.lease.release()
            logger.info("Manager lease released")
        except Exception:
            logger.warning("Failed to release manager lease; it will expire naturally")

    async def _heartbeat_monitor(self, workers: list[Worker]) -> None:
        while not self._shutdown.is_set():
            for worker in workers:
                status = worker.check_heartbeat()
                if status.timed_out and worker.run_task is not None:
                    stale_names = ", ".join(h.name for h in status.stale)
                    msg = (
                        f"Worker {worker.name!r} heartbeat(s) timed out: {stale_names}"
                    )
                    logger.warning(msg)
                    worker.run_task.cancel(msg)
            await asyncio.sleep(5)


async def main() -> None:
    settings = Settings()
    config_logging(settings.log_level)
    logger.info("Initializing controller manager")

    async with settings.build_clients() as client_state:
        manager = ControllerManager(client_state)
        await manager.run()


if __name__ == "__main__":
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    asyncio.run(main())
