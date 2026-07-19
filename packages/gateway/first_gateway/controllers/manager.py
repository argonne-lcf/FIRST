import asyncio
import logging
import signal

import uvloop

from first_gateway.log_config import config_logging

from ..settings import ClientState, Settings
from .lease import ManagerLease
from .metrics_server import serve as serve_metrics
from .wakeup import WakeupDispatcher
from .worker import Worker
from .workers.autoscaler import PilotAutoscaler
from .workers.health_observer import HealthObserver
from .workers.inflight_observer import InflightObserver
from .workers.pilot_job_observer import PilotJobObserver
from .workers.pilot_replica_observer import PilotReplicaObserver
from .workers.replica_drainer import ReplicaDrainer
from .workers.replica_launcher import ReplicaLauncher
from .workers.replica_placement import ReplicaPlacer
from .workers.replica_reconciler import ReplicaReconciler
from .workers.retention import RetentionSweeper
from .workers.router_config_observer import RouterConfigObserver

logger = logging.getLogger("first_gateway.controllers.manager")


class ControllerManager:
    def __init__(self, client_state: ClientState) -> None:
        self.client_state = client_state
        self.lease = ManagerLease(client_state.db_sessionmaker)
        self.dispatcher = WakeupDispatcher(client_state)
        self._shutdown = asyncio.Event()

    def _build_workers(self) -> list[Worker]:
        return [
            HealthObserver("health-observer", self.client_state, self.dispatcher),
            InflightObserver("inflight-observer", self.client_state, self.dispatcher),
            PilotJobObserver("pilot-job-observer", self.client_state, self.dispatcher),
            PilotReplicaObserver(
                "pilot-replica-observer", self.client_state, self.dispatcher
            ),
            PilotAutoscaler("pilot-autoscaler", self.client_state, self.dispatcher),
            ReplicaReconciler("replica-reconciler", self.client_state, self.dispatcher),
            ReplicaPlacer("replica-placer", self.client_state, self.dispatcher),
            ReplicaLauncher("replica-launcher", self.client_state, self.dispatcher),
            ReplicaDrainer("replica-drainer", self.client_state, self.dispatcher),
            RetentionSweeper("retention-sweeper", self.client_state, self.dispatcher),
            RouterConfigObserver("router-config", self.client_state, self.dispatcher),
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
            asyncio.create_task(self.dispatcher.run(), name="wakeup-dispatcher")
        )
        tasks.append(asyncio.create_task(serve_metrics(workers), name="metrics-server"))

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
