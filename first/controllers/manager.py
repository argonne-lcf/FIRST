import asyncio
import logging
import signal

import uvloop

from first.log_config import config_logging

from .cluster.health import ClusterHealthController
from .worker import Worker

logger = logging.getLogger("first.controllers.manager")


def build_workers() -> list[Worker]:
    return [ClusterHealthController("cluster-health", heartbeat_timeout=20)]


async def heartbeat_monitor(workers: list[Worker], shutdown: asyncio.Event) -> None:
    while not shutdown.is_set():
        for worker in workers:
            status = worker.check_heartbeat()
            if status.timed_out and worker.run_task is not None:
                logger.error(
                    f"Worker {worker.name!r} heartbeat timed out: {status.since_last:.1f}s since last beat."
                )
                worker.run_task.cancel()
        await asyncio.sleep(5)


async def main() -> None:
    workers = build_workers()
    logger.info("Initializing controller manager")
    for worker in workers:
        logger.info(f"Registered worker {worker.name}")

    shutdown = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown.set)

    tasks = [asyncio.create_task(w.supervise(shutdown), name=w.name) for w in workers]
    tasks.append(asyncio.create_task(heartbeat_monitor(workers, shutdown)))

    await shutdown.wait()
    logger.info("shutdown requested; cancelling workers")

    for t in tasks:
        t.cancel()

    try:
        results = await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True), timeout=10
        )
    except asyncio.TimeoutError:
        logger.warning("workers did not exit within 10s; forcing")
    else:
        for w, r in zip(workers, results):
            if isinstance(r, Exception) and not isinstance(r, asyncio.CancelledError):
                logger.error("worker %s raised during shutdown: %r", w.name, r)


if __name__ == "__main__":
    config_logging()
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    asyncio.run(main())
