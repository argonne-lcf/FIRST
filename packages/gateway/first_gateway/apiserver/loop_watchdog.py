"""Exit the process when its event loop stops turning.

Each gateway container runs one uvicorn process, and Compose restarts it only
when it exits, not when it hangs.  Gunicorn's ``--timeout`` heartbeat used to
kill a worker whose loop was blocked; this replaces it.  A coroutine on the
loop stamps a heartbeat every ``interval`` seconds; a thread off the loop
checks the stamp and calls ``os._exit(1)`` once it is ``timeout`` seconds old,
so the restart policy brings a fresh process back.  nginx's ``max_fails``
routes around the replica meanwhile.
"""

import asyncio
import os
import sys
import threading
import time
from typing import Callable


class LoopWatchdog:
    def __init__(
        self,
        timeout: float = 40.0,
        interval: float = 2.0,
        exit: Callable[[int], object] = os._exit,
    ) -> None:
        self.timeout = timeout
        self.interval = interval
        self._exit = exit
        self._last_beat = time.monotonic()
        self._stopped = threading.Event()
        self._beat_task: asyncio.Task[None] | None = None
        self._thread: threading.Thread | None = None

    async def start(self) -> None:
        self._last_beat = time.monotonic()
        self._beat_task = asyncio.create_task(self._beat(), name="loop-watchdog")
        self._thread = threading.Thread(
            target=self._watch, name="loop-watchdog", daemon=True
        )
        self._thread.start()

    async def stop(self) -> None:
        self._stopped.set()
        if self._beat_task is not None:
            self._beat_task.cancel()
            try:
                await self._beat_task
            except asyncio.CancelledError:
                pass
        if self._thread is not None:
            self._thread.join()

    async def _beat(self) -> None:
        while True:
            self._last_beat = time.monotonic()
            await asyncio.sleep(self.interval)

    def _watch(self) -> None:
        while not self._stopped.wait(self.interval):
            stalled = time.monotonic() - self._last_beat
            if stalled > self.timeout:
                # Straight to stderr: the logging queue's listener may never
                # get to run before the process is gone.
                print(
                    f"loop watchdog: event loop stalled for {stalled:.1f}s "
                    f"(limit {self.timeout:.0f}s); exiting so the container restarts",
                    file=sys.stderr,
                    flush=True,
                )
                self._exit(1)
                return
