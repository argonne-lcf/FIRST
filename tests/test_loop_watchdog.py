"""The apiserver exits when its event loop stalls, so Compose restarts it."""

import asyncio
import time

from first_gateway.apiserver.loop_watchdog import LoopWatchdog


async def test_exits_when_loop_is_blocked() -> None:
    exits: list[int] = []
    watchdog = LoopWatchdog(timeout=0.2, interval=0.05, exit=exits.append)
    await watchdog.start()
    time.sleep(0.5)  # block the loop: no heartbeats
    await watchdog.stop()
    assert exits == [1]


async def test_quiet_while_loop_turns() -> None:
    exits: list[int] = []
    watchdog = LoopWatchdog(timeout=0.2, interval=0.05, exit=exits.append)
    await watchdog.start()
    await asyncio.sleep(0.5)
    await watchdog.stop()
    assert exits == []
