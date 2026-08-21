import asyncio
import logging
from collections.abc import Awaitable, Callable

from redis.asyncio import Redis

from ..database.redis.router_config import RouterConfig

logger = logging.getLogger(__name__)

SwapCallback = Callable[[RouterConfig], Awaitable[None]]


class RouterConfigManager:
    """Maintains a hot-swapped RouterConfig snapshot for the apiserver.

    A single supervising task keeps ``self._current`` pointed at the latest
    RouterConfig, driven by both pub/sub notifications and a periodic poll
    fallback. Routes read ``.current`` and hold their own reference for the life
    of the request; a mid-request swap only rebinds the attribute, so the old
    instance stays alive until its last reader drops it and is then GC'd.

    Swap callbacks registered via ``add_swap_callback`` are called after every
    successful config swap, in registration order. In particular, this is used
    to manage the httpx AsyncClient of each healthy backend.
    """

    POLL_INTERVAL_S = 30.0
    SUBSCRIBE_RETRY_S = 1.0

    def __init__(self, redis: Redis) -> None:
        self._redis = redis
        self._current = RouterConfig()
        self._task: asyncio.Task[None] | None = None
        self._swap_callbacks: list[SwapCallback] = []

    def add_swap_callback(self, cb: SwapCallback) -> None:
        # Register async callback functions to be executed after _swap
        self._swap_callbacks.append(cb)

    @property
    def current(self) -> RouterConfig:
        return self._current

    async def _swap(self, cfg: RouterConfig) -> None:
        # Both drivers read Redis independently; guard against a slow in-flight
        # load overwriting a newer config that already landed.
        if cfg.version >= self._current.version:
            self._current = cfg
            for cb in self._swap_callbacks:
                try:
                    await cb(cfg)
                except Exception:
                    # TODO: Find a better way to handle errors here (might need to raise exception)
                    logger.warning(
                        "RouterConfigManager swap callback failed", exc_info=True
                    )

    async def start(self) -> None:
        initial = await RouterConfig.load(self._redis)
        await self._swap(initial)
        self._task = asyncio.create_task(self._run(), name="router-config-manager")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    async def _run(self) -> None:
        await asyncio.gather(self._subscribe_loop(), self._poll_loop())

    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self.POLL_INTERVAL_S)
            try:
                await self._swap(await RouterConfig.load(self._redis))
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("RouterConfig poll failed", exc_info=True)

    async def _subscribe_loop(self) -> None:
        while True:
            try:
                async for cfg in RouterConfig.subscribe(self._redis):
                    await self._swap(cfg)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning(
                    "RouterConfig subscribe dropped; retrying", exc_info=True
                )
                await asyncio.sleep(self.SUBSCRIBE_RETRY_S)
