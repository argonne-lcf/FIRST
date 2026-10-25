from unittest.mock import AsyncMock, MagicMock

from first_gateway.apiserver.router_config_manager import RouterConfigManager
from first_gateway.database.redis.router_config import RouterConfig


async def test_failed_swap_callback_reports_health_problem() -> None:
    manager = RouterConfigManager(MagicMock())
    cb = AsyncMock(side_effect=[ValueError("env var not set"), None])
    manager.add_swap_callback(cb)

    await manager._swap(RouterConfig(version=1))
    assert manager.current.version == 1
    assert manager.health_problem == "RouterConfig failed to apply"

    # Unchanged version: callbacks are not retried and the problem sticks
    await manager._swap(RouterConfig(version=1))
    assert manager.health_problem == "RouterConfig failed to apply"

    await manager._swap(RouterConfig(version=2))
    assert manager.health_problem is None
