import asyncio

import pytest

from first_gateway.apiserver import backend_client_manager as bcm
from first_gateway.database.redis.router_config import (
    BackendConfig,
    DeploymentConfig,
    ModelConfig,
    RouterConfig,
)
from first_gateway.settings import Settings


def _config(model_url: str) -> RouterConfig:
    return RouterConfig(
        version=1,
        models=[
            ModelConfig(
                name="m",
                aliases=[],
                allowed_groups=[],
                allowed_domains=[],
                supported_endpoints=["chat/completions"],
                usage_limits={},  # type: ignore[arg-type]
                overload={},  # type: ignore[arg-type]
                deployments=[
                    DeploymentConfig(
                        kind="static",
                        name="dep",
                        cluster_name="test-cluster",
                        router_params={},  # type: ignore[arg-type]
                        prometheus_metrics_path=None,
                        prometheus_scrape_interval_sec=30,
                        backends=[
                            BackendConfig(
                                id="b1",
                                model_url=model_url,
                                backend_model_name="upstream",
                                api_key=None,
                            )
                        ],
                    )
                ],
            )
        ],
    )


async def test_recreated_client_stays_open_for_grace_period(
    pilot_mtls_settings: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(bcm, "CLOSE_GRACE_SEC", 0)
    manager = bcm.BackendClientManager(Settings())
    try:
        await manager.on_config_swap(_config("http://old/v1"))
        old = manager.get("b1")
        assert old is not None

        await manager.on_config_swap(_config("http://new/v1"))
        new = manager.get("b1")
        assert new is not None and new is not old
        assert str(new.base_url) == "http://new/v1/"
        # In-flight requests holding the old client can still finish
        closed_on_swap = old.is_closed
        assert not closed_on_swap

        await asyncio.gather(*bcm._background_tasks)
        assert old.is_closed
        assert not new.is_closed
    finally:
        await manager.close_all()
