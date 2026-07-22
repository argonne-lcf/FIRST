"""Tests for the Prometheus HTTP SD discovery endpoint."""

from first_common.schema.types import OverloadPolicy, RouterParams, UsagePolicy
from first_gateway.apiserver.routes.discovery import prometheus_service_discovery
from first_gateway.database.redis.router_config import (
    BackendConfig,
    DeploymentConfig,
    ModelConfig,
    RouterConfig,
)


def _model(name: str, deployments: list[DeploymentConfig]) -> ModelConfig:
    return ModelConfig(
        name=name,
        aliases=[],
        allowed_groups=["all"],
        allowed_domains=[],
        supported_endpoints=["chat"],
        usage_limits=UsagePolicy(),
        overload=OverloadPolicy(),
        deployments=deployments,
    )


def _deployment(
    name: str,
    *,
    metrics_path: str | None,
    interval: int = 30,
    backends: list[BackendConfig],
) -> DeploymentConfig:
    return DeploymentConfig(
        kind="static",
        name=name,
        router_params=RouterParams(),
        prometheus_metrics_path=metrics_path,
        prometheus_scrape_interval_sec=interval,
        backends=backends,
    )


def _backend(id: str, url: str) -> BackendConfig:
    return BackendConfig(id=id, model_url=url, backend_model_name="m", api_key=None)


async def test_empty_config_returns_empty_list() -> None:
    assert await prometheus_service_discovery(RouterConfig()) == []


async def test_deployment_without_metrics_path_is_skipped() -> None:
    config = RouterConfig(
        models=[
            _model(
                "m1",
                [
                    _deployment(
                        "d1",
                        metrics_path=None,
                        backends=[_backend("b1", "http://host:8080/v1")],
                    )
                ],
            )
        ]
    )
    assert await prometheus_service_discovery(config) == []


async def test_deployment_without_backends_is_skipped() -> None:
    config = RouterConfig(
        models=[_model("m1", [_deployment("d1", metrics_path="/metrics", backends=[])])]
    )
    assert await prometheus_service_discovery(config) == []


async def test_target_labels_and_url_decomposition() -> None:
    config = RouterConfig(
        models=[
            _model(
                "llama-70b",
                [
                    _deployment(
                        "dep-1",
                        metrics_path="/metrics",
                        interval=45,
                        backends=[_backend("pilot_replica/7", "http://host:8080/v1")],
                    )
                ],
            )
        ]
    )
    targets = await prometheus_service_discovery(config)
    assert len(targets) == 1
    t = targets[0]
    assert t.targets == ["host:8080"]
    assert t.labels == {
        "__scheme__": "http",
        "__metrics_path__": "/v1/metrics",
        "__scrape_interval__": "45s",
        "model": "llama-70b",
        "deployment": "dep-1",
        "instance": "pilot_replica/7",
    }


async def test_metrics_path_leading_slash_is_stripped_once() -> None:
    config = RouterConfig(
        models=[
            _model(
                "m1",
                [
                    _deployment(
                        "d1",
                        metrics_path="metrics",  # no leading slash
                        backends=[_backend("b1", "http://host:9000")],
                    )
                ],
            )
        ]
    )
    targets = await prometheus_service_discovery(config)
    assert targets[0].targets == ["host:9000"]
    assert targets[0].labels["__metrics_path__"] == "/metrics"


async def test_duplicate_metrics_urls_are_deduplicated() -> None:
    shared = _backend("b1", "http://host:8080/v1")
    config = RouterConfig(
        models=[
            _model(
                "m1",
                [
                    _deployment("d1", metrics_path="/metrics", backends=[shared]),
                    _deployment("d2", metrics_path="/metrics", backends=[shared]),
                ],
            )
        ]
    )
    targets = await prometheus_service_discovery(config)
    assert len(targets) == 1
