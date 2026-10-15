"""Backend candidate ordering for the admission path."""

import random
from collections import Counter

from first_common.schema.types import OverloadPolicy, RouterParams, UsagePolicy
from first_gateway.database.redis.router_config import (
    BackendConfig,
    DeploymentConfig,
    ModelConfig,
)
from first_gateway.services.orchestration import get_shuffled_backends


def _dep(name: str, weight: int, n_backends: int) -> DeploymentConfig:
    return DeploymentConfig(
        kind="pilot",
        name=name,
        cluster_name="c",
        router_params=RouterParams(weight=weight),
        prometheus_metrics_path=None,
        prometheus_scrape_interval_sec=30,
        backends=[
            BackendConfig(
                id=f"{name}-{i}",
                model_url="http://backend",
                backend_model_name="m",
                api_key=None,
            )
            for i in range(n_backends)
        ],
    )


def _model(*deployments: DeploymentConfig) -> ModelConfig:
    return ModelConfig(
        name="open/model",
        aliases=[],
        allowed_groups=["all"],
        allowed_domains=[],
        supported_endpoints=["chat"],
        usage_limits=UsagePolicy(),
        overload=OverloadPolicy(),
        deployments=list(deployments),
    )


def test_shuffle_returns_each_weighted_backend_once() -> None:
    model = _model(_dep("a", 1, 2), _dep("b", 3, 2), _dep("off", 0, 2))
    uids = [c.uid for c in get_shuffled_backends(model)]
    assert sorted(uids) == ["a-0", "a-1", "b-0", "b-1"]


def test_shuffle_handles_no_candidates() -> None:
    assert get_shuffled_backends(_model()) == []
    assert get_shuffled_backends(_model(_dep("off", 0, 2))) == []


def test_shuffle_first_pick_follows_weights() -> None:
    """A backend leads the order in proportion to its deployment's weight."""
    random.seed(1234)
    model = _model(_dep("light", 1, 1), _dep("heavy", 9, 1))
    n = 4000
    first = Counter(get_shuffled_backends(model)[0].uid for _ in range(n))
    assert abs(first["heavy-0"] / n - 0.9) < 0.03


def test_shuffle_pinned_deployment() -> None:
    model = _model(_dep("a", 1, 2), _dep("b", 3, 2))
    uids = [c.uid for c in get_shuffled_backends(model, deployment_name="a")]
    assert sorted(uids) == ["a-0", "a-1"]
