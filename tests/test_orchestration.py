"""Backend candidate ordering for the admission path."""

import random
from collections import Counter
from unittest.mock import AsyncMock, MagicMock

import pytest

from first_common.errors import FirstError, RequestTooLarge
from first_common.schema.types import (
    OverloadPolicy,
    RouterParams,
    UsageLimits,
    UsagePolicy,
)
from first_gateway.database.redis.admission import (
    AdmitResult,
    AdmitStatus,
    CapacityReason,
    QuotaReason,
)
from first_gateway.database.redis.router_config import (
    BackendConfig,
    DeploymentConfig,
    ModelConfig,
)
from first_gateway.services.orchestration import (
    admit_request,
    get_shuffled_backends,
    jittered_retry_after,
    raise_admit_error,
)


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


# --------------------------------------------------------------------------
# Retry-After: rounded up, jittered upward
# --------------------------------------------------------------------------


def test_retry_after_rounds_up_without_jitter() -> None:
    policy = OverloadPolicy(retry_jitter_percent=0)
    assert jittered_retry_after(2.7, policy) == 3
    assert jittered_retry_after(2.01, policy) == 3
    assert jittered_retry_after(0.2, policy) == 1
    assert jittered_retry_after(4.0, policy) == 4


def test_retry_after_falls_back_to_short_retry() -> None:
    policy = OverloadPolicy(short_retry_sec=15, retry_jitter_percent=0)
    assert jittered_retry_after(None, policy) == 15
    assert jittered_retry_after(0.0, policy) == 15
    assert jittered_retry_after(None, OverloadPolicy(short_retry_sec=0)) is None


def test_retry_after_jitter_spreads_but_never_shortens() -> None:
    random.seed(42)
    policy = OverloadPolicy(short_retry_sec=100, retry_jitter_percent=30)
    values = [jittered_retry_after(None, policy) for _ in range(500)]
    assert all(v is not None and 100 <= v <= 130 for v in values)
    # Spread across the window, not a single herd value.
    assert len(set(values)) > 20


# --------------------------------------------------------------------------
# Admission rejection errors
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("result", "reason", "status"),
    [
        (
            AdmitResult(
                status=AdmitStatus.REJECT_QUOTA,
                quota_reason=QuotaReason.USER_TPM,
                retry_after_sec=2.7,
            ),
            "quota:user_tpm",
            429,
        ),
        (
            AdmitResult(
                status=AdmitStatus.REJECT_CAPACITY,
                capacity_reason=CapacityReason.SATURATED,
            ),
            "capacity:saturated",
            503,
        ),
    ],
)
def test_raise_admit_error_tags_reason(
    result: AdmitResult, reason: str, status: int
) -> None:
    model = _model(_dep("a", 1, 1))
    model.overload = OverloadPolicy(retry_jitter_percent=0)
    with pytest.raises(FirstError) as exc_info:
        raise_admit_error(result, UsageLimits(), model)
    exc = exc_info.value
    assert exc.status_code == status
    assert exc.info["reject_reason"] == reason
    expected = 3 if result.retry_after_sec else model.overload.short_retry_sec
    assert exc.retry_after_sec == expected
    assert f"Retry in {expected} seconds." in str(exc)


async def test_request_over_burst_tokens_fails_fast() -> None:
    model = _model(_dep("a", 1, 1))
    model.usage_limits = UsagePolicy(default=UsageLimits(burst_tokens=1000))
    controller = AsyncMock()
    user = MagicMock(id="u", username="u", user_group_uuids=[])

    with pytest.raises(RequestTooLarge) as exc_info:
        await admit_request(user, model, controller, [], "rid", estimated_tokens=1001)
    exc = exc_info.value
    assert exc.status_code == 400
    assert exc.retry_after_sec is None
    assert exc.info["reject_reason"] == "request_too_large"
    assert "1,001" in str(exc) and "1,000" in str(exc)
    controller.admit.assert_not_called()
