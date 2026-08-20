import numpy as np

from first_common.errors import (
    FirstError,
    NotFound,
    ServiceUnavailable,
    TooManyRequests,
)
from first_common.schema.types import UsageLimits, UsagePolicy
from first_gateway.apiserver.dependencies import AuthUser

from ..database.redis.admission import (
    AdmissionController,
    AdmitResult,
    AdmitStatus,
    CandidateBackend,
    CapacityReason,
    QuotaReason,
)
from ..database.redis.router_config import BackendConfig, DeploymentConfig, ModelConfig


def get_backend_candidates(
    model: ModelConfig,
    deployment_name: str | None = None,
) -> list[CandidateBackend]:
    """Generate list of backend candidates for a given model and deployment (if provided)."""

    if deployment_name:
        d = next((d for d in model.deployments if d.name == deployment_name))
        deployments = [d]
    else:
        deployments = model.deployments

    return get_candidates_from_deployments(deployments)


def get_candidates_from_deployments(
    deployments: list[DeploymentConfig],
) -> list[CandidateBackend]:
    if not deployments:
        raise NotFound("Attempt to sort models with empty list of deployments.")

    items = [
        (backend, d.router_params.weight, d.router_params)
        for d in deployments
        if d.router_params.weight > 0
        for backend in d.backends
    ]
    backends, weights, router_params = zip(*items)
    nb_backends = len(backends)

    total = sum(weights)
    probabilities = [weight / total for weight in weights]
    idx = np.random.choice(
        nb_backends, size=nb_backends, replace=False, p=probabilities
    )

    return [
        CandidateBackend(
            uid=backends[i].id,
            max_backend_concurrency=router_params[i].max_backend_concurrency,
            cooldown_threshold=router_params[i].cooldown_threshold,
        )
        for i in idx
    ]


async def get_backend(
    user: AuthUser,
    model: ModelConfig,
    admission_controller: AdmissionController,
    backend_candidates: list[CandidateBackend],
    estimated_tokens: int | None = 0,
) -> BackendConfig:
    """
    Find and return the backend that will serve the request.
    This uses probabilities through the admission controler, which
    takes in to account weights and recent activities.
    """

    usage_limits = get_usage_limits(user, model.usage_limits)

    # TODO: Incorporate request id
    admit_result = await admission_controller.admit(
        request_id="temporary-request-id",
        model_name=model.name,
        user_id=user.id,
        candidates=backend_candidates,
        estimated_tokens=estimated_tokens,
        quota=usage_limits,
    )
    if not admit_result.admitted:
        raise_admit_error(admit_result, usage_limits, model)

    return next(
        (
            backend
            for d in model.deployments
            for backend in d.backends
            if backend.id == admit_result.backend_id
        )
    )


def get_deployment_from_backend(
    deployments: list[DeploymentConfig],
    backend: BackendConfig,
) -> DeploymentConfig:
    """Return the deployment that includes a specific backend."""

    deployment = next((d for d in deployments if backend in d.backends))

    return deployment


def get_usage_limits(user: AuthUser, usage_policy: UsagePolicy) -> UsageLimits:
    # TODO: check groups too, but what happen if more than 1 user groups are in overrides?
    if user.id in usage_policy.overrides:
        return usage_policy.overrides[user.id]
    else:
        return usage_policy.default


def raise_admit_error(
    admit_result: AdmitResult,
    usage_limits: UsageLimits,
    model: ModelConfig,
) -> None:
    retry_after_sec: int | None
    if admit_result.retry_after_sec:
        retry_after_sec = int(admit_result.retry_after_sec)
        retry_str = f" Retry in {retry_after_sec} seconds."
    elif model.overload.short_retry_sec:
        retry_after_sec = int(model.overload.short_retry_sec)
        retry_str = f" Retry in {retry_after_sec} seconds."
    else:
        retry_after_sec = None
        retry_str = ""

    if admit_result.status == AdmitStatus.REJECT_QUOTA:
        if admit_result.quota_reason == QuotaReason.USER_CONCURRENCY:
            raise TooManyRequests(
                f"Concurent requests above f{usage_limits.max_user_concurrency}."
                + retry_str,
                retry_after_sec=retry_after_sec,
            )
        elif admit_result.quota_reason == QuotaReason.USER_RPM:
            raise TooManyRequests(
                f"Requests per minute above f{usage_limits.rpm}." + retry_str,
                retry_after_sec=retry_after_sec,
            )
        elif admit_result.quota_reason == QuotaReason.USER_TPM:
            raise TooManyRequests(
                f"Tokens per minute above f{usage_limits.tpm}." + retry_str,
                retry_after_sec=retry_after_sec,
            )
        else:
            raise FirstError(
                f"Uncaught reject reason for status {AdmitStatus.REJECT_QUOTA}: {admit_result.quota_reason}."
            )

    elif admit_result.status == AdmitStatus.REJECT_CAPACITY:
        if admit_result.capacity_reason == CapacityReason.SATURATED:
            raise ServiceUnavailable(
                "Backend saturated." + retry_str,
                retry_after_sec=retry_after_sec,
            )
        elif admit_result.capacity_reason == CapacityReason.ALL_BENCHED:
            raise ServiceUnavailable(
                "Backend in cooldown phase." + retry_str,
                retry_after_sec=retry_after_sec,
            )
        elif admit_result.capacity_reason == CapacityReason.NO_CANDIDATES:
            raise ServiceUnavailable(
                "Backend not available yet." + retry_str,
                retry_after_sec=retry_after_sec,
            )
        else:
            raise FirstError(
                f"Uncaught reject reason for status {AdmitStatus.REJECT_CAPACITY}: {admit_result.capacity_reason}."
            )

    else:
        raise FirstError(f"Uncaught admit_result status: {admit_result.status}.")


def get_name_from_slug(slug: str) -> str:
    return slug.replace("~", "/")


def get_slug_from_name(name: str) -> str:
    return name.replace("/", "~")
