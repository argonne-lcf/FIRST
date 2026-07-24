import numpy as np

from first_common.errors import (
    InternalServerError,
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


async def get_backend(
    user: AuthUser,
    model: ModelConfig,
    admission_controler: AdmissionController,
    deployment_name: str | None = None,
) -> BackendConfig:
    """
    Find and return the backend that will serve the request.
    This uses probabilities through the admission controler, which
    takes in to account weights and recent activities.
    """

    backend_id = await get_backend_id(user, model, admission_controler, deployment_name)
    return next(
        (
            backend
            for d in model.deployments
            for backend in d.backends
            if backend.id == backend_id
        ),
        None,
    )


async def get_backend_id(
    user: AuthUser,
    model: ModelConfig,
    admission_controler: AdmissionController,
    deployment_name: str | None = None,
) -> str:
    """
    Return the ID of the backend that will serve the request.
    This uses probabilities through the admission controler, which
    takes in to account weights and recent activities.
    """

    if deployment_name:
        d = next((d for d in model.deployments if d.name == deployment_name), None)
        if d is None:
            raise NotFound(f"Deployment {deployment_name} does not exist.")
        deployments = [d]
    else:
        deployments = model.deployments

    backend_candidates = await get_candidates_from_deployments(deployments)

    usage_limits = await get_usage_limits(user, model.usage_limits)

    # TODO: find where the request id is
    # TODO: calculate estimated_tokens
    admit_result = await admission_controler.admit(
        request_id="temporary",
        model_name=model.name,
        user_id=user.id,
        candidates=backend_candidates,
        estimated_tokens=1,
        quota=usage_limits,
    )

    if admit_result.admitted:
        return admit_result.backend_id
    else:
        raise_admit_error(admit_result, usage_limits)


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
            uid=backends[i].uid,
            max_backend_concurrency=router_params[i].max_backend_concurrency,
            cooldown_threshold=router_params[i].cooldown_threshold,
        )
        for i in idx
    ]


async def get_usage_limits(user: AuthUser, usage_policy: UsagePolicy) -> UsageLimits:
    # TODO: check groups too, but what happen if more than 1 user groups are in overrides?
    if user.id in usage_policy.overrides:
        return usage_policy.overrides[user.id]
    else:
        return usage_policy.default


def raise_admit_error(admit_result: AdmitResult, usage_limits: UsageLimits) -> None:
    if admit_result.status == AdmitStatus.REJECT_QUOTA:
        if admit_result.quota_reason == QuotaReason.USER_CONCURRENCY:
            raise TooManyRequests(
                f"Concurent requests above f{usage_limits.max_user_concurrency}."
            )
        elif admit_result.quota_reason == QuotaReason.USER_RPM:
            raise TooManyRequests(f"Requests per minute above f{usage_limits.rpm}.")
        elif admit_result.quota_reason == QuotaReason.USER_TPM:
            raise TooManyRequests(f"Tokens per minute above f{usage_limits.tpm}.")
        else:
            raise InternalServerError(
                f"Uncaught reject reason for status {AdmitStatus.REJECT_QUOTA}: {admit_result.quota_reason}."
            )

    elif admit_result.status == AdmitStatus.REJECT_CAPACITY:
        if admit_result.capacity_reason == CapacityReason.SATURATED:
            raise ServiceUnavailable(
                f"Backend saturated. Retry in {admit_result.retry_after_sec} seconds."
            )
        elif admit_result.capacity_reason == CapacityReason.ALL_BENCHED:
            raise ServiceUnavailable(
                f"Backend in cooldown phase. Retry in {admit_result.retry_after_sec} seconds."
            )
        elif admit_result.capacity_reason == CapacityReason.NO_CANDIDATES:
            raise ServiceUnavailable(
                f"Backend not available yet. Retry in {admit_result.retry_after_sec} seconds."
            )
        else:
            raise InternalServerError(
                f"Uncaught reject reason for status {AdmitStatus.REJECT_CAPACITY}: {admit_result.capacity_reason}."
            )

    else:
        raise InternalServerError(
            f"Uncaught admit_result status: {admit_result.status}."
        )
