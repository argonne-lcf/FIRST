import random
from datetime import datetime, timezone
from typing import NoReturn

from first_common.errors import (
    FirstError,
    NotFound,
    ServiceUnavailable,
    TooManyRequests,
)
from first_common.schema.types import UsageLimits
from first_gateway.apiserver.dependencies import AuthUser

from ..database.redis.admission import (
    AdmissionController,
    AdmitResult,
    AdmitStatus,
    CandidateBackend,
    CapacityReason,
    QuotaReason,
)
from ..database.redis.router_config import DeploymentConfig, ModelConfig


def get_shuffled_backends(
    model: ModelConfig,
    deployment_name: str | None = None,
) -> list[CandidateBackend]:
    """
    Generate list of backend candidates for a given model and deployment (if provided).

    The shuffle order is obtained by weighted random selection (without
    replacement) of backends according to the deployment weight.
    """

    if deployment_name:
        d = next((d for d in model.deployments if d.name == deployment_name), None)
        if d is None:
            raise NotFound(
                f"{deployment_name=} was not found in the current configuration."
            )
        deployments = [d]
    else:
        deployments = model.deployments

    if not deployments:
        return []

    # Sorting by an Exp(weight) draw is equivalent to repeatedly picking from
    # the remaining backends with probability proportional to weight.
    items = sorted(
        (
            (random.expovariate(d.router_params.weight), backend, d.router_params)
            for d in deployments
            if d.router_params.weight > 0
            for backend in d.backends
        ),
        key=lambda item: item[0],
    )

    return [
        CandidateBackend(
            uid=backend.id,
            max_backend_concurrency=router_params.max_backend_concurrency,
            cooldown_threshold=router_params.cooldown_threshold,
        )
        for _, backend, router_params in items
    ]


async def admit_request(
    user: AuthUser,
    model: ModelConfig,
    admission_controller: AdmissionController,
    backend_candidates: list[CandidateBackend],
    request_id: str,
    estimated_tokens: int = 0,
    deployment_name: str | None = None,
    charge_request: bool = True,
) -> str:
    """
    Find and return the backend ID that will serve the request.
    This uses probabilities through the admission controler, which
    takes in to account weights and recent activities.

    Pass `charge_request=False` when retrying a request that was already
    admitted, so it only counts once against the user's RPM.
    """

    usage_limits = model.usage_limits.get_limits(user)

    admit_result = await admission_controller.admit(
        request_id=request_id,
        model_name=model.name,
        user_id=user.id,
        candidates=backend_candidates,
        estimated_tokens=estimated_tokens,
        quota=usage_limits,
        charge_request=charge_request,
    )
    if not admit_result.admitted:
        raise_admit_error(admit_result, usage_limits, model, deployment_name)

    if admit_result.backend_id:
        return admit_result.backend_id
    else:
        raise NotFound(f"Backend not found for model {model.name}.")


def get_deployment_from_backend_id(
    deployments: list[DeploymentConfig],
    backend_id: str,
) -> DeploymentConfig:
    """Return the deployment that includes a specific backend ID."""

    return next(
        d
        for d in deployments
        if any(backend.id == backend_id for backend in d.backends)
    )


def raise_admit_error(
    admit_result: AdmitResult,
    usage_limits: UsageLimits,
    model: ModelConfig,
    deployment_name: str | None = None,
) -> NoReturn:
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
                f"Concurrent requests above {usage_limits.max_user_concurrency}."
                + retry_str,
                retry_after_sec=retry_after_sec,
            )
        elif admit_result.quota_reason == QuotaReason.USER_RPM:
            raise TooManyRequests(
                f"Requests per minute above {usage_limits.rpm}." + retry_str,
                retry_after_sec=retry_after_sec,
            )
        elif admit_result.quota_reason == QuotaReason.USER_TPM:
            raise TooManyRequests(
                f"Tokens per minute above {usage_limits.tpm}." + retry_str,
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
            raise cold_start_error(model, retry_after_sec, retry_str, deployment_name)
        else:
            raise FirstError(
                f"Uncaught reject reason for status {AdmitStatus.REJECT_CAPACITY}: {admit_result.capacity_reason}."
            )

    else:
        raise FirstError(f"Uncaught admit_result status: {admit_result.status}.")


def cold_start_error(
    model: ModelConfig,
    retry_after_sec: int | None,
    retry_str: str,
    deployment_name: str | None = None,
) -> ServiceUnavailable:
    """
    503 for a model with no routable backend, with a startup ETA when known.

    Each pilot deployment carries its own `earliest_placed_at` and
    `last_startup_sec`, so the ETA is never computed by mixing one
    deployment's placement time with another's startup duration. A request
    pinned to one deployment only reports that deployment's outlook.
    """
    now = datetime.now(timezone.utc)
    deployments = [d for d in model.deployments if deployment_name in (None, d.name)]
    etas = [eta for d in deployments if (eta := d.startup_eta_sec(now)) is not None]
    typical = [
        d.last_startup_sec for d in deployments if d.last_startup_sec is not None
    ]

    if etas and min(etas) > 0:
        message = f"Model {model.name} is starting; ~{min(etas):.0f}s until ready."
    elif etas:
        # Already past the last observed startup duration: don't claim "0s".
        message = f"Model {model.name} is starting and should be ready shortly."
    elif any(d.incoming for d in deployments):
        message = f"Model {model.name} is starting."
    elif typical:
        message = f"Model {model.name} is not running; a replica will be started."
    else:
        message = "Backend not available yet."

    if not etas and typical:
        message += f" Startup typically takes ~{min(typical):.0f}s."

    return ServiceUnavailable(
        message + retry_str,
        retry_after_sec=retry_after_sec,
        info={
            "startup_eta_sec": round(min(etas)) if etas else None,
            "typical_startup_sec": round(min(typical)) if typical else None,
        },
    )


def get_name_from_slug(slug: str) -> str:
    return slug.replace("~", "/")


def get_slug_from_name(name: str) -> str:
    return name.replace("/", "~")
