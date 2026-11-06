"""Cluster/model retirement guidance independent of deployment existence."""

from dataclasses import dataclass
from http import HTTPStatus

from first_common.errors import FirstError
from first_common.schema.auth import UserAuthEvent

from ..database.redis.router_config import ModelConfig
from .auth import enforce_permission


class ModelRetired(FirstError):
    status_code = HTTPStatus.GONE
    code = "model_retired"


@dataclass
class ModelRetirement:
    message: str
    replacement_cluster: str
    replacement_model: str
    replacement_api_url: str
    # Retain the old access policy even if the last deployment/model disappears.
    allowed_groups: list[str]
    allowed_domains: list[str]


MODEL_RETIREMENTS: dict[tuple[str, str], ModelRetirement] = {
    ("minerva", "inkling-bf16"): ModelRetirement(
        message="Inkling has been retired on Minerva. Use inkling-bf16 on Tara.",
        replacement_cluster="tara",
        replacement_model="inkling-bf16",
        replacement_api_url=(
            "https://inference-api.alcf.anl.gov/resource_server/tara/api/v1"
        ),
        allowed_groups=[],
        allowed_domains=[],
    ),
}


def raise_if_retired(
    user: UserAuthEvent,
    cluster: str,
    requested_model: str,
    model: ModelConfig | None,
) -> None:
    """Global auth runs first; keep model ACLs without requiring a deployment."""
    name = model.name if model is not None else requested_model
    retirement = MODEL_RETIREMENTS.get((cluster, name))
    if retirement is None:
        return
    enforce_permission(user, retirement)
    if model is not None:
        enforce_permission(user, model)
    raise ModelRetired(
        retirement.message,
        info={
            "replacement_cluster": retirement.replacement_cluster,
            "replacement_model": retirement.replacement_model,
            "replacement_api_url": retirement.replacement_api_url,
        },
    )
