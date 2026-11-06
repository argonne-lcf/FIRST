"""Migration guidance, not endpoints: keep entries after fixture tombstoning."""

from dataclasses import dataclass
from http import HTTPStatus

from .auth import check_permission
from .errors import BaseError, ClusterNotFound
from .models import Cluster
from .schemas.structured_logs import UserPydantic


class ModelRetired(BaseError):
    status_code = HTTPStatus.GONE
    code = "model_retired"


@dataclass(frozen=True)
class ModelRetirement:
    message: str
    replacement_cluster: str
    replacement_model: str
    replacement_api_url: str
    # Explicitly retain the retired endpoint's ACL, not the tombstone's ACL.
    # Authentication and the live cluster's ACL are enforced separately.
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


async def raise_if_retired(cluster: str, model: str, user: UserPydantic) -> None:
    """Check authorization before guidance; never resolve or contact a backend."""
    retirement = MODEL_RETIREMENTS.get((cluster, model))
    if retirement is None:
        return
    # Read only the cluster ACL. Initializing an adapter would unnecessarily
    # require the retired backend's certificates/configuration to remain usable.
    try:
        source = await Cluster.objects.aget(cluster_name=cluster)
    except Cluster.DoesNotExist:
        raise ClusterNotFound(
            f"The requested cluster {cluster!r} does not exist."
        ) from None
    check_permission(user, source.allowed_globus_groups, source.allowed_domains)
    check_permission(user, retirement.allowed_groups, retirement.allowed_domains)
    raise ModelRetired(
        retirement.message,
        info={
            "replacement_cluster": retirement.replacement_cluster,
            "replacement_model": retirement.replacement_model,
            "replacement_api_url": retirement.replacement_api_url,
        },
    )
