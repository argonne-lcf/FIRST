"""V2 catalog tombstones for V1 callers, not fake serving endpoints."""

from http import HTTPStatus
from urllib.parse import urlsplit

from .auth import check_permission
from .errors import BaseError, Unauthorized
from .first_v2_bridge.router_config import ModelRetirementConfig, RetiredDeployment
from .models import Cluster, Endpoint
from .schemas.structured_logs import UserPydantic


class ModelRetired(BaseError):
    status_code = HTTPStatus.GONE
    code = "model_retired"


class DeploymentRetired(BaseError):
    status_code = HTTPStatus.GONE
    code = "deployment_retired"


def _retired_on_cluster(retirement: ModelRetirementConfig, cluster: str) -> bool:
    return retirement.retired or (
        any(d.cluster_name == cluster for d in retirement.retired_deployments)
        and not any(d.cluster_name == cluster for d in retirement.active_deployments)
    )


async def retired_model_names(cluster_name: str) -> set[str]:
    """Hide tombstones before adapter lookup, including during fixture rollout."""
    try:
        cluster = await Cluster.objects.aget(cluster_name=cluster_name)
    except Cluster.DoesNotExist:
        return set()
    return {
        name
        for raw in cluster.model_retirements
        if _retired_on_cluster(
            entry := ModelRetirementConfig.model_validate(raw), cluster_name
        )
        for name in (entry.name, *entry.aliases)
    }


async def _alternatives(
    retirement: ModelRetirementConfig,
    user: UserPydantic,
    retired: RetiredDeployment,
) -> list[dict[str, str]]:
    """Only advertise V1 routes the caller can actually access; no adapters.

    Healthy bridge rows come and go independently of retirement metadata. A
    temporarily cold/unavailable replacement must not turn a retirement into
    a 404 or require contacting any backend to construct the error.
    """
    clusters = {c.cluster_name: c async for c in Cluster.objects.all()}
    replacement = retired.replacement
    if replacement and retirement.replacement_deployments is not None:
        # Do not require a fabricated serving row for a cold PilotDeployment.
        # The bridge mirrors the target Model ACL independently of backend health.
        alternatives = {}
        path = urlsplit(replacement.base_url).path
        for target in retirement.replacement_deployments:
            if (
                target.cluster_name != replacement.cluster_name
                or target.model_name != replacement.model_name
            ):
                continue
            cluster = clusters.get(target.cluster_name)
            if cluster is None or not any(
                path == f"/resource_server/{target.cluster_name}/{framework}/v1"
                for framework in cluster.frameworks
            ):
                continue
            try:
                check_permission(
                    user, cluster.allowed_globus_groups, cluster.allowed_domains
                )
                check_permission(user, target.allowed_groups, target.allowed_domains)
                async for endpoint in Endpoint.objects.filter(
                    cluster=target.cluster_name, model=target.model_name
                ):
                    check_permission(
                        user, endpoint.allowed_globus_groups, endpoint.allowed_domains
                    )
            except Unauthorized:
                continue
            alternatives[target.name] = {
                "deployment": target.name,
                "cluster": target.cluster_name,
                "model": target.model_name,
                "base_path": path,
                "base_url": replacement.base_url,
            }
        return [alternatives[name] for name in sorted(alternatives)]
    active = {
        d.cluster_name: d.name
        for d in sorted(
            retirement.active_deployments, key=lambda d: d.name, reverse=True
        )
    }
    retired_clusters = {
        d.cluster_name for d in retirement.retired_deployments
    } - active.keys()
    target_model = replacement.model_name if replacement else retirement.name
    endpoints = Endpoint.objects.filter(model=target_model)
    if replacement:
        endpoints = endpoints.filter(cluster=replacement.cluster_name)
    alternatives = {}
    async for endpoint in endpoints.order_by("cluster", "framework", "endpoint_slug"):
        cluster = clusters.get(endpoint.cluster)
        if cluster is None or (
            not replacement and endpoint.cluster in retired_clusters
        ):
            continue
        try:
            check_permission(
                user, cluster.allowed_globus_groups, cluster.allowed_domains
            )
            check_permission(
                user, endpoint.allowed_globus_groups, endpoint.allowed_domains
            )
        except Unauthorized:
            continue
        path = f"/resource_server/{endpoint.cluster}/{endpoint.framework}/v1"
        alternatives[path] = {
            "deployment": active.get(endpoint.cluster, endpoint.endpoint_slug),
            "cluster": endpoint.cluster,
            "base_path": path,
        }
        if replacement:
            alternatives[path].update(
                model=target_model,
                base_url=replacement.base_url,
                deployment=endpoint.endpoint_slug,
            )
    return [alternatives[path] for path in sorted(alternatives)]


async def raise_if_retired(
    cluster_name: str, framework: str, model_name: str, user: UserPydantic
) -> None:
    """Authorize before 410; never resolve, dispatch, redirect or replay."""
    try:
        cluster = await Cluster.objects.aget(cluster_name=cluster_name)
    except Cluster.DoesNotExist:
        return  # Keep the normal cluster-not-found path unchanged.
    for raw in cluster.model_retirements:
        retirement = ModelRetirementConfig.model_validate(raw)
        if model_name == retirement.name or model_name in retirement.aliases:
            break
    else:
        return
    retired = next(
        (d for d in retirement.retired_deployments if d.cluster_name == cluster_name),
        None,
    )
    if not _retired_on_cluster(retirement, cluster_name):
        return
    check_permission(user, cluster.allowed_globus_groups, cluster.allowed_domains)
    check_permission(user, retirement.allowed_groups, retirement.allowed_domains)
    if retirement.retired:
        raise ModelRetired(
            f"Model '{retirement.name}' has been retired and is no longer available. "
            "Please switch to a different model; GET .../resource_server/list-endpoints "
            "lists the models available to you.",
            info={"model": retirement.name},
        )
    assert retired is not None
    alternatives = await _alternatives(retirement, user, retired)
    subject = f"Model '{retirement.name}' on cluster '{cluster_name}'"
    if retired.replacement and alternatives:
        replacement = retired.replacement
        message = (
            f"{subject} has been retired. Change both your client's model to "
            f"'{replacement.model_name}' on cluster '{replacement.cluster_name}' "
            f"and its base URL to {replacement.base_url}. "
            "Requests are not redirected or replayed."
        )
    elif alternatives:
        paths = ", ".join(f"...{a['base_path']}" for a in alternatives)
        message = (
            f"{subject} has been retired, but model '{retirement.name}' is still "
            "available elsewhere. Change your client's base URL from "
            f".../resource_server/{cluster_name}/{framework}/v1 to one of: {paths}."
        )
    else:
        message = (
            f"{subject} has been retired, and no alternative deployment is currently "
            "listed for you. GET .../resource_server/list-endpoints lists the models "
            "available to you."
        )
    info = {
        "model": retirement.name,
        "retired_deployment": retired.name,
        "retired_cluster": retired.cluster_name,
        "alternatives": alternatives,
    }
    if retired.replacement and alternatives:
        info.update(
            replacement_cluster=retired.replacement.cluster_name,
            replacement_model=retired.replacement.model_name,
            replacement_base_url=retired.replacement.base_url,
        )
    raise DeploymentRetired(message, info=info)
