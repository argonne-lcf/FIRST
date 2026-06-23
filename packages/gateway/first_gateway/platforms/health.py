import json
from http import HTTPMethod

from httpx import AsyncClient, HTTPError

from first_common.errors import ClusterStatusCheckError
from first_common.schema.types import ClusterStatus, HealthEndpointStatus


async def get_alcf_cluster_status(
    client: AsyncClient, status_url: str, timeout: int
) -> ClusterStatus:
    """
    ALCF IRI API Cluster Status Check
    """

    try:
        resp = await client.get(status_url, timeout=timeout)
        resp.raise_for_status()
    except HTTPError as e:
        raise ClusterStatusCheckError(f"{e.request.url} - {e}")

    try:
        status_info = resp.json()
    except json.JSONDecodeError:
        raise ClusterStatusCheckError(
            f"{status_url} - response was not valid JSON: {resp.content[:256]!r}"
        )

    current_status = str(status_info.get("current_status", "unknown")).lower()
    return {
        "up": ClusterStatus.up,
        "down": ClusterStatus.down,
        "degraded": ClusterStatus.degraded,
        "maintenance": ClusterStatus.maintenance,
        "unknown": ClusterStatus.unknown,
    }.get(current_status, ClusterStatus.unknown)


async def get_metis_cluster_status(
    client: AsyncClient, status_url: str, timeout: int
) -> ClusterStatus:
    """
    Metis-specific cluster status introspection
    """
    try:
        resp = await client.get(status_url, timeout=timeout)
        resp.raise_for_status()
    except HTTPError as e:
        raise ClusterStatusCheckError(f"{e.request.url} - {e}")

    try:
        status_info = resp.json()
    except json.JSONDecodeError:
        raise ClusterStatusCheckError(
            f"{status_url} response was not valid JSON: {resp.content[:256]!r}"
        )

    if isinstance(status_info, dict):
        model_stats = [
            str(model.get("status", "")).lower()
            for model in status_info.values()
            if isinstance(model, dict)
        ]
    else:
        model_stats = []

    if "live" in model_stats:
        return ClusterStatus.up
    else:
        return ClusterStatus.down


async def check_health_endpoint(
    httpx_client: AsyncClient,
    base_url: str,
    health_path: str,
    timeout: int,
    headers: dict[str, str] | None = None,
    method: HTTPMethod = HTTPMethod.GET,
) -> HealthEndpointStatus:
    """
    Check http(s) health endpoint
    """
    try:
        resp = await httpx_client.request(
            method,
            f"{base_url}/{health_path.strip('/')}",
            timeout=timeout,
            headers=headers,
        )
    except HTTPError:
        return HealthEndpointStatus.unhealthy

    if 200 <= resp.status_code < 300:
        return HealthEndpointStatus.healthy
    else:
        return HealthEndpointStatus.unhealthy
