import json

from httpx import HTTPError

from first.errors import ClusterStatusCheckError
from first.http_client import aclient
from first.schema.types import ClusterStatus


async def get_alcf_cluster_status(status_url: str, timeout: int) -> ClusterStatus:
    """
    ALCF IRI API Cluster Status Check
    """

    try:
        resp = await aclient.get(status_url, timeout=timeout)
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


async def get_metis_cluster_status(status_url: str, timeout: int) -> ClusterStatus:
    try:
        resp = await aclient.get(status_url, timeout=timeout)
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
