import logging

from django.http import HttpRequest
from ninja import Router

from ..clusters import BaseCluster
from ..models import Cluster
from ..schemas import ListEndpointsResponse
from ..schemas.auth import AuthedRequest
from ..schemas.clusters import JobInfo, JobsByStatus
from ..schemas.structured_logs import (
    UserPydantic,
)
from ..services import (
    filter_jobs_for_user,
    get_list_endpoints_data,
)

router = Router()
log = logging.getLogger(__name__)


# Health Check (GET) - No authentication required
# Lightweight endpoint for Kubernetes/load balancer health checks
@router.get("/health", auth=None)
async def health_check(request: HttpRequest) -> dict[str, str]:
    """Lightweight health check endpoint - returns OK if API is responding."""
    return {"status": "ok"}


# Status Check (GET) - No authentication required
@router.get("/status", auth=None)
async def status_check(request: HttpRequest) -> dict[str, bool]:
    """Status check of publicly-available clusters - True if up, False if down."""

    # Mock auth user with basic permissions
    prefix = "ALCF-public-status-check"
    user = UserPydantic(
        id=f"{prefix}-id",
        name=f"{prefix}-name",
        username=f"{prefix}-username@no-domain.com",
        user_group_uuids=[],
        idp_id=f"{prefix}-idp-id",
        idp_name=f"{prefix}-idp-name",
        auth_service=f"{prefix}-auth-service",
    )

    # Get list of all publicy-available clusters
    authorized_clusters = [
        c
        async for db_cluster in Cluster.objects.all()
        if (c := await BaseCluster.load_adapter(db_cluster.cluster_name))
        and c.check_permission(user, raise_exc=False)
    ]

    # Build status
    return {
        cluster.cluster_name: not cluster.check_maintenance().is_under_maintenance
        for cluster in authorized_clusters
    }


# Whoami (GET)
@router.get("/whoami", response=UserPydantic)
async def whoami(request: AuthedRequest) -> UserPydantic:
    """
    GET basic user information from access token, or error message otherwise.
    """
    return request.auth


# List Endpoints (GET)
@router.get("/list-endpoints", response=ListEndpointsResponse)
async def get_list_endpoints(request: AuthedRequest) -> ListEndpointsResponse:
    """GET request to list the available frameworks and models."""
    return await get_list_endpoints_data(request.auth)


# List running and queue models (GET)
@router.get("/{cluster_name}/jobs", response=JobsByStatus)
async def get_jobs(request: AuthedRequest, cluster_name: str) -> JobsByStatus:
    """GET request to list the available frameworks and models."""

    cluster = await BaseCluster.load_adapter(cluster_name)

    # Make sure the user is authorized to see this cluster
    cluster.check_permission(request.auth)

    # If the cluster is under maintenance, report all jobs stopped:
    if cluster.check_maintenance().is_under_maintenance:
        all_endpoints = await get_list_endpoints_data(request.auth)
        cluster_info = all_endpoints.clusters.get(cluster.cluster_name)
        frameworks = cluster_info.frameworks if cluster_info else {}

        return JobsByStatus(
            stopped=[
                JobInfo(Models=model, Framework=framework, Cluster=cluster.cluster_name)
                for framework, fw_info in frameworks.items()
                for model in fw_info.models
            ]
        )
    else:
        return await filter_jobs_for_user(cluster, request.auth)
