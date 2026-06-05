import logging

from ninja import Router

from ..clusters import BaseCluster
from ..endpoints import BaseEndpoint, GlobusComputeEndpoint
from ..schemas import (
    Sam3Request,
)
from ..schemas.auth import AuthedRequest
from ..schemas.endpoints import (
    SubmitTaskAsyncResponse,
    SubmitTaskResult,
)

router = Router()
log = logging.getLogger(__name__)


# Inference (POST)
@router.post("/sophia/sam3service/process", response=SubmitTaskAsyncResponse)
async def sam3_infer(
    request: AuthedRequest, payload: Sam3Request
) -> SubmitTaskAsyncResponse:
    """
    Submit single-image inference request to SAM3 Globus Compute endpoint.
    """
    # Get cluster wrapper from database
    cluster = await BaseCluster.load_adapter("sophia")

    # Error if the cluster is under maintenance
    cluster.check_maintenance().raise_if_down()

    # Endpoint slug (sophia-sam3service-sam3 hardcoded for now)
    endpoint = await BaseEndpoint.load_adapter(
        cluster.cluster_name, "sam3service", "sam3"
    )
    assert isinstance(endpoint, GlobusComputeEndpoint)
    log.info(f"endpoint_slug: {endpoint.endpoint_slug} - user: {request.auth.username}")

    # Block access if the user is not allowed to use the endpoint
    endpoint.check_permission(request.auth)

    # Submit task
    data = payload.model_dump(exclude={"weights_dir_override"})
    config = (
        {"sam3_weights_dir": str(payload.weights_dir_override)}
        if payload.weights_dir_override
        else None
    )

    task_response = await endpoint.submit_task_async(data, endpoint_config=config)
    return task_response


@router.get("/sophia/sam3service/tasks/{task_id}", response=SubmitTaskResult)
async def sam3_get_task_result(
    request: AuthedRequest, task_id: str
) -> SubmitTaskResult:
    # Get cluster wrapper from database
    cluster = await BaseCluster.load_adapter("sophia")

    # Error if the cluster is under maintenance
    cluster.check_maintenance().raise_if_down()

    endpoint = await BaseEndpoint.load_adapter(
        cluster.cluster_name, "sam3service", "sam3"
    )
    assert isinstance(endpoint, GlobusComputeEndpoint)
    log.info(f"endpoint_slug: {endpoint.endpoint_slug} - user: {request.auth.username}")

    # Block access if the user is not allowed to use the endpoint
    endpoint.check_permission(request.auth)
    return await endpoint.get_task_result(task_id)
