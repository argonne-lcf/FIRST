import logging

from ninja import Router

from ..clusters import BaseCluster
from ..endpoints import BaseEndpoint, GlobusComputeEndpoint
from ..schemas.auth import AuthedRequest
from ..schemas.d3_triton import D3TritonRequest
from ..schemas.endpoints import (
    SubmitTaskAsyncResponse,
    SubmitTaskResult,
)

router = Router()
log = logging.getLogger(__name__)


@router.post("/sophia/triton/amsc-d3/process", response=SubmitTaskAsyncResponse)
async def d3_triton_infer(
    request: AuthedRequest, payload: D3TritonRequest
) -> SubmitTaskAsyncResponse:
    """
    Submit a Triton HEP inference request to Globus Compute endpoint.
    """
    cluster = await BaseCluster.load_adapter("sophia")
    (await cluster.check_maintenance()).raise_if_down()

    endpoint = await BaseEndpoint.load_adapter(
        cluster.cluster_name, "triton", "amsc-d3"
    )
    assert isinstance(endpoint, GlobusComputeEndpoint)
    log.info(f"endpoint_slug: {endpoint.endpoint_slug} - user: {request.auth.username}")

    endpoint.check_permission(request.auth)

    data = payload.model_dump()
    task_response = await endpoint.submit_task_async(data)
    return task_response


@router.get("/sophia/triton/amsc-d3/tasks/{task_id}", response=SubmitTaskResult)
async def d3_triton_get_task_result(
    request: AuthedRequest, task_id: str
) -> SubmitTaskResult:
    cluster = await BaseCluster.load_adapter("sophia")
    (await cluster.check_maintenance()).raise_if_down()

    endpoint = await BaseEndpoint.load_adapter(
        cluster.cluster_name, "triton", "amsc-d3"
    )
    assert isinstance(endpoint, GlobusComputeEndpoint)
    log.info(f"endpoint_slug: {endpoint.endpoint_slug} - user: {request.auth.username}")

    endpoint.check_permission(request.auth)
    return await endpoint.get_task_result(task_id)
