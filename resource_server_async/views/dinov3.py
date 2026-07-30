import logging

from ninja import Router

from ..clusters import BaseCluster
from ..endpoints import BaseEndpoint, GlobusComputeEndpoint
from ..errors import BaseError
from ..schemas.auth import AuthedRequest
from ..schemas.dinov3 import DINOv3Request
from ..schemas.endpoints import (
    SubmitTaskAsyncResponse,
    SubmitTaskResult,
)

router = Router()
log = logging.getLogger(__name__)

# Suffix appended to input_dir to derive the results directory. Kept as a sibling
# within the same staging area so the service can write results there and
# the user can read them back through Globus.
OUTPUT_DIR_SUFFIX = ".dinov3.output"


def _derive_output_dir(input_dir: str, principal_id: str) -> str:
    """
    Constrain and derive the results directory from ``input_dir``.
    """
    staging_segment = f"/user-staging/{principal_id}/"
    normalized = input_dir.rstrip("/")
    marker = normalized.find(staging_segment)
    # Require the segment to be present AND a real subfolder to follow it, so
    # that ``normalized + suffix`` stays inside /user-staging/{id}/.
    if marker == -1 or not normalized[marker + len(staging_segment) :]:
        raise BaseError(
            "input_dir must be a subdirectory of your staging area "
            f"({staging_segment}); got: {input_dir!r}",
            status_code=400,
        )
    return normalized + OUTPUT_DIR_SUFFIX


@router.post("/sophia/dinoserver/process", response=SubmitTaskAsyncResponse)
async def dinov3_infer(
    request: AuthedRequest, payload: DINOv3Request
) -> SubmitTaskAsyncResponse:
    """
    Submit a DINOv3 image segmentation request to the Globus Compute endpoint.
    """
    cluster = await BaseCluster.load_adapter("sophia")
    (await cluster.check_maintenance()).raise_if_down()

    endpoint = await BaseEndpoint.load_adapter(
        cluster.cluster_name, "dinoserver", "dinov3"
    )
    assert isinstance(endpoint, GlobusComputeEndpoint)
    log.info(f"endpoint_slug: {endpoint.endpoint_slug} - user: {request.auth.username}")

    endpoint.check_permission(request.auth)

    # Drop unset optional fields so the Globus Compute function's own defaults
    # apply, then derive output_dir server-side (never trusted from the client).
    data = payload.model_dump(exclude_none=True)
    data["output_dir"] = _derive_output_dir(payload.input_dir, request.auth.id)
    task_response = await endpoint.submit_task_async(data)
    return task_response


@router.get("/sophia/dinoserver/tasks/{task_id}", response=SubmitTaskResult)
async def dinov3_get_task_result(
    request: AuthedRequest, task_id: str
) -> SubmitTaskResult:
    cluster = await BaseCluster.load_adapter("sophia")
    (await cluster.check_maintenance()).raise_if_down()

    endpoint = await BaseEndpoint.load_adapter(
        cluster.cluster_name, "dinoserver", "dinov3"
    )
    assert isinstance(endpoint, GlobusComputeEndpoint)
    log.info(f"endpoint_slug: {endpoint.endpoint_slug} - user: {request.auth.username}")

    endpoint.check_permission(request.auth)
    return await endpoint.get_task_result(task_id)
