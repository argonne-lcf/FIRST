import logging

from ninja import Query, Router

from ..endpoints import BaseEndpoint
from ..errors import (
    AccessDenied,
    BatchFailed,
    BatchNotFound,
    BatchOngoing,
)
from ..logging import get_request_context
from ..models import BatchLog
from ..schemas.auth import AuthedRequest
from ..schemas.batch import (
    BatchListFilter,
    BatchLogSummary,
    BatchStatus,
    BatchSubmit,
)
from ..schemas.endpoints import (
    SubmitBatchResult,
)
from ..services import (
    submit_batch,
)

router = Router()
log = logging.getLogger(__name__)


# Inference batch (POST)
@router.post("/{cluster_name}/{framework}/v1/batches", response=SubmitBatchResult)
async def post_batch_inference(
    request: AuthedRequest, cluster_name: str, framework: str, batch_data: BatchSubmit
) -> SubmitBatchResult:
    """POST request to send a batch to Globus Compute endpoints."""

    context = get_request_context()
    batch_response = await submit_batch(context, cluster_name, framework, batch_data)
    await BatchLog.create(
        context, batch_response, cluster_name, framework, batch_data.model
    )
    return batch_response


# List of batches (GET)
@router.get("/v1/batches", response=list[BatchLogSummary])
async def get_batch_list(
    request: AuthedRequest,
    filters: Query[BatchListFilter],
) -> list[BatchLogSummary]:
    """GET request to list all batches linked to the authenticated user."""

    batch_list = []

    # For each batch object owned by the user ...
    async for batch in BatchLog.objects.filter(user_id=request.auth.id).aiterator():
        # If the batch status needs to be revised ...
        if (
            batch.status
            not in [
                BatchStatus.completed.value,
                BatchStatus.failed.value,
            ]
            and batch.task_ids
        ):
            endpoint = await BaseEndpoint.load_adapter(
                batch.cluster, batch.framework, batch.model
            )
            status_result = await endpoint.get_batch_status(batch)
            await batch.update(status_result)

        # If no optional status filter was provided ...
        # or if the status filter matches the current batch status ...
        if filters.status is None or filters.status == batch.status:
            batch_list.append(BatchLogSummary.model_validate(batch))

    return batch_list


# Inference batch status (GET)
# TODO: Use primary identity username to claim ownership on files and batches
@router.get("/v1/batches/{batch_id}", response=str)
async def get_batch_status(request: AuthedRequest, batch_id: str) -> str:
    """GET request to query status of an existing batch job."""
    try:
        batch: BatchLog = await BatchLog.objects.aget(id=batch_id)
    except BatchLog.DoesNotExist:
        raise BatchNotFound(f"Batch {batch_id} does not exist")

    # Make sure user has permission to access this batch_id
    if not batch.user_id == request.auth.id:
        raise AccessDenied(f"Permission denied to Batch {batch_id}.")

    # Return status directly if batch already completed or failed
    if (
        batch.status not in [BatchStatus.completed, BatchStatus.failed]
        and batch.task_ids
    ):
        endpoint = await BaseEndpoint.load_adapter(
            batch.cluster, batch.framework, batch.model
        )
        status_result = await endpoint.get_batch_status(batch)
        await batch.update(status_result)

    return batch.status


# Inference batch result (GET)
# TODO: Use primary identity username to claim ownership on files and batches
@router.get("/v1/batches/{batch_id}/result", response=str)
async def get_batch_result(request: AuthedRequest, batch_id: str) -> str:
    """GET request to recover result from an existing batch job."""

    try:
        batch: BatchLog = await BatchLog.objects.aget(id=batch_id)
    except BatchLog.DoesNotExist:
        raise BatchNotFound(f"Batch {batch_id} does not exist")

    # Make sure user has permission to access this batch_id
    if not batch.user_id == request.auth.id:
        raise AccessDenied(f"Permission denied to Batch {batch_id}.")

    # Return status directly if batch already completed or failed
    if (
        batch.status not in [BatchStatus.completed, BatchStatus.failed]
        and batch.task_ids
    ):
        endpoint = await BaseEndpoint.load_adapter(
            batch.cluster, batch.framework, batch.model
        )
        status_result = await endpoint.get_batch_status(batch)
        await batch.update(status_result)

    if batch.status == BatchStatus.failed:
        raise BatchFailed(f"Batch failed: {batch.result}", 400, request)
    elif batch.status == BatchStatus.completed:
        return batch.result
    else:
        raise BatchOngoing("Batch not completed yet. Results not ready.")
