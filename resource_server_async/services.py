import json
import logging
import uuid
from typing import Any

from django.conf import settings
from django.http import StreamingHttpResponse
from django.utils import timezone
from pydantic import ValidationError

from resource_server_async.globus_utils import get_transfer_client
from resource_server_async.schemas.anthropic_messages import AnthropicMessagesPydantic
from resource_server_async.schemas.openai_control import (
    FIRST_RESERVED_OPENAI_FIELDS,
    OPENAI_CONTROL_MODELS,
    OPENAI_PROMPT_FIELDS,
    OpenAIControlFields,
    OpenAIEndpoint,
)
from resource_server_async.schemas.structured_logs import (
    RequestLogPydantic,
)

from .clusters import BaseCluster
from .endpoints import BaseEndpoint, DirectAPIEndpoint
from .errors import (
    BatchOngoing,
    BatchUnavailable,
    EndpointNotFound,
    InvalidRequest,
    QuotaExceeded,
    TooManyRequests,
    UnsupportedEndpoint,
    UnsupportedFramework,
)
from .logging import RequestContext
from .models import BatchLog, Cluster, Endpoint
from .schemas import GlobusStagingAreaPrepared
from .schemas.batch import (
    BatchStatus,
    BatchSubmit,
)
from .schemas.clusters import JobsByStatus
from .schemas.endpoints import (
    ClusterSummary,
    FrameworkSummary,
    ListEndpointsResponse,
    SubmitBatchResult,
    SubmitStreamingTaskResponse,
    SubmitTaskResult,
)
from .schemas.structured_logs import UserPydantic

OpenAIRequestPayload = dict[str, Any] | AnthropicMessagesPydantic

logger = logging.getLogger(__name__)


def _sanitized_control_errors(error: ValidationError) -> list[dict[str, str]]:
    """Return validation details without client-supplied values or context."""
    return [
        {
            "field": ".".join(str(part) for part in detail["loc"]),
            "message": detail["msg"],
            "type": detail["type"],
        }
        for detail in error.errors(
            include_url=False,
            include_context=False,
            include_input=False,
        )
    ]


def _prepare_openai_request(
    payload: dict[str, Any], openai_endpoint: OpenAIEndpoint
) -> tuple[OpenAIControlFields, dict[str, Any], Any]:
    """Validate FIRST-owned fields while preserving backend-owned JSON values."""
    supplied_reserved_fields = sorted(
        FIRST_RESERVED_OPENAI_FIELDS.intersection(payload)
    )
    if supplied_reserved_fields:
        raise InvalidRequest(
            "FIRST-reserved request fields are not accepted.",
            info={"fields": supplied_reserved_fields},
        )

    try:
        control = OPENAI_CONTROL_MODELS[openai_endpoint].model_validate(payload)
    except ValidationError as error:
        raise InvalidRequest(
            "Invalid FIRST control fields.",
            info={"errors": _sanitized_control_errors(error)},
        ) from None

    outbound = dict(payload)
    outbound["stream"] = control.stream is True
    outbound["openai_endpoint"] = openai_endpoint
    prompt = payload[OPENAI_PROMPT_FIELDS[openai_endpoint]]
    return control, outbound, prompt


async def get_all_endpoints(
    user: UserPydantic, cluster: BaseCluster
) -> list[BaseEndpoint]:
    """Generate and return all endpoint adaptors for a given cluster."""

    # For all endpoints in the database for this cluster ...
    endpoint_adaptors: list[BaseEndpoint] = []
    async for db_endpoint in Endpoint.objects.filter(cluster=cluster.cluster_name):
        endpoint = await BaseEndpoint.load_adapter(
            db_endpoint.cluster, db_endpoint.framework, db_endpoint.model
        )

        # Add endpoint adaptor to the list if authorized
        if endpoint.check_permission(user, raise_exc=False):
            endpoint_adaptors.append(endpoint)

    # Return list of authorized endpoints
    return endpoint_adaptors


async def get_list_endpoints_data(user: UserPydantic) -> ListEndpointsResponse:
    """Prepare and return data for the list of available frameworks and models."""
    by_cluster: dict[str, ClusterSummary] = {}

    # Get list of all clusters
    db_clusters = [c async for c in Cluster.objects.all()]
    authorized_clusters = [
        c
        for db_cluster in db_clusters
        if (c := await BaseCluster.load_adapter(db_cluster.cluster_name))
        and c.check_permission(user, raise_exc=False)
    ]

    for cluster in authorized_clusters:
        # For each authorized endpoint related to this cluster ...
        frameworks: dict[str, FrameworkSummary] = {}

        authorized_endpoints = await get_all_endpoints(user, cluster)
        for endpoint in authorized_endpoints:
            # Add framework if needed
            if endpoint.framework not in frameworks:
                frameworks[endpoint.framework] = FrameworkSummary(
                    models=[],
                    endpoints=[f"/v1/{e}" for e in cluster.openai_endpoints],
                )

            # Add model to the framework
            frameworks[endpoint.framework].models.append(endpoint.model)

        # Sort models alphabetically
        for fw in frameworks:
            frameworks[fw].models = sorted(frameworks[fw].models)

        # Add endpoint list to the response
        by_cluster[cluster.cluster_name] = ClusterSummary(
            base_url=f"/resource_server/{cluster.cluster_name}",
            frameworks=frameworks,
        )

    return ListEndpointsResponse(clusters=by_cluster)


def prep_globus_staging_area(
    principal_id: str, collection_id: str
) -> GlobusStagingAreaPrepared:
    """
    Create or refresh ACLs on a staging directory for the inference service.

    A temporary directory under the Globus collection_id is named with the
    user's principal ID.  Ensure this directory exists and ensure read/write
    ACLs are granted to the user to initiate data transfers in and out of this
    area.
    """
    logger.info(f"User {principal_id=} requesting staging area in {collection_id=}")

    staging_path = f"/user-staging/{principal_id}/"

    tc = get_transfer_client()

    try:
        tc.operation_mkdir(collection_id, staging_path)
        logger.info(f"staging directory {staging_path=} created")
    except tc.error_class as e:
        if "exists" not in str(e).lower():
            raise
        logger.info(f"staging directory {staging_path=} already exists")

    existing_rules = tc.endpoint_acl_list(collection_id)
    acl_rule_id = next(
        (
            r
            for r in existing_rules
            if r["principal"] == principal_id and r["path"] == staging_path
        ),
        None,
    )

    if acl_rule_id is None:
        acl_result = tc.add_endpoint_acl_rule(
            collection_id,
            dict(
                DATA_TYPE="access",
                principal_type="identity",
                principal=principal_id,
                path=staging_path,
                permissions="rw",
            ),
        )
        acl_rule_id = acl_result["access_id"]
        logger.info(f"Granted rw access via {acl_rule_id=}")
    else:
        logger.info(f"Staging area {acl_rule_id=} already exists for {principal_id=}")

    return GlobusStagingAreaPrepared(
        collection_id=collection_id,
        path=staging_path,
        acl_rule_id=str(acl_rule_id),
        principal=principal_id,
    )


async def _should_show(
    cluster: str, framework: str, model: str, user: UserPydantic
) -> bool:
    """
    Return whether user is authorized to see this endpoint.
    """
    try:
        endpoint = await BaseEndpoint.load_adapter(cluster, framework, model)
    except EndpointNotFound:
        return False
    return endpoint.check_permission(user, raise_exc=False)


async def filter_jobs_for_user(
    cluster: BaseCluster, user: UserPydantic
) -> JobsByStatus:
    """
    Report jobs from the given cluster, grouped by status and filtered according
    to which endpoints the user is authorized to see.
    """
    # Get jobs from the targetted cluster
    jobs = await cluster.get_jobs(user)

    # For each job state listed in the jobs response ...
    for jobs_state in [
        jobs.running,
        jobs.queued,
        jobs.stopped,
        jobs.others,
        jobs.private_batch_running,
        jobs.private_batch_queued,
    ]:
        # For each block (set of models) in this state
        # -1, -1, -1 for reversed order to safely remove/edit values jobs_state
        for i_block in range(len(jobs_state) - 1, -1, -1):
            block = jobs_state[i_block]

            models = [m.strip() for m in block.Models.split(",") if m.strip()]
            visible_models = [
                model
                for model in models
                if await _should_show(block.Cluster, block.Framework, model, user)
            ]

            # Remove block if no model should be visible
            if len(visible_models) == 0:
                del jobs_state[i_block]

            # Update models if some (or all) of them are still visible
            else:
                jobs_state[i_block].Models = ",".join(visible_models)

    return jobs


async def submit_openai_inference_request(
    context: RequestContext,
    cluster_name: str,
    framework: str,
    payload: OpenAIRequestPayload,
    *,
    openai_endpoint: OpenAIEndpoint | None = None,
) -> StreamingHttpResponse | Any:
    route: str
    if isinstance(payload, AnthropicMessagesPydantic):
        is_anthropic_messages = True
        route = payload.openai_endpoint
        stream = payload.stream is True
        prompt = payload.model_dump(include={"messages"}, mode="json")["messages"]
        requested_model = payload.model
        outbound = payload.model_dump(
            exclude_none=True, exclude_unset=True, mode="json"
        )
        outbound["stream"] = stream
        outbound["openai_endpoint"] = route
    else:
        is_anthropic_messages = False
        if openai_endpoint is None:
            raise ValueError("openai_endpoint is required for OpenAI requests")
        route = openai_endpoint
        control, outbound, prompt = _prepare_openai_request(payload, openai_endpoint)
        stream = control.stream is True
        requested_model = control.model

    is_openai_responses = route == "responses"

    assert context.user is not None

    # Get cluster wrapper from database
    cluster = await BaseCluster.load_adapter(cluster_name)

    # Error if the cluster is under maintenance
    (await cluster.check_maintenance()).raise_if_down()

    # Verify that the framework is available by the cluster
    if framework not in cluster.frameworks:
        raise UnsupportedFramework(
            f"framework {framework} not available on cluster {cluster.cluster_name}."
        )

    # Verify that the openAI endpoint is available by the cluster
    if route not in cluster.openai_endpoints:
        raise UnsupportedEndpoint(
            f"{route!r} not available on cluster {cluster.cluster_name!r}"
        )

    endpoint = await BaseEndpoint.load_adapter(
        cluster.cluster_name, framework, requested_model
    )
    logger.debug(
        f"endpoint_slug: {endpoint.endpoint_slug} - user: {context.user.username}"
    )

    if (
        stream
        and (is_openai_responses or is_anthropic_messages)
        and not isinstance(endpoint, DirectAPIEndpoint)
    ):
        # We don't support streaming on non-DirectAPI backed endpoints currently
        raise UnsupportedEndpoint(
            "Streaming is not supported for the "
            f"{'OpenAI Responses' if is_openai_responses else 'Anthropic Messages'}"
            " API on this endpoint. Re-issue this request with 'stream': false."
        )

    # Keep endpoint aliases useful for routing, but make the backend and logs use
    # the canonical model selected by FIRST.
    outbound["model"] = endpoint.model

    # Block access if the user is not allowed to use the endpoint
    endpoint.check_permission(context.user)

    # Return 429 status if TPM limits are exceeded
    tpm_check = endpoint.check_token_rate_limit(context.user)
    if not tpm_check.allow:
        logger.info(f"{endpoint.endpoint_slug} rate-limited: {tpm_check}")
        raise TooManyRequests(
            "Tokens/minute limit exceeded",
            info={
                "global_model_usage": tpm_check.usage_model,
                "user_model_usage": tpm_check.usage_user,
            },
        )

    # Initialize the request log
    context.request_log = RequestLogPydantic(
        id=str(uuid.uuid4()),
        access_log_id=context.access_log.id,
        user_id=context.user.id,
        cluster=cluster.cluster_name,
        framework=framework,
        model=endpoint.model,
        openai_endpoint=route,
        prompt=json.dumps(prompt),
        timestamp_compute_request=timezone.now(),
    )

    data = {"model_params": outbound}

    # Submit task
    task_response: SubmitStreamingTaskResponse | SubmitTaskResult
    if stream:
        task_response = await endpoint.submit_streaming_task(data)
    else:
        task_response = await endpoint.submit_task(data)

    # Update request log data
    context.request_log.task_uuid = task_response.task_id
    context.request_log.timestamp_compute_response = timezone.now()

    # If streaming, meaning that the StreamingHttpResponse object will be returned directly ...
    if isinstance(task_response, SubmitStreamingTaskResponse):
        # Return StreamingHttpResponse object directly
        return task_response.response
    # If not streaming, return the complete response and automate database operations
    else:
        return task_response.result


async def submit_batch(
    context: RequestContext, cluster_name: str, framework: str, batch_data: BatchSubmit
) -> SubmitBatchResult:
    assert context.user is not None

    # Get cluster wrapper from database
    cluster = await BaseCluster.load_adapter(cluster_name)

    # Error if the cluster is under maintenance
    (await cluster.check_maintenance()).raise_if_down()

    # Verify that the framework is enabled by the cluster
    if framework not in cluster.frameworks:
        raise UnsupportedFramework(
            f"Framework {framework!r} not available on cluster {cluster.cluster_name!r}."
        )

    endpoint = await BaseEndpoint.load_adapter(
        cluster_name, framework, batch_data.model
    )

    # Error if batch is disabled for this endpoint
    if not endpoint.has_batch_enabled():
        raise BatchUnavailable(
            f"Batch is unavailable for endpoint {endpoint.endpoint_slug}"
        )

    # Block access if the user is not allowed to use the endpoint
    endpoint.check_permission(context.user)

    # Reject request if the allowed quota per user would be exceeded
    number_of_active_batches = await BatchLog.objects.filter(
        user_id=context.user.id,
        status__in=["pending", "running"],
    ).acount()

    if number_of_active_batches >= settings.MAX_BATCHES_PER_USER:
        raise QuotaExceeded(
            f"Quota of {settings.MAX_BATCHES_PER_USER} active batch(es) per user exceeded."
        )

    # Error if an ongoing batch already exists with the same input_file for the same user
    existing_batch = (
        await BatchLog.objects.filter(
            user_id=context.user.id,
            input_file=batch_data.input_file,
        )
        .exclude(
            status__in=[
                BatchStatus.failed.value,
                BatchStatus.completed.value,
            ],
        )
        .afirst()
    )

    if existing_batch is not None:
        raise BatchOngoing(
            f"Input file {batch_data.input_file} "
            f"already used by ongoing batch {existing_batch.id}."
        )

    # Submit batch
    return await endpoint.submit_batch(batch_data, context.user.username)
