import json
import logging
import uuid
from typing import Any

from django.conf import settings
from django.http import StreamingHttpResponse
from django.utils import timezone

from resource_server_async.globus_utils import get_transfer_client
from resource_server_async.schemas.anthropic_messages import AnthropicMessagesPydantic
from resource_server_async.schemas.openai_chat_completions import (
    OpenAIChatCompletionsPydantic,
)
from resource_server_async.schemas.openai_completions import OpenAICompletionsPydantic
from resource_server_async.schemas.openai_embeddings import OpenAIEmbeddingsPydantic
from resource_server_async.schemas.openai_responses import (
    OpenAIResponsesPydantic,
)
from resource_server_async.schemas.structured_logs import (
    RequestLogPydantic,
)

from .clusters import BaseCluster
from .endpoints import BaseEndpoint
from .errors import (
    BatchOngoing,
    BatchUnavailable,
    EndpointNotFound,
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

OpenAIRequestPayload = (
    OpenAIChatCompletionsPydantic
    | OpenAICompletionsPydantic
    | OpenAIEmbeddingsPydantic
    | OpenAIResponsesPydantic
    | AnthropicMessagesPydantic
)

logger = logging.getLogger(__name__)


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
        # For each endpoint related to this cluster ...
        frameworks: dict[str, FrameworkSummary] = {}

        async for db_endpoint in Endpoint.objects.filter(cluster=cluster.cluster_name):
            endpoint = await BaseEndpoint.load_adapter(
                db_endpoint.cluster, db_endpoint.framework, db_endpoint.model
            )

            # If the user is allowed to see this endpoint ...
            if endpoint.check_permission(user, raise_exc=False):
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
) -> StreamingHttpResponse | Any:
    if isinstance(payload, OpenAIChatCompletionsPydantic):
        stream = payload.stream or False
        prompt = payload.model_dump(include={"messages"})["messages"]
    elif isinstance(payload, OpenAICompletionsPydantic):
        stream = payload.stream or False
        prompt = payload.prompt
    elif isinstance(payload, OpenAIEmbeddingsPydantic):
        stream = False
        prompt = payload.input
    elif isinstance(payload, OpenAIResponsesPydantic):
        stream = payload.stream or False
        prompt = payload.model_dump(include={"input"}, mode="json")["input"]
    elif isinstance(payload, AnthropicMessagesPydantic):
        stream = payload.stream or False
        prompt = payload.model_dump(include={"messages"}, mode="json")["messages"]
    else:
        raise ValueError(f"Invalid {payload=}")

    assert context.user is not None

    # Get cluster wrapper from database
    cluster = await BaseCluster.load_adapter(cluster_name)

    # Error if the cluster is under maintenance
    cluster.check_maintenance().raise_if_down()

    # Verify that the framework is available by the cluster
    if framework not in cluster.frameworks:
        raise UnsupportedFramework(
            f"framework {framework} not available on cluster {cluster.cluster_name}."
        )

    # Verify that the openAI endpoint is available by the cluster
    if payload.openai_endpoint not in cluster.openai_endpoints:
        raise UnsupportedEndpoint(
            f"{payload.openai_endpoint!r} not available on cluster {cluster.cluster_name!r}"
        )

    endpoint = await BaseEndpoint.load_adapter(
        cluster.cluster_name, framework, payload.model
    )
    logger.debug(
        f"endpoint_slug: {endpoint.endpoint_slug} - user: {context.user.username}"
    )

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
        model=payload.model,
        openai_endpoint=payload.openai_endpoint,
        prompt=json.dumps(prompt),
        timestamp_compute_request=timezone.now(),
    )

    data = {
        "model_params": payload.model_dump(
            exclude_none=True, exclude_unset=True, mode="json"
        )
    }
    data["model_params"]["openai_endpoint"] = payload.openai_endpoint
    logger.debug("Sending openai inference request", extra={"openai_payload": data})

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
    cluster.check_maintenance().raise_if_down()

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
