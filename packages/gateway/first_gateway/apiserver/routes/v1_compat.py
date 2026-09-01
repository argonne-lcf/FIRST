"""
Temporary V1 (`/resource_server`) compatibility shim.
"""

import logging

import anyio
from fastapi import APIRouter
from fastapi.responses import JSONResponse, StreamingResponse
from globus_sdk import ClientCredentialsAuthorizer, TransferClient
from pydantic import BaseModel

from first_common.errors import ServiceUnavailable
from first_common.schema.auth import UserAuthEvent
from first_common.schema.endpoints.llm import (
    AnthropicMessagesPayload,
    OpenAIChatCompletionsPayload,
    OpenAIEmbeddingsPayload,
    OpenAIResponsesPayload,
)
from first_common.schema.resources.read import ModelSummary
from first_common.schema.types import HealthCheckResult, PilotDeploymentState

from ...database import models as db
from ...database.redis.router_config import ModelConfig, RouterConfig
from ...settings import ClientState
from ..auth import user_can_access_group
from ..dependencies import (
    AppState,
    AuthUser,
    DbSession,
    RedisRepo,
    RouterConfigDep,
)
from ..inference import InferenceService, InferenceServiceDep

logger = logging.getLogger(__name__)

# Public (unauthenticated) V1 routes.
anon_router = APIRouter(prefix="/resource_server")
# Authenticated V1 routes (auth enforced by the parent router in routers.py).
router = APIRouter(prefix="/resource_server")


# --- V1 response shapes (mirrors the archived Django/Ninja schemas) ----------


class FrameworkSummary(BaseModel):
    models: list[str]
    endpoints: list[str]


class ClusterSummary(BaseModel):
    base_url: str
    frameworks: dict[str, FrameworkSummary]


class ListEndpointsResponse(BaseModel):
    clusters: dict[str, ClusterSummary]


class JobInfo(BaseModel):
    Models: str
    Framework: str
    Cluster: str
    model_config = {"extra": "allow"}  # V1 blocks carried loose extra fields


class JobsByStatus(BaseModel):
    running: list[JobInfo] = []
    queued: list[JobInfo] = []
    stopped: list[JobInfo] = []
    others: list[JobInfo] = []
    private_batch_running: list[JobInfo] = []
    private_batch_queued: list[JobInfo] = []
    cluster_status: dict[str, object] = {}


class StagingAreaPrepared(BaseModel):
    collection_id: str
    path: str
    acl_rule_id: str
    principal: str


# --- Helpers -----------------------------------------------------------------


def _framework(cluster: str) -> str:
    """Frameworks are irrelevant in V2; keep V1 clients happy with a stand-in."""
    return "vllm" if cluster == "sophia" else "api"


async def _visible_models(sess: DbSession, user: UserAuthEvent) -> list[db.Model]:
    """Models the caller may see (filtered by access group, like /v1/models)."""
    return [
        m
        for m in await db.Model.list(sess)
        if user_can_access_group(user, m.access_group)
    ]


def _clusters_of(model: db.Model) -> set[str]:
    return {d.cluster_name for d in model.pilot_deployments} | {
        d.cluster_name for d in model.static_deployments
    }


# --- health / whoami: trivial re-maps onto V2 --------------------------------


@anon_router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/whoami", response_model=UserAuthEvent)
async def whoami(user: AuthUser) -> UserAuthEvent:
    return user


# --- list-endpoints: grouped by cluster, derived from the ORM ----------------


@router.get("/list-endpoints", response_model=ListEndpointsResponse)
async def list_endpoints(sess: DbSession, user: AuthUser) -> ListEndpointsResponse:
    """
    List available frameworks and models, grouped by the cluster each model is
    deployed on.  Models with no deployment are invisible here.
    """
    by_cluster: dict[str, ClusterSummary] = {}

    for model in await _visible_models(sess, user):
        for cluster in _clusters_of(model):
            summary = by_cluster.setdefault(
                cluster,
                ClusterSummary(base_url=f"resource_server/{cluster}", frameworks={}),
            )
            fw = summary.frameworks.setdefault(
                _framework(cluster), FrameworkSummary(models=[], endpoints=[])
            )
            fw.models.append(model.name)
            fw.endpoints.extend(f"/v1/{e}" for e in model.supported_endpoints)

    # De-dup and sort for a stable, V1-close-enough shape.
    for summary in by_cluster.values():
        for fw in summary.frameworks.values():
            fw.models = sorted(set(fw.models))
            fw.endpoints = sorted(set(fw.endpoints))

    return ListEndpointsResponse(clusters=by_cluster)


# --- jobs: model "job" listing for a cluster, grouped by state ---------------

# Keyed by the raw string column values (ORM stores enum `.value`).
_PILOT_STATE_BUCKET = {
    PilotDeploymentState.healthy.value: "running",
    PilotDeploymentState.degraded.value: "running",
    PilotDeploymentState.starting.value: "queued",
    PilotDeploymentState.awaiting_capacity.value: "queued",
    PilotDeploymentState.stopping.value: "stopped",
    PilotDeploymentState.failed.value: "stopped",
    PilotDeploymentState.offline.value: "stopped",
}
_STATIC_HEALTH_BUCKET = {
    HealthCheckResult.healthy.value: "running",
    HealthCheckResult.unhealthy.value: "stopped",
    HealthCheckResult.unknown.value: "others",
}


@router.get("/{cluster}/jobs", response_model=JobsByStatus)
async def cluster_jobs(cluster: str, sess: DbSession, user: AuthUser) -> JobsByStatus:
    """
    Status of all (visible) models on a cluster, mapped into V1's grouped-by-state
    shape.  Deployment states are mapped to running/queued/stopped/others as best
    they fit; this is a shim, not a faithful translation.
    """
    buckets: dict[str, list[JobInfo]] = {
        "running": [],
        "queued": [],
        "stopped": [],
        "others": [],
    }

    def add(model_name: str, status: str) -> None:
        buckets[status].append(
            JobInfo(
                **{
                    "Models": model_name,
                    "Framework": _framework(cluster),
                    "Cluster": cluster,
                    "Model Status": status,
                }
            )
        )

    for model in await _visible_models(sess, user):
        for pilot in model.pilot_deployments:
            if pilot.cluster_name == cluster:
                add(model.name, _PILOT_STATE_BUCKET.get(pilot.state, "others"))
        for static in model.static_deployments:
            if static.cluster_name == cluster:
                add(model.name, _STATIC_HEALTH_BUCKET.get(static.health, "others"))

    total = sum(len(v) for v in buckets.values())
    return JobsByStatus(
        running=buckets["running"],
        queued=buckets["queued"],
        stopped=buckets["stopped"],
        others=buckets["others"],
        cluster_status={
            "cluster": cluster,
            "total_models": total,
            "live_models": len(buckets["running"]),
            "stopped_models": len(buckets["stopped"]),
        },
    )


# --- cluster models: the catalog models API, filtered by cluster -------------


@router.get("/{cluster}/models", response_model=list[ModelSummary])
async def cluster_models(
    cluster: str, sess: DbSession, user: AuthUser, repo: RedisRepo
) -> list[ModelSummary]:
    models = [
        m for m in await _visible_models(sess, user) if cluster in _clusters_of(m)
    ]
    runtimes = await repo.get_many_model_runtimes([m.name for m in models])
    return [
        ModelSummary.merge(model, runtime=rt) for model, rt in zip(models, runtimes)
    ]


# --- staging: hoisted from V1 (the one genuinely new V2 feature) -------------

_transfer_client: TransferClient | None = None


def _get_transfer_client(state: ClientState) -> TransferClient:
    """Cache a TransferClient built from the existing confidential app creds."""
    global _transfer_client
    if _transfer_client is None:
        _transfer_client = TransferClient(
            authorizer=ClientCredentialsAuthorizer(
                state.auth_client, TransferClient.scopes.all
            )
        )
    return _transfer_client


def _prep_globus_staging_area(
    tc: TransferClient, principal_id: str, collection_id: str
) -> StagingAreaPrepared:
    """
    Create or refresh ACLs on a per-user staging directory under `collection_id`.

    A directory named with the user's principal ID is (idempotently) created and
    granted read/write ACLs to that user so they can transfer data in and out.
    """
    logger.info(f"User {principal_id=} requesting staging area in {collection_id=}")

    staging_path = f"/user-staging/{principal_id}/"

    try:
        tc.operation_mkdir(collection_id, staging_path)
        logger.info(f"staging directory {staging_path=} created")
    except tc.error_class as e:
        if "exists" not in str(e).lower():
            raise
        logger.info(f"staging directory {staging_path=} already exists")

    existing = tc.endpoint_acl_list(collection_id)
    rule = next(
        (
            r
            for r in existing
            if r["principal"] == principal_id and r["path"] == staging_path
        ),
        None,
    )

    if rule is None:
        result = tc.add_endpoint_acl_rule(
            collection_id,
            dict(
                DATA_TYPE="access",
                principal_type="identity",
                principal=principal_id,
                path=staging_path,
                permissions="rw",
            ),
        )
        acl_rule_id = str(result["access_id"])
        logger.info(f"Granted rw access via {acl_rule_id=}")
    else:
        acl_rule_id = str(rule["id"])
        logger.info(f"Staging area {acl_rule_id=} already exists for {principal_id=}")

    return StagingAreaPrepared(
        collection_id=collection_id,
        path=staging_path,
        acl_rule_id=acl_rule_id,
        principal=principal_id,
    )


@router.put("/staging", response_model=StagingAreaPrepared)
async def ensure_staging_area(user: AuthUser, state: AppState) -> StagingAreaPrepared:
    """Idempotently create a Globus staging area for the caller."""
    tc = _get_transfer_client(state)
    return await anyio.to_thread.run_sync(
        _prep_globus_staging_area,
        tc,
        user.id,
        state.settings.data_staging_globus_collection_id,
    )


# --- inference: reuse the deployment-scoped route, cluster picks deployment ---


def _deployment_name(cluster: str, config: RouterConfig, model_name: str) -> str:
    """First live deployment for `model_name` on `cluster` (framework ignored)."""
    model: ModelConfig | None = config.models_by_name.get(
        model_name
    ) or config.models_by_alias.get(model_name)
    if model is not None:
        for dep in model.deployments:
            if dep.name.startswith(f"{cluster}/") and dep.backends:
                return dep.name
    raise ServiceUnavailable(
        f"No available deployment for {model_name!r} on cluster {cluster!r}."
    )


async def _submit(
    inference: InferenceService,
    cluster: str,
    config: RouterConfig,
    payload: OpenAIChatCompletionsPayload
    | OpenAIResponsesPayload
    | AnthropicMessagesPayload
    | OpenAIEmbeddingsPayload,
) -> StreamingResponse | JSONResponse:
    return await inference.submit_inference(
        payload, deployment_name=_deployment_name(cluster, config, payload.model)
    )


@router.post("/{cluster}/{framework}/v1/chat/completions", response_model=None)
async def chat_completions(
    cluster: str,
    framework: str,  # noqa: ARG001  (path segment; irrelevant in V2)
    inference: InferenceServiceDep,
    config: RouterConfigDep,
    payload: OpenAIChatCompletionsPayload,
) -> StreamingResponse | JSONResponse:
    return await _submit(inference, cluster, config, payload)


@router.post("/{cluster}/{framework}/v1/responses", response_model=None)
async def responses(
    cluster: str,
    framework: str,  # noqa: ARG001  (path segment; irrelevant in V2)
    inference: InferenceServiceDep,
    config: RouterConfigDep,
    payload: OpenAIResponsesPayload,
) -> StreamingResponse | JSONResponse:
    return await _submit(inference, cluster, config, payload)


@router.post("/{cluster}/{framework}/v1/messages", response_model=None)
async def messages(
    cluster: str,
    framework: str,  # noqa: ARG001  (path segment; irrelevant in V2)
    inference: InferenceServiceDep,
    config: RouterConfigDep,
    payload: AnthropicMessagesPayload,
) -> StreamingResponse | JSONResponse:
    return await _submit(inference, cluster, config, payload)


@router.post("/{cluster}/{framework}/v1/embeddings", response_model=None)
async def embeddings(
    cluster: str,
    framework: str,  # noqa: ARG001  (path segment; irrelevant in V2)
    inference: InferenceServiceDep,
    config: RouterConfigDep,
    payload: OpenAIEmbeddingsPayload,
) -> StreamingResponse | JSONResponse:
    return await _submit(inference, cluster, config, payload)
