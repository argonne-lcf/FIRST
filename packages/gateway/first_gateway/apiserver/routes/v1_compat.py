"""
Temporary V1 (`/resource_server`) compatibility shim.
"""

import logging
from typing import Any, Literal

from fastapi import APIRouter
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from first_common.errors import NotFound
from first_common.schema.auth import UserAuthEvent
from first_common.schema.endpoints.llm import (
    AnthropicMessagesPayload,
    OpenAIChatCompletionsPayload,
    OpenAIEmbeddingsPayload,
    OpenAIResponsesPayload,
)
from first_common.schema.endpoints.models import api_protocols
from first_common.schema.endpoints.staging import StagingArea
from first_common.schema.types import HealthCheckResult, PilotDeploymentState

from ...database import models as db
from ...database.redis.router_config import ModelConfig, RouterConfig
from ...services import staging
from ..auth import user_can_access_group
from ..dependencies import (
    AppState,
    AuthUser,
    DbSession,
    RouterConfigDep,
)
from ..inference import InferenceService, InferenceServiceDep
from ..retirements import raise_if_retired

logger = logging.getLogger(__name__)

# Public (unauthenticated) V1 routes.
anon_router = APIRouter(prefix="/resource_server")
# Authenticated V1 routes (auth enforced by the parent router in routers.py).
router = APIRouter(prefix="/resource_server")


# --- V1 response shapes (mirrors the archived Django/Ninja schemas) ----------


class FrameworkSummary(BaseModel):
    models: list[str]
    endpoints: list[str]


class ClusterEndpointSummary(BaseModel):
    base_url: str
    frameworks: dict[str, FrameworkSummary]


class ListEndpointsResponse(BaseModel):
    clusters: dict[str, ClusterEndpointSummary]


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


class ClusterModelInfo(BaseModel):
    """
    One entry of V1's per-cluster model list.  V1 `alcf-ai agent configure`
    groups these by `framework`, filters on `id`, and reads `capabilities`
    (api_protocols, context_window_tokens, reasoning, tool_calling, ...) to
    write agent configs.
    """

    id: str
    object: Literal["model"] = "model"
    cluster: str
    framework: str
    display_name: str
    capabilities: dict[str, Any]


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


def _cluster_model_info(model: db.Model, cluster: str) -> ClusterModelInfo:
    """
    V1 capability block: the model's stored capabilities, plus the protocol and
    context facts V2 keeps in first-class columns (supported_endpoints and
    max_model_len are authoritative, since the gateway enforces them).
    """
    capabilities = dict(model.capabilities or {})
    capabilities["api_protocols"] = api_protocols(model.supported_endpoints)
    if model.max_model_len:
        capabilities["context_window_tokens"] = model.max_model_len
    return ClusterModelInfo(
        id=model.name,
        cluster=cluster,
        framework=_framework(cluster),
        display_name=model.display_name or model.name,
        capabilities=capabilities,
    )


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
    by_cluster: dict[str, ClusterEndpointSummary] = {}

    for model in await _visible_models(sess, user):
        for cluster in model.cluster_names:
            summary = by_cluster.setdefault(
                cluster,
                ClusterEndpointSummary(
                    base_url=f"resource_server/{cluster}", frameworks={}
                ),
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
    PilotDeploymentState.halted.value: "stopped",
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


# --- cluster models: V1's per-cluster model list ------------------------------


@router.get("/{cluster}/models", response_model=list[ClusterModelInfo])
async def cluster_models(
    cluster: str, sess: DbSession, user: AuthUser
) -> list[ClusterModelInfo]:
    """
    Visible models with a deployment on `cluster`, in V1's list shape.  An
    unknown cluster has no models.
    """
    return sorted(
        (
            _cluster_model_info(m, cluster)
            for m in await _visible_models(sess, user)
            if cluster in m.cluster_names
        ),
        key=lambda m: m.id,
    )


# --- staging: served by the V2 route; kept here for old clients -------------


@router.put("/data/staging", response_model=StagingArea)
async def ensure_staging_area(user: AuthUser, state: AppState) -> StagingArea:
    """Idempotently create a Globus staging area for the caller (see /data/v1)."""
    return await staging.ensure_staging_area(user, state)


# --- inference: reuse the deployment-scoped route, cluster picks deployment ---


def _cluster_deployment(model: ModelConfig | None, cluster: str) -> str | None:
    """
    The deployment of `model` on `cluster` to pin the request to, or None if
    there is none.  A deployment that can serve now is preferred; otherwise a
    cold one is returned, so that admission records the capacity reject the
    autoscaler scales on, and the caller gets the usual cold-start 503.
    """
    if model is None:
        return None
    on_cluster = [d for d in model.deployments if d.cluster_name == cluster]
    if not on_cluster:
        return None
    return next((d for d in on_cluster if d.backends), on_cluster[0]).name


async def _submit(
    inference: InferenceService,
    cluster: str,
    config: RouterConfig,
    payload: OpenAIChatCompletionsPayload
    | OpenAIResponsesPayload
    | AnthropicMessagesPayload
    | OpenAIEmbeddingsPayload,
) -> StreamingResponse | JSONResponse:
    model = config.models_by_name.get(payload.model) or config.models_by_alias.get(
        payload.model
    )
    raise_if_retired(inference.user, cluster, payload.model, model)
    deployment_name = _cluster_deployment(model, cluster)
    if deployment_name is None:
        if model is None:
            raise NotFound(f"Model {payload.model} does not exist.")
        raise NotFound(f"Model {model.name} is not deployed on cluster {cluster!r}.")
    return await inference.submit_inference(payload, deployment_name=deployment_name)


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
