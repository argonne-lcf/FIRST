from fastapi import APIRouter
from fastapi.responses import JSONResponse, StreamingResponse

from first_common.schema.auth import UserAuthEvent
from first_common.schema.endpoints.generic import GenericTaskPayload
from first_common.schema.endpoints.llm import (
    AnthropicMessagesPayload,
    OpenAIChatCompletionsPayload,
    OpenAIEmbeddingsPayload,
    OpenAIResponsesPayload,
)
from first_common.schema.endpoints.models import ModelInfo, ModelListResponse

from ...database.redis.router_config import ModelConfig, RouterConfig
from ...services.orchestration import get_name_from_slug
from ..auth import user_can_access_group
from ..dependencies import AuthUser, RouterConfigDep
from ..inference import InferenceServiceDep

federated_router = APIRouter(prefix="/federated/v1")
deployment_router = APIRouter(prefix="/deployments/{deployment_slug}/v1")


def _model_info(model: ModelConfig) -> ModelInfo:
    return ModelInfo(
        id=model.name,
        created_at=model.created_at,
        display_name=model.display_name or model.name,
        capabilities=model.capabilities,
        max_tokens=model.max_model_len,
    )


def _visible_models(config: RouterConfig, user: UserAuthEvent) -> list[ModelConfig]:
    return [m for m in config.models if user_can_access_group(user, m)]


# --- Federated routes: backend chosen via `model` in the request body. -------


@federated_router.post("/chat/completions", response_model=None)
async def chat_completions(
    inference: InferenceServiceDep,
    payload: OpenAIChatCompletionsPayload,
) -> StreamingResponse | JSONResponse:
    return await inference.submit_inference(payload)


@federated_router.post("/responses", response_model=None)
async def responses(
    inference: InferenceServiceDep,
    payload: OpenAIResponsesPayload,
) -> StreamingResponse | JSONResponse:
    return await inference.submit_inference(payload)


@federated_router.post("/messages", response_model=None)
async def messages(
    inference: InferenceServiceDep,
    payload: AnthropicMessagesPayload,
) -> StreamingResponse | JSONResponse:
    return await inference.submit_inference(payload)


@federated_router.post("/embeddings", response_model=None)
async def embeddings(
    inference: InferenceServiceDep,
    payload: OpenAIEmbeddingsPayload,
) -> StreamingResponse | JSONResponse:
    return await inference.submit_inference(payload)


@federated_router.post("/tasks", response_model=None)
async def tasks(
    inference: InferenceServiceDep,
    payload: GenericTaskPayload,
) -> StreamingResponse | JSONResponse:
    return await inference.submit_inference(payload)


@federated_router.get("/models", response_model=ModelListResponse)
async def list_models(config: RouterConfigDep, user: AuthUser) -> ModelListResponse:
    return ModelListResponse(
        data=[_model_info(m) for m in _visible_models(config, user)]
    )


# --- Deployment-scoped routes: backend pinned to a single deployment. ---------


@deployment_router.post("/chat/completions", response_model=None)
async def deployment_chat_completions(
    deployment_slug: str,
    inference: InferenceServiceDep,
    payload: OpenAIChatCompletionsPayload,
) -> StreamingResponse | JSONResponse:
    return await inference.submit_inference(
        payload, deployment_name=get_name_from_slug(deployment_slug)
    )


@deployment_router.post("/responses", response_model=None)
async def deployment_responses(
    deployment_slug: str,
    inference: InferenceServiceDep,
    payload: OpenAIResponsesPayload,
) -> StreamingResponse | JSONResponse:
    return await inference.submit_inference(
        payload, deployment_name=get_name_from_slug(deployment_slug)
    )


@deployment_router.post("/messages", response_model=None)
async def deployment_messages(
    deployment_slug: str,
    inference: InferenceServiceDep,
    payload: AnthropicMessagesPayload,
) -> StreamingResponse | JSONResponse:
    return await inference.submit_inference(
        payload, deployment_name=get_name_from_slug(deployment_slug)
    )


@deployment_router.post("/embeddings", response_model=None)
async def deployment_embeddings(
    deployment_slug: str,
    inference: InferenceServiceDep,
    payload: OpenAIEmbeddingsPayload,
) -> StreamingResponse | JSONResponse:
    return await inference.submit_inference(
        payload, deployment_name=get_name_from_slug(deployment_slug)
    )


@deployment_router.post("/tasks", response_model=None)
async def deployment_tasks(
    deployment_slug: str,
    inference: InferenceServiceDep,
    payload: GenericTaskPayload,
) -> StreamingResponse | JSONResponse:
    return await inference.submit_inference(
        payload, deployment_name=get_name_from_slug(deployment_slug)
    )


@deployment_router.get("/models", response_model=ModelListResponse)
async def list_deployment_models(
    deployment_slug: str, config: RouterConfigDep, user: AuthUser
) -> ModelListResponse:
    """List the model served by this deployment (visible to the caller)."""
    deployment_name = get_name_from_slug(deployment_slug)
    return ModelListResponse(
        data=[
            _model_info(m)
            for m in _visible_models(config, user)
            if any(d.name == deployment_name for d in m.deployments)
        ]
    )


router = APIRouter()
router.include_router(federated_router)
router.include_router(deployment_router)
