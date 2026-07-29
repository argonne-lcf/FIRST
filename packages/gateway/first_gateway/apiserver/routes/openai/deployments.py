from typing import Any

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from first_common.schema.endpoints.openai import (
    OpenAIChatCompletionsPayload,
    OpenAIEmbeddingsPayload,
    OpenAIResponsesPayload,
)

from ....services.orchestration import get_backend, get_name_from_slug
from ....services.submit_inference import submit_inference
from ...dependencies import AdmissionControllerDep, AuthUser
from .dependencies import AuthorizedModel

router = APIRouter(prefix="/deployments/{deployment_slug}/v1")


@router.post("/chat/completions", response_model=None)
async def chat_completions(
    deployment_slug: str,
    user: AuthUser,
    payload: OpenAIChatCompletionsPayload,
    model: AuthorizedModel,
    admission_controller: AdmissionControllerDep,
) -> StreamingResponse | dict[str, Any]:
    backend = await get_backend(
        user,
        model,
        admission_controller,
        deployment_name=get_name_from_slug(deployment_slug),
    )
    return await submit_inference(backend, payload)


@router.post("/responses", response_model=None)
async def responses(
    deployment_slug: str,
    user: AuthUser,
    payload: OpenAIResponsesPayload,
    model: AuthorizedModel,
    admission_controller: AdmissionControllerDep,
) -> StreamingResponse | dict[str, Any]:
    backend = await get_backend(
        user,
        model,
        admission_controller,
        deployment_name=get_name_from_slug(deployment_slug),
    )
    return await submit_inference(backend, payload)


@router.post("/embeddings", response_model=None)
async def embeddings(
    deployment_slug: str,
    user: AuthUser,
    payload: OpenAIEmbeddingsPayload,
    model: AuthorizedModel,
    admission_controller: AdmissionControllerDep,
) -> StreamingResponse | dict[str, Any]:
    backend = await get_backend(
        user,
        model,
        admission_controller,
        deployment_name=get_name_from_slug(deployment_slug),
    )
    return await submit_inference(backend, payload)
