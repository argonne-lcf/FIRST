from typing import Any

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from first_common.schema.endpoints.openai import (
    OpenAIChatCompletionsPayload,
    OpenAIEmbeddingsPayload,
    OpenAIResponsesPayload,
)

from ....services.orchestration import get_backend
from ....services.submit_inference import submit_inference
from ...dependencies import AdmissionControllerDep, AuthUser
from .dependencies import AuthorizedModel

router = APIRouter(prefix="/deployments/{deployment_name}/v1")


@router.post("/chat/completions", response_model=None)
async def chat_completions(
    deployment_name: str,
    user: AuthUser,
    payload: OpenAIChatCompletionsPayload,
    model: AuthorizedModel,
    admission_controler: AdmissionControllerDep,
) -> StreamingResponse | dict[str, Any]:
    backend = await get_backend(
        user, model, admission_controler, deployment_name=deployment_name
    )
    return await submit_inference(backend, payload)


@router.post("/responses", response_model=None)
async def responses(
    deployment_name: str,
    user: AuthUser,
    payload: OpenAIResponsesPayload,
    model: AuthorizedModel,
    admission_controler: AdmissionControllerDep,
) -> StreamingResponse | dict[str, Any]:
    backend = await get_backend(
        user, model, admission_controler, deployment_name=deployment_name
    )
    return await submit_inference(backend, payload)


@router.post("/embeddings", response_model=None)
async def embeddings(
    deployment_name: str,
    user: AuthUser,
    payload: OpenAIEmbeddingsPayload,
    model: AuthorizedModel,
    admission_controler: AdmissionControllerDep,
) -> StreamingResponse | dict[str, Any]:
    backend = await get_backend(
        user, model, admission_controler, deployment_name=deployment_name
    )
    return await submit_inference(backend, payload)
