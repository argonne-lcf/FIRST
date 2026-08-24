from typing import Any

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from first_common.schema.endpoints.openai import (
    OpenAIChatCompletionsPayload,
    OpenAIEmbeddingsPayload,
    OpenAIResponsesPayload,
)

from ....services.submit_inference import submit_inference_with_retry
from ...dependencies import AdmissionControllerDep, AuthUser, BackendClientManagerDep
from .dependencies import AuthorizedModel

router = APIRouter(prefix="/federated/v1")


@router.post("/chat/completions", response_model=None)
async def chat_completions(
    user: AuthUser,
    model: AuthorizedModel,
    admission_controller: AdmissionControllerDep,
    backend_client_manager: BackendClientManagerDep,
    payload: OpenAIChatCompletionsPayload,
) -> StreamingResponse | dict[str, Any]:
    return await submit_inference_with_retry(
        user,
        model,
        admission_controller,
        backend_client_manager,
        payload,
    )


@router.post("/responses", response_model=None)
async def responses(
    user: AuthUser,
    model: AuthorizedModel,
    admission_controller: AdmissionControllerDep,
    backend_client_manager: BackendClientManagerDep,
    payload: OpenAIResponsesPayload,
) -> StreamingResponse | dict[str, Any]:
    return await submit_inference_with_retry(
        user,
        model,
        admission_controller,
        backend_client_manager,
        payload,
    )


@router.post("/embeddings", response_model=None)
async def embeddings(
    user: AuthUser,
    model: AuthorizedModel,
    admission_controller: AdmissionControllerDep,
    backend_client_manager: BackendClientManagerDep,
    payload: OpenAIEmbeddingsPayload,
) -> StreamingResponse | dict[str, Any]:
    return await submit_inference_with_retry(
        user,
        model,
        admission_controller,
        backend_client_manager,
        payload,
    )
