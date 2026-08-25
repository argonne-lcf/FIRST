from typing import Any

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from first_common.schema.endpoints.llm import (
    OpenAIChatCompletionsPayload,
    OpenAIEmbeddingsPayload,
    OpenAIResponsesPayload,
)

from ....services.orchestration import get_name_from_slug
from ....services.submit_inference import submit_inference_with_retry
from ...dependencies import (
    AdmissionControllerDep,
    AuthUser,
    BackendClientManagerDep,
    RequestId,
)
from .dependencies import AuthorizedModel

router = APIRouter(prefix="/deployments/{deployment_slug}/v1")


@router.post("/chat/completions", response_model=None)
async def chat_completions(
    deployment_slug: str,
    user: AuthUser,
    model: AuthorizedModel,
    admission_controller: AdmissionControllerDep,
    backend_client_manager: BackendClientManagerDep,
    payload: OpenAIChatCompletionsPayload,
    request_id: RequestId,
) -> StreamingResponse | dict[str, Any]:
    return await submit_inference_with_retry(
        user,
        model,
        admission_controller,
        backend_client_manager,
        payload,
        request_id,
        deployment_name=get_name_from_slug(deployment_slug),
    )


@router.post("/responses", response_model=None)
async def responses(
    deployment_slug: str,
    user: AuthUser,
    model: AuthorizedModel,
    admission_controller: AdmissionControllerDep,
    backend_client_manager: BackendClientManagerDep,
    payload: OpenAIResponsesPayload,
    request_id: RequestId,
) -> StreamingResponse | dict[str, Any]:
    return await submit_inference_with_retry(
        user,
        model,
        admission_controller,
        backend_client_manager,
        payload,
        request_id,
        deployment_name=get_name_from_slug(deployment_slug),
    )


@router.post("/embeddings", response_model=None)
async def embeddings(
    deployment_slug: str,
    user: AuthUser,
    model: AuthorizedModel,
    admission_controller: AdmissionControllerDep,
    backend_client_manager: BackendClientManagerDep,
    payload: OpenAIEmbeddingsPayload,
    request_id: RequestId,
) -> StreamingResponse | dict[str, Any]:
    return await submit_inference_with_retry(
        user,
        model,
        admission_controller,
        backend_client_manager,
        payload,
        request_id,
        deployment_name=get_name_from_slug(deployment_slug),
    )
