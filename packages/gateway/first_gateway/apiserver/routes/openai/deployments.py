from fastapi import APIRouter
from fastapi.responses import JSONResponse, StreamingResponse

from first_common.schema.endpoints.llm import (
    OpenAIChatCompletionsPayload,
    OpenAIEmbeddingsPayload,
    OpenAIResponsesPayload,
)

from ....services.orchestration import get_name_from_slug
from ...inference import InferenceServiceDep

router = APIRouter(prefix="/deployments/{deployment_slug}/v1")


@router.post("/chat/completions", response_model=None)
async def chat_completions(
    deployment_slug: str,
    inference: InferenceServiceDep,
    payload: OpenAIChatCompletionsPayload,
) -> StreamingResponse | JSONResponse:
    return await inference.submit_inference(
        payload, deployment_name=get_name_from_slug(deployment_slug)
    )


@router.post("/responses", response_model=None)
async def responses(
    deployment_slug: str,
    inference: InferenceServiceDep,
    payload: OpenAIResponsesPayload,
) -> StreamingResponse | JSONResponse:
    return await inference.submit_inference(
        payload, deployment_name=get_name_from_slug(deployment_slug)
    )


@router.post("/embeddings", response_model=None)
async def embeddings(
    deployment_slug: str,
    inference: InferenceServiceDep,
    payload: OpenAIEmbeddingsPayload,
) -> StreamingResponse | JSONResponse:
    return await inference.submit_inference(
        payload, deployment_name=get_name_from_slug(deployment_slug)
    )
