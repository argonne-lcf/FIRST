from fastapi import APIRouter
from fastapi.responses import JSONResponse, StreamingResponse

from first_common.schema.endpoints.llm import (
    OpenAIChatCompletionsPayload,
    OpenAIEmbeddingsPayload,
    OpenAIResponsesPayload,
)

from ...inference import InferenceServiceDep

router = APIRouter(prefix="/federated/v1")


@router.post("/chat/completions", response_model=None)
async def chat_completions(
    inference: InferenceServiceDep,
    payload: OpenAIChatCompletionsPayload,
) -> StreamingResponse | JSONResponse:
    return await inference.submit_inference(payload)


@router.post("/responses", response_model=None)
async def responses(
    inference: InferenceServiceDep,
    payload: OpenAIResponsesPayload,
) -> StreamingResponse | JSONResponse:
    return await inference.submit_inference(payload)


@router.post("/embeddings", response_model=None)
async def embeddings(
    inference: InferenceServiceDep,
    payload: OpenAIEmbeddingsPayload,
) -> StreamingResponse | JSONResponse:
    return await inference.submit_inference(payload)
