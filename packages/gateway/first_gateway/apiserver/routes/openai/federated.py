from typing import Any

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from first_common.schema.endpoints.openai import (
    OpenAIChatCompletionsPayload,
    OpenAIEmbeddingsPayload,
    OpenAIResponsesPayload,
)

from ...dependencies import AdmissionControllerDep, AuthUser
from .dependencies import AuthorizedModel

router = APIRouter(prefix="/federated/v1")


@router.post("/chat/completions", response_model=None)
async def chat_completions(
    user: AuthUser,
    payload: OpenAIChatCompletionsPayload,
    model: AuthorizedModel,
    admission_controler: AdmissionControllerDep,
) -> StreamingResponse | dict[str, Any]:
    # backend = await get_backend()
    # await submit_to_backend()
    raise NotImplementedError("Not implemented yet.")


@router.post("/responses", response_model=None)
async def responses(
    user: AuthUser,
    payload: OpenAIResponsesPayload,
    model: AuthorizedModel,
    admission_controler: AdmissionControllerDep,
) -> StreamingResponse | dict[str, Any]:
    raise NotImplementedError("Not implemented yet.")


@router.post("/embeddings", response_model=None)
async def embeddings(
    user: AuthUser,
    payload: OpenAIEmbeddingsPayload,
    model: AuthorizedModel,
    admission_controler: AdmissionControllerDep,
) -> StreamingResponse | dict[str, Any]:
    raise NotImplementedError("Not implemented yet.")
