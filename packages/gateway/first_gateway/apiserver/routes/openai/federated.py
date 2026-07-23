from typing import Any

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from first_common.schema.endpoints.openai import (
    OpenAIChatCompletionsPayload,
    OpenAIResponsesPayload,
)

from ...dependencies import AuthUser
from .dependencies import ChatCompletionsModel, ResponsesModel

router = APIRouter(prefix="/federated/v1")


@router.post("/chat/completions")
async def chat_completions(
    user: AuthUser, payload: OpenAIChatCompletionsPayload, model: ChatCompletionsModel
) -> StreamingResponse | dict[str, Any]:
    raise NotImplementedError("Not implemented yet.")


@router.post("/responses")
async def responses(
    user: AuthUser, payload: OpenAIResponsesPayload, model: ResponsesModel
) -> StreamingResponse | dict[str, Any]:
    raise NotImplementedError("Not implemented yet.")
