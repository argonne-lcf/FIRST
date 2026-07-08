from typing import Annotated, Any

from common.first_common.schema.payloads.openai_chat_completions import (
    OpenAIChatCompletionsPayload,
)
from common.first_common.schema.payloads.openai_responses import OpenAIResponsesPayload
from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse

from ...database import models as db
from ...services.inference_submit import submit_openai_request
from ..auth import enforce_permission
from ..dependencies import AuthUser, DbSession
from .llm_router import router as llm_router

openai_router = APIRouter(prefix="/v1")

Payload = OpenAIChatCompletionsPayload | OpenAIResponsesPayload


async def check_model_permission(
    sess: DbSession,
    payload: Payload,
    user: AuthUser,
) -> None:
    model = await db.Model.get_by_name(sess, payload.model)
    enforce_permission(user, model.access_group)


@openai_router.post("/chat/completions")
async def chat_completions(
    payload: OpenAIChatCompletionsPayload,
    _: Annotated[None, Depends(check_model_permission)],
) -> StreamingResponse | dict[str, Any]:
    return await submit_openai_request(payload, llm_router.acompletion)


@openai_router.post("/responses")
async def responses(
    payload: OpenAIResponsesPayload,
    _: Annotated[None, Depends(check_model_permission)],
) -> StreamingResponse | dict[str, Any]:
    return await submit_openai_request(payload, llm_router.responses)
