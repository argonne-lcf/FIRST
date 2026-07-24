from enum import Enum
from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)


class BaseModelAllowExtra(BaseModel):
    model_config = ConfigDict(extra="allow")


class BasePayload(BaseModelAllowExtra):
    model: str = Field(..., min_length=1)


class ChatCompletionsMessage(BaseModelAllowExtra):
    role: str
    content: Any


class ResponsesInputItem(BaseModelAllowExtra):
    type: str | None = None
    role: str | None = None
    content: Any | None = None


# https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create
class OpenAIChatCompletionsPayload(BasePayload):
    """OpenAI chat/completions spec."""

    messages: list[ChatCompletionsMessage]
    stream: bool | None = Field(default=False)


# https://platform.openai.com/docs/api-reference/responses/create
class OpenAIResponsesPayload(BasePayload):
    """OpenAI responses spec."""

    input: str | list[ResponsesInputItem] | list[dict[str, Any]]
    stream: bool | None = Field(default=False)


# https://platform.openai.com/docs/api-reference/embeddings/create
class OpenAIEmbeddingsPayload(BasePayload):
    """OpenAI embeddings spec."""

    input: str | list[str] | list[int] | list[list[int]]


class OpenAIEndpoints(Enum):
    chat_completions = "chat/completions"
    responses = "response"
    embeddings = "embeddings"
