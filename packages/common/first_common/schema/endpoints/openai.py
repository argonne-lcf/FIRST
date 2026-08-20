from enum import Enum
from typing import Any

from pydantic import Field

from .base import BasePayload


# https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create
class OpenAIChatCompletionsPayload(BasePayload):
    messages: list[Any]
    stream: bool | None = Field(default=False)


# https://platform.openai.com/docs/api-reference/responses/create
class OpenAIResponsesPayload(BasePayload):
    input: Any
    stream: bool | None = Field(default=False)


# https://platform.openai.com/docs/api-reference/embeddings/create
class OpenAIEmbeddingsPayload(BasePayload):
    input: str | list[str] | list[int] | list[list[int]]


class OpenAIEndpoints(str, Enum):
    chat_completions = "chat/completions"
    responses = "responses"
    embeddings = "embeddings"
