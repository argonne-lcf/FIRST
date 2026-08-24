from typing import Any, ClassVar, Literal

from pydantic import Field

from .base import BasePayload


# https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create
class OpenAIChatCompletionsPayload(BasePayload):
    endpoint: ClassVar[Literal["chat/completions"]] = "chat/completions"
    messages: list[Any]
    stream: bool | None = Field(default=False)


# https://platform.openai.com/docs/api-reference/responses/create
class OpenAIResponsesPayload(BasePayload):
    endpoint: ClassVar[Literal["responses"]] = "responses"
    input: Any
    stream: bool | None = Field(default=False)


# https://platform.openai.com/docs/api-reference/embeddings/create
class OpenAIEmbeddingsPayload(BasePayload):
    endpoint: ClassVar[Literal["embeddings"]] = "embeddings"
    input: str | list[str] | list[int] | list[list[int]]
