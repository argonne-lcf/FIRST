from typing import Any, List

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)


class BaseModelAllowExtra(BaseModel):
    """
    Pydantic class that allow extra arguments.
    """

    model_config = ConfigDict(extra="allow")


class ChatCompletionsMessage(BaseModelAllowExtra):
    """
    Core parameters of chat/completions Message objects.
    """

    role: str
    content: Any


class ResponsesInputItem(BaseModelAllowExtra):
    type: str | None = None
    role: str | None = None
    content: Any | None = None


# https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create
class OpenAIChatCompletionsPayload(BaseModelAllowExtra):
    """
    OpenAI chat/completions spec.
    """

    messages: list[ChatCompletionsMessage]
    model: str = Field(..., min_length=1)
    stream: bool | None = Field(default=False)


# https://platform.openai.com/docs/api-reference/responses/create
class OpenAIResponsesPayload(BaseModelAllowExtra):
    """
    OpenAI responses spec.
    """

    input: str | List[ResponsesInputItem] | List[dict[str, Any]]
    model: str = Field(..., min_length=1)
    stream: bool | None = Field(default=False)
