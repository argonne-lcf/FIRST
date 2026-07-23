from enum import Enum
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


class BasePayload(BaseModelAllowExtra):
    """
    Properties shared by all OpenAI endpoints
    """

    model: str = Field(..., min_length=1)
    stream: bool | None = Field(default=False)


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
class OpenAIChatCompletionsPayload(BasePayload):
    """
    OpenAI chat/completions spec.
    """

    messages: list[ChatCompletionsMessage]


# https://platform.openai.com/docs/api-reference/responses/create
class OpenAIResponsesPayload(BasePayload):
    """
    OpenAI responses spec.
    """

    input: str | List[ResponsesInputItem] | List[dict[str, Any]]


class OpenAIEndpoints(Enum):
    chat_completions = "chat/completions"
    responses = "response"
