from typing import Annotated, Any, Literal, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)


class BaseModelNoExtra(BaseModel):
    """
    Pydantic class that rejects extra arguments.
    """

    model_config = ConfigDict(extra="forbid")


class DeveloperMessage(BaseModelNoExtra):
    """
    Developer-provided instructions that the model should follow,
    regardless of messages sent by the user.
    """

    role: Literal["developer"] = "developer"
    content: Any
    name: str | None = None


class SystemMessage(BaseModelNoExtra):
    """
    System-provided instructions that the model should follow,
    regardless of messages sent by the user.
    """

    role: Literal["system"] = "system"
    content: Any
    name: str | None = None


class UserMessage(BaseModelNoExtra):
    """
    Messages sent by an end user, containing prompts or additional
    context information.
    """

    role: Literal["user"] = "user"
    content: Any
    name: str | None = None


class AssistantMessage(BaseModelNoExtra):
    """
    Messages sent by the model in response to user messages.
    """

    role: Literal["assistant"] = "assistant"
    content: Any
    name: str | None = None
    audio: Any | None = None
    refusal: str | None = None
    tool_calls: Any | None = None


class ToolMessage(BaseModelNoExtra):
    """
    Tool message.
    """

    role: Literal["tool"] = "tool"
    content: Any
    tool_call_id: str


class FunctionMessage(BaseModelNoExtra):
    """
    Function message.
    """

    role: Literal["function"] = "function"
    content: str
    name: str


ChatMessage = Annotated[
    Union[
        DeveloperMessage,
        SystemMessage,
        UserMessage,
        AssistantMessage,
        ToolMessage,
        FunctionMessage,
    ],
    Field(discriminator="role"),
]


# https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create
class OpenAIChatCompletionsPayload(BaseModelNoExtra):
    """
    OpenAI chat/completions spec.
    """

    messages: list[ChatMessage]
    model: str = Field(..., min_length=1)
    audio: Any | None = None
    frequency_penalty: float | None = Field(default=None, ge=-2, le=2)
    logit_bias: Any | None = None
    logprobs: bool | None = None
    max_completion_tokens: int | None = Field(default=None, ge=1)
    metadata: Any | None = None
    modalities: list[Literal["text", "audio"]] | None = None
    moderation: Any | None = None
    n: int | None = Field(default=None, ge=1, le=128)
    parallel_tool_calls: bool | None = None
    prediction: Any | None = None
    presence_penalty: float | None = Field(default=None, ge=-2, le=2)
    prompt_cache_key: str | None = None
    prompt_cache_retention: Literal["in_memory", "24h"] | None = None
    reasoning_effort: (
        Literal["none", "minimal", "low", "medium", "high", "xhigh"] | None
    ) = None
    response_format: Any | None = None
    safety_identifier: str | None = Field(default=None, max_length=64)
    service_tier: Literal["auto", "default", "flex", "scale", "priority"] | None = None
    stop: str | list[str] | None = None
    store: bool | None = None
    stream: bool | None = Field(default=False)
    stream_options: Any | None = None
    temperature: float | None = Field(default=None, ge=0, le=2)
    tool_choice: Any | None = None
    tools: Any | None = None
    top_logprobs: int | None = Field(default=None, ge=0, le=20)
    top_p: float | None = Field(default=None, ge=0, le=1)
    verbosity: Literal["low", "medium", "high"] | None = None
    web_search_options: Any | None = None
