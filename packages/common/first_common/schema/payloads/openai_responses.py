from typing import Any, List, Literal

from pydantic import BaseModel, ConfigDict, Field


class BaseModelNoExtra(BaseModel):
    """
    Pydantic class that rejects extra arguments.
    """

    model_config = ConfigDict(extra="forbid")


class ResponsesInputItem(BaseModelNoExtra):
    type: str | None = None
    role: str | None = None
    content: Any | None = None


class ResponsesReasoning(BaseModelNoExtra):
    effort: str | None = None
    summary: str | None = None


class ResponsesTextFormat(BaseModelNoExtra):
    format: dict[str, Any] | None = None


# https://platform.openai.com/docs/api-reference/responses/create
class OpenAIResponsesPayload(BaseModelNoExtra):
    """
    OpenAI responses spec.
    """

    background: bool | None = None
    context_management: Any | None = None
    conversation: Any | None = None
    include: List[str] | None = None
    input: str | List[ResponsesInputItem] | List[dict[str, Any]]
    instructions: str | None = None
    max_output_tokens: int | None = Field(default=None, ge=16)
    max_tool_calls: int | None = Field(default=None, ge=0)
    metadata: dict[str, Any] | None = None
    model: str = Field(..., min_length=1)
    moderation: Any | None = None
    parallel_tool_calls: bool | None = None
    previous_response_id: str | None = None
    prompt: Any | None = None
    prompt_cache_key: str | None = None
    prompt_cache_retention: Literal["in_memory", "24h"] | None = None
    reasoning: ResponsesReasoning | None = None
    safety_identifier: str | None = Field(default=None, max_length=64)
    service_tier: Literal["auto", "default", "flex", "scale", "priority"] | None = None
    store: bool | None = None
    stream: bool | None = Field(default=False)
    stream_options: Any | None = None
    temperature: float | None = Field(default=None, ge=0, le=2)
    text: ResponsesTextFormat | None = None
    tool_choice: str | dict[str, Any] | None = None
    tools: List[dict[str, Any]] | None = None
    top_logprobs: int | None = Field(default=None, ge=0, le=20)
    top_p: float | None = Field(default=None, ge=0, le=1)
