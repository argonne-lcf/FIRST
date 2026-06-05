from typing import Any, List, Literal

from pydantic import BaseModel, ConfigDict, Field


class BaseModelExtraAllow(BaseModel):
    model_config = ConfigDict(extra="allow")


class ResponsesInputItem(BaseModelExtraAllow):
    type: str | None = None
    role: str | None = None
    content: Any | None = None


class ResponsesReasoning(BaseModelExtraAllow):
    effort: str | None = None
    summary: str | None = None


class ResponsesTextFormat(BaseModelExtraAllow):
    format: dict[str, Any] | None = None


# https://platform.openai.com/docs/api-reference/responses/create
class OpenAIResponsesPydantic(BaseModelExtraAllow):
    openai_endpoint: Literal["responses"] = "responses"
    model: str = Field(..., min_length=1)
    input: str | List[ResponsesInputItem] | List[dict[str, Any]]
    instructions: str | None = None
    max_output_tokens: int | None = Field(default=None, ge=0)
    metadata: dict[str, Any] | None = None
    parallel_tool_calls: bool | None = None
    previous_response_id: str | None = None
    reasoning: ResponsesReasoning | None = None
    store: bool | None = None
    stream: bool | None = Field(default=False)
    temperature: float | None = Field(default=None, ge=0, le=2)
    text: ResponsesTextFormat | None = None
    tool_choice: str | dict[str, Any] | None = None
    tools: List[dict[str, Any]] | None = None
    top_p: float | None = Field(default=None, ge=0, le=1)
    truncation: str | None = None
    include: List[str] | None = None
    user: str | None = None
