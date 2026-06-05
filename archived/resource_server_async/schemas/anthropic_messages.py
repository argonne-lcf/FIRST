from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class BaseModelExtraAllow(BaseModel):
    model_config = ConfigDict(extra="allow")


class AnthropicMessage(BaseModelExtraAllow):
    role: Literal["user", "assistant"]
    content: str | list[dict[str, Any]]


class AnthropicSystemBlock(BaseModelExtraAllow):
    type: str | None = None
    text: str | None = None


class AnthropicMessagesPydantic(BaseModelExtraAllow):
    openai_endpoint: Literal["messages"] = "messages"

    model: str = Field(..., min_length=1)
    messages: list[AnthropicMessage]
    max_tokens: int = Field(..., ge=1)
    system: str | list[AnthropicSystemBlock] | None = None
    metadata: dict[str, Any] | None = None
    stop_sequences: list[str] | None = None
    stream: bool | None = Field(default=False)
    temperature: float | None = Field(default=None, ge=0, le=1)
    top_k: int | None = Field(default=None, ge=0)
    top_p: float | None = Field(default=None, ge=0, le=1)
    tool_choice: dict[str, Any] | None = None
    tools: list[dict[str, Any]] | None = None
    thinking: dict[str, Any] | None = None
    service_tier: str | None = None
    user: str | None = None
