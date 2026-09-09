from typing import Any, Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field

OpenAIEndpoint: TypeAlias = Literal[
    "chat/completions", "completions", "embeddings", "responses", "messages"
]
CompletionInput: TypeAlias = str | list[str] | list[int] | list[list[int]]


class OpenAIControlFields(BaseModel):
    """Fields FIRST reads to route and account for an OpenAI request."""

    model_config = ConfigDict(extra="ignore", strict=True)

    model: str = Field(min_length=1)
    stream: bool | None = False


class ChatCompletionsControl(OpenAIControlFields):
    messages: list[dict[str, Any]]


class CompletionsControl(OpenAIControlFields):
    prompt: CompletionInput


class EmbeddingsControl(OpenAIControlFields):
    input: CompletionInput
    stream: Literal[False] | None = False


class ResponsesControl(OpenAIControlFields):
    input: str | list[dict[str, Any]]


class AnthropicMessagesControl(OpenAIControlFields):
    """FIRST control fields for the Anthropic Messages route."""

    messages: list[dict[str, Any]]


OPENAI_CONTROL_MODELS: dict[OpenAIEndpoint, type[OpenAIControlFields]] = {
    "chat/completions": ChatCompletionsControl,
    "completions": CompletionsControl,
    "embeddings": EmbeddingsControl,
    "responses": ResponsesControl,
    "messages": AnthropicMessagesControl,
}

OPENAI_PROMPT_FIELDS: dict[OpenAIEndpoint, str] = {
    "chat/completions": "messages",
    "completions": "prompt",
    "embeddings": "input",
    "responses": "input",
    "messages": "messages",
}

# These fields exist only inside FIRST's adapter protocol. A client may use the
# same names inside an opaque nested backend object; only top-level occurrences
# are rejected.
FIRST_RESERVED_OPENAI_FIELDS = frozenset(
    {
        "api_port",
        "cache_salt",
        "openai_endpoint",
        "stream_task_id",
        "stream_task_token",
        "streaming_server_host",
        "streaming_server_port",
        "streaming_server_protocol",
    }
)
