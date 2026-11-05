from typing import Any, ClassVar, Literal

from pydantic import Field

from .base import BasePayload
from .token_estimation import (
    CHARS_PER_TOKEN,
    TokenEstimate,
    estimate_llm_tokens,
)


# https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create
class OpenAIChatCompletionsPayload(BasePayload):
    endpoint: ClassVar[Literal["chat/completions"]] = "chat/completions"
    messages: list[Any]
    stream: bool | None = Field(default=False)
    max_completion_tokens: int | None = Field(default=None, ge=1)
    max_tokens: int | None = Field(default=None, ge=1)  # Legacy but common
    tools: Any = Field(default=None)

    def estimate_tokens(
        self,
        max_context: int | None,
        *,
        chars_per_token: float | None = None,
        output_estimate: float | None = None,
    ) -> TokenEstimate:
        return estimate_llm_tokens(
            self.messages,
            tools=self.tools,
            max_context=max_context,
            max_output=self.max_completion_tokens or self.max_tokens,
            chars_per_token=chars_per_token,
            output_estimate=output_estimate,
        )


# https://platform.openai.com/docs/api-reference/responses/create
class OpenAIResponsesPayload(BasePayload):
    endpoint: ClassVar[Literal["responses"]] = "responses"
    input: Any
    stream: bool | None = Field(default=False)
    max_output_tokens: int | None = Field(default=None, ge=1)
    tools: Any = Field(default=None)
    instructions: Any = Field(default=None)

    def estimate_tokens(
        self,
        max_context: int | None,
        *,
        chars_per_token: float | None = None,
        output_estimate: float | None = None,
    ) -> TokenEstimate:
        return estimate_llm_tokens(
            self.input,
            self.instructions,
            tools=self.tools,
            max_context=max_context,
            max_output=self.max_output_tokens,
            chars_per_token=chars_per_token,
            output_estimate=output_estimate,
        )


# https://docs.claude.com/en/api/messages
class AnthropicMessagesPayload(BasePayload):
    endpoint: ClassVar[Literal["messages"]] = "messages"
    messages: list[Any]
    system: str | list[Any] | None = Field(default=None)
    stream: bool | None = Field(default=False)
    # Required by the Anthropic API, so the output estimate is bounded by the
    # client's own cap (and remaining context); a learned estimate only lowers
    # it -- the DEFAULT_OUTPUT_ESTIMATE fallback never applies here.
    max_tokens: int = Field(ge=1)
    tools: Any = Field(default=None)  # untyped pass-through, estimator-only

    def estimate_tokens(
        self,
        max_context: int | None,
        *,
        chars_per_token: float | None = None,
        output_estimate: float | None = None,
    ) -> TokenEstimate:
        return estimate_llm_tokens(
            self.messages,
            self.system,
            tools=self.tools,
            max_context=max_context,
            max_output=self.max_tokens,
            chars_per_token=chars_per_token,
            output_estimate=output_estimate,
        )


# https://platform.openai.com/docs/api-reference/embeddings/create
class OpenAIEmbeddingsPayload(BasePayload):
    endpoint: ClassVar[Literal["embeddings"]] = "embeddings"
    input: str | list[str] | list[int] | list[list[int]]

    def estimate_tokens(
        self,
        max_context: int | None,
        *,
        chars_per_token: float | None = None,  # noqa: ARG002 (N/A for embeddings)
        output_estimate: float | None = None,  # noqa: ARG002 (N/A for embeddings)
    ) -> TokenEstimate:
        # Embeddings produce no completion tokens and their inputs are often
        # pre-tokenized, so neither learned parameter applies here.
        inp = self.input
        chars = 0
        if isinstance(inp, str):
            chars = len(inp)
            tokens = chars // CHARS_PER_TOKEN
        elif inp and isinstance(inp[0], int):
            # Already tokenized: count tokens directly, don't measure the repr.
            tokens = len(inp)
        elif inp and isinstance(inp[0], list):
            # Batch of token arrays.
            tokens = sum(len(arr) for arr in inp)
        else:
            # Batch of strings (or empty input).
            chars = sum(len(s) for s in inp)  # type: ignore
            tokens = chars // CHARS_PER_TOKEN
        tokens = max(1, tokens)
        return TokenEstimate(
            total_estimated_tokens=min(max_context, tokens) if max_context else tokens,
            input_chars=chars,
        )
