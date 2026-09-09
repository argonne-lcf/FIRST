"""Unit tests for preflight token estimation across all LLM API payloads.

These are pure-unit tests over ``first_common`` schema only -- no DB required.
"""

import pytest

from first_common.schema.endpoints.base import BasePayload
from first_common.schema.endpoints.llm import (
    AnthropicMessagesPayload,
    OpenAIChatCompletionsPayload,
    OpenAIEmbeddingsPayload,
    OpenAIResponsesPayload,
)
from first_common.schema.endpoints.token_estimation import (
    CHARS_PER_TOKEN,
    DEFAULT_OUTPUT_ESTIMATE,
    IMAGE_TOKEN_ESTIMATE,
    MAX_OUTPUT_ESTIMATE,
    count_input,
    count_tool_chars,
    estimate_input_tokens,
    estimate_tool_tokens,
    estimate_total_tokens,
)

# A base64-ish blob that must NOT be counted toward the char total.
BLOB = "A" * 100_000


# --------------------------------------------------------------------------
# Low-level helpers
# --------------------------------------------------------------------------


def test_estimate_input_tokens_counts_string_chars() -> None:
    text = "x" * (CHARS_PER_TOKEN * 10)
    assert estimate_input_tokens([{"role": "user", "content": text}]) == 10


def test_estimate_input_tokens_floor_is_one() -> None:
    # Empty / whitespace-only content still costs at least one token.
    assert estimate_input_tokens([]) == 1
    assert estimate_input_tokens([{"role": "user", "content": ""}]) == 1


def test_estimate_input_tokens_multiple_nodes_accumulate() -> None:
    a = [{"role": "user", "content": "y" * (CHARS_PER_TOKEN * 3)}]
    b = "z" * (CHARS_PER_TOKEN * 2)
    assert estimate_input_tokens(a, b) == 5


def test_estimate_input_tokens_none_node_contributes_nothing() -> None:
    msgs = [{"role": "user", "content": "w" * (CHARS_PER_TOKEN * 4)}]
    assert estimate_input_tokens(msgs, None) == 4


@pytest.mark.parametrize("part_type", ["image", "image_url", "input_image"])
def test_image_parts_add_fixed_estimate_and_ignore_blob(part_type: str) -> None:
    """Image parts cost a flat estimate; their base64 payload is not counted."""
    msgs = [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "hi"},
                {
                    "type": part_type,
                    "image_url": {"url": f"data:image/png;base64,{BLOB}"},
                },
            ],
        }
    ]
    # "hi" (2 chars -> 0 tokens) + one image, floored to at least 1.
    assert estimate_input_tokens(msgs) == IMAGE_TOKEN_ESTIMATE


def test_anthropic_image_source_data_excluded() -> None:
    msgs = [
        {
            "role": "user",
            "content": [{"type": "image", "source": {"data": BLOB}}],
        }
    ]
    assert estimate_input_tokens(msgs) == IMAGE_TOKEN_ESTIMATE


def test_tool_call_arguments_counted() -> None:
    args = "q" * (CHARS_PER_TOKEN * 5)
    msgs = [
        {
            "role": "assistant",
            "tool_calls": [
                {"type": "function", "function": {"name": "f", "arguments": args}}
            ],
        }
    ]
    # name "f" (1 char) + args (20 chars) = 21 chars -> 5 tokens.
    assert estimate_input_tokens(msgs) == (1 + CHARS_PER_TOKEN * 5) // CHARS_PER_TOKEN


def test_scan_tolerates_unknown_shapes() -> None:
    # Numbers, bools, and unknown dict keys contribute nothing (no raise).
    assert estimate_input_tokens([1, 2.5, True, {"unknown": "ignored"}]) == 1


# --------------------------------------------------------------------------
# estimate_tool_tokens
# --------------------------------------------------------------------------


def test_estimate_tool_tokens_none_and_empty_are_zero() -> None:
    assert estimate_tool_tokens(None) == 0
    assert estimate_tool_tokens([]) == 0


def test_estimate_tool_tokens_measures_repr() -> None:
    tools = [{"type": "function"}]
    assert estimate_tool_tokens(tools) == len(str(tools)) // CHARS_PER_TOKEN


# --------------------------------------------------------------------------
# estimate_total_tokens (output estimate + context clamping)
# --------------------------------------------------------------------------


def test_total_uses_default_output_when_uncapped() -> None:
    assert estimate_total_tokens(100, max_context=None, max_output=None) == (
        100 + DEFAULT_OUTPUT_ESTIMATE
    )


def test_total_uses_client_cap_when_given() -> None:
    assert estimate_total_tokens(100, max_context=None, max_output=50) == 150


def test_total_clamps_output_to_remaining_context() -> None:
    # Only 30 tokens of context remain, so a 500-token output is clamped to 30.
    assert estimate_total_tokens(70, max_context=100, max_output=500) == 100


def test_total_output_never_negative_when_input_exceeds_context() -> None:
    # Input already over context -> output clamped to 0, total == input.
    assert estimate_total_tokens(200, max_context=100, max_output=500) == 200


def test_total_default_output_also_clamped() -> None:
    assert estimate_total_tokens(10, max_context=20, max_output=None) == 20


# --------------------------------------------------------------------------
# Payload.estimate_tokens: one path per LLM API
# --------------------------------------------------------------------------


def test_base_payload_defaults_to_zero() -> None:
    class Dummy(BasePayload):
        endpoint = "dummy"

    assert Dummy(model="m").estimate_tokens(1000) == 0
    assert Dummy(model="m").estimate_tokens(None) == 0


def test_openai_chat_completions_input_plus_default_output() -> None:
    payload = OpenAIChatCompletionsPayload(
        model="m",
        messages=[{"role": "user", "content": "h" * (CHARS_PER_TOKEN * 8)}],
    )
    assert payload.estimate_tokens(None) == 8 + DEFAULT_OUTPUT_ESTIMATE


def test_openai_chat_completions_prefers_max_completion_tokens() -> None:
    payload = OpenAIChatCompletionsPayload(
        model="m",
        messages=[{"role": "user", "content": "hi"}],
        max_completion_tokens=42,
        max_tokens=999,
    )
    # input "hi" -> 0 chars//4 floored to 1; output prefers max_completion_tokens.
    assert payload.estimate_tokens(None) == 1 + 42


def test_openai_chat_completions_falls_back_to_legacy_max_tokens() -> None:
    payload = OpenAIChatCompletionsPayload(
        model="m",
        messages=[{"role": "user", "content": "hi"}],
        max_tokens=7,
    )
    assert payload.estimate_tokens(None) == 1 + 7


def test_openai_chat_completions_includes_tools() -> None:
    tools = [{"type": "function", "function": {"name": "search"}}]
    payload = OpenAIChatCompletionsPayload(
        model="m",
        messages=[{"role": "user", "content": "hi"}],
        max_tokens=10,
        tools=tools,
    )
    expected_input = 1 + estimate_tool_tokens(tools)
    assert payload.estimate_tokens(None) == expected_input + 10


def test_openai_responses_counts_input_and_instructions() -> None:
    payload = OpenAIResponsesPayload(
        model="m",
        input="a" * (CHARS_PER_TOKEN * 4),
        instructions="b" * (CHARS_PER_TOKEN * 2),
        max_output_tokens=5,
    )
    assert payload.estimate_tokens(None) == (4 + 2) + 5


def test_openai_responses_default_output() -> None:
    payload = OpenAIResponsesPayload(model="m", input="hello")
    assert payload.estimate_tokens(None) == 1 + DEFAULT_OUTPUT_ESTIMATE


def test_anthropic_messages_uses_required_max_tokens() -> None:
    payload = AnthropicMessagesPayload(
        model="m",
        messages=[{"role": "user", "content": "c" * (CHARS_PER_TOKEN * 6)}],
        system="s" * (CHARS_PER_TOKEN * 2),
        max_tokens=100,
    )
    assert payload.estimate_tokens(None) == (6 + 2) + 100


def test_anthropic_messages_max_tokens_is_required() -> None:
    with pytest.raises(ValueError):
        AnthropicMessagesPayload(  # type: ignore[call-arg]
            model="m", messages=[{"role": "user", "content": "hi"}]
        )


def test_anthropic_messages_clamped_by_context() -> None:
    payload = AnthropicMessagesPayload(
        model="m",
        messages=[{"role": "user", "content": "x" * (CHARS_PER_TOKEN * 10)}],
        max_tokens=1000,
    )
    # input=10, context=50 -> output clamped to 40, total 50.
    assert payload.estimate_tokens(50) == 50


# --------------------------------------------------------------------------
# Embeddings: distinct input shapes
# --------------------------------------------------------------------------


def test_embeddings_single_string() -> None:
    payload = OpenAIEmbeddingsPayload(model="m", input="e" * (CHARS_PER_TOKEN * 9))
    assert payload.estimate_tokens(None) == 9


def test_embeddings_list_of_strings() -> None:
    payload = OpenAIEmbeddingsPayload(
        model="m",
        input=["a" * CHARS_PER_TOKEN * 3, "b" * CHARS_PER_TOKEN * 2],
    )
    # (12 + 8) chars // 4 = 5 tokens.
    assert payload.estimate_tokens(None) == 5


def test_embeddings_pretokenized_ids_counted_directly() -> None:
    payload = OpenAIEmbeddingsPayload(model="m", input=[1, 2, 3, 4, 5])
    assert payload.estimate_tokens(None) == 5


def test_embeddings_batch_of_token_arrays() -> None:
    payload = OpenAIEmbeddingsPayload(model="m", input=[[1, 2, 3], [4, 5]])
    assert payload.estimate_tokens(None) == 5


def test_embeddings_empty_input_floored_to_one() -> None:
    payload = OpenAIEmbeddingsPayload(model="m", input=[])
    assert payload.estimate_tokens(None) == 1


def test_embeddings_clamped_by_context() -> None:
    payload = OpenAIEmbeddingsPayload(model="m", input=[1, 2, 3, 4, 5])
    assert payload.estimate_tokens(3) == 3


def test_embeddings_never_adds_output_estimate() -> None:
    # Embeddings produce no completion tokens; estimate must equal the input.
    payload = OpenAIEmbeddingsPayload(model="m", input="z" * (CHARS_PER_TOKEN * 7))
    assert payload.estimate_tokens(None) == 7


def test_embeddings_ignore_learned_params() -> None:
    # The learned params are accepted for interface uniformity but do nothing.
    payload = OpenAIEmbeddingsPayload(model="m", input="z" * (CHARS_PER_TOKEN * 7))
    assert payload.estimate_tokens(None, chars_per_token=2.0, output_estimate=999) == 7


# --------------------------------------------------------------------------
# Learned chars-per-token ratio (input side)
# --------------------------------------------------------------------------


def test_learned_chars_per_token_scales_input_estimate() -> None:
    # 40 chars at the default ratio (4) = 10 tokens; at a learned 2.0 = 20.
    msgs = [{"role": "user", "content": "x" * 40}]
    assert estimate_input_tokens(msgs) == 10
    assert estimate_input_tokens(msgs, chars_per_token=2.0) == 20


def test_learned_chars_per_token_zero_falls_back_to_default() -> None:
    # A falsy ratio must not divide-by-zero; it falls back to CHARS_PER_TOKEN.
    msgs = [{"role": "user", "content": "x" * 40}]
    assert estimate_input_tokens(msgs, chars_per_token=0.0) == 10


def test_learned_chars_per_token_applies_to_tools() -> None:
    tools = [{"type": "function", "function": {"name": "search"}}]
    chars = count_tool_chars(tools)
    assert estimate_tool_tokens(tools, chars_per_token=2.0) == int(chars / 2.0)


def test_learned_chars_per_token_leaves_image_estimate_flat() -> None:
    # A steeper ratio scales text but the flat per-image cost is untouched.
    msgs = [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "x" * 40},
                {
                    "type": "image_url",
                    "image_url": {"url": "data:image/png;base64,AAA"},
                },
            ],
        }
    ]
    # 40 chars / 2.0 = 20 text tokens + one flat image estimate.
    assert estimate_input_tokens(msgs, chars_per_token=2.0) == 20 + IMAGE_TOKEN_ESTIMATE


def test_chat_completions_uses_learned_chars_per_token() -> None:
    payload = OpenAIChatCompletionsPayload(
        model="m",
        messages=[{"role": "user", "content": "x" * 40}],
        max_tokens=5,
    )
    # 40 chars / 2.0 = 20 input tokens + 5 output.
    assert payload.estimate_tokens(None, chars_per_token=2.0) == 25


# --------------------------------------------------------------------------
# Learned output estimate (output side)
# --------------------------------------------------------------------------


def test_output_estimate_replaces_default_when_uncapped() -> None:
    assert (
        estimate_total_tokens(
            100, max_context=None, max_output=None, output_estimate=300
        )
        == 400
    )


def test_output_estimate_never_exceeds_client_cap() -> None:
    # Learned 5000 but client capped at 500 -> reserve only up to the cap.
    assert (
        estimate_total_tokens(
            100, max_context=None, max_output=500, output_estimate=5000
        )
        == 600
    )


def test_output_estimate_below_cap_shrinks_reservation() -> None:
    # Learned 200 under a client cap of 5000 -> reserve the smaller learned value.
    assert (
        estimate_total_tokens(
            100, max_context=None, max_output=5000, output_estimate=200
        )
        == 300
    )


def test_output_estimate_clamped_by_context() -> None:
    assert (
        estimate_total_tokens(70, max_context=100, max_output=None, output_estimate=500)
        == 100
    )


def test_output_estimate_clamped_by_global_max() -> None:
    assert (
        estimate_total_tokens(
            100, max_context=None, max_output=None, output_estimate=10**9
        )
        == 100 + MAX_OUTPUT_ESTIMATE
    )


def test_chat_completions_uses_learned_output_estimate() -> None:
    payload = OpenAIChatCompletionsPayload(
        model="m",
        messages=[{"role": "user", "content": "hi"}],
        max_tokens=5000,
    )
    # input "hi" -> 1 token; learned output 42 < cap 5000.
    assert payload.estimate_tokens(None, output_estimate=42) == 1 + 42


def test_anthropic_learned_output_below_required_max_tokens() -> None:
    payload = AnthropicMessagesPayload(
        model="m",
        messages=[{"role": "user", "content": "c" * (CHARS_PER_TOKEN * 6)}],
        max_tokens=1000,
    )
    # Learned 30 output shrinks the reservation below the required max_tokens.
    assert payload.estimate_tokens(None, output_estimate=30) == 6 + 30


# --------------------------------------------------------------------------
# input_basis: char/image basis carried to settlement
# --------------------------------------------------------------------------


def test_base_payload_input_basis_is_zero() -> None:
    class Dummy(BasePayload):
        endpoint = "dummy"

    assert Dummy(model="m").input_basis() == (0, 0)


def test_count_input_matches_input_basis_for_chat() -> None:
    msgs = [{"role": "user", "content": "x" * 40}]
    tools = [{"type": "function", "function": {"name": "s"}}]
    payload = OpenAIChatCompletionsPayload(model="m", messages=msgs, tools=tools)
    expected_chars = count_input(msgs)[0] + count_tool_chars(tools)
    assert payload.input_basis() == (expected_chars, 0)


def test_input_basis_counts_images() -> None:
    msgs = [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "hello"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,AA"}},
            ],
        }
    ]
    payload = OpenAIChatCompletionsPayload(model="m", messages=msgs)
    chars, images = payload.input_basis()
    assert chars == len("hello")
    assert images == 1


def test_anthropic_input_basis_includes_system_and_tools() -> None:
    payload = AnthropicMessagesPayload(
        model="m",
        messages=[{"role": "user", "content": "c" * 12}],
        system="s" * 8,
        max_tokens=10,
        tools=[{"type": "function", "function": {"name": "f"}}],
    )
    chars, images = payload.input_basis()
    assert chars == 12 + 8 + count_tool_chars(payload.tools)
    assert images == 0
