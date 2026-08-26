"""Preflight token estimation for gateway admission control.

Estimates are intentionally cheap and rough: actual usage is settled after the
request completes.  We bias toward *modest* output estimates to avoid spuriously
rejecting user requests.
"""

from typing import Any

CHARS_PER_TOKEN = 4
DEFAULT_OUTPUT_ESTIMATE = 2048
MAX_OUTPUT_ESTIMATE = 32768
IMAGE_TOKEN_ESTIMATE = 1000


# Content-part types (across OpenAI chat, OpenAI Responses, and Anthropic
# Messages) that represent an image.
_IMAGE_PART_TYPES = frozenset({"image", "image_url", "input_image"})

# Dict keys worth descending into when scanning message structures.  Anything
# not listed here -- "source", "image_url", "data", "cache_control", ids,
# metadata -- is ignored, which is what keeps base64 payloads out of the
# character count.  "output" carries tool results in Responses API.
_RECURSE_KEYS = ("content", "text", "system", "function", "tool_calls", "output")
_TOOL_INPUT_KEYS = ("input", "arguments")


def _scan(node: Any) -> tuple[int, int]:
    """Return (text_chars, image_count) for an arbitrary message structure.

    Tolerant by design: unknown shapes contribute nothing rather than raising,
    since the gateway sees ``list[Any]`` and providers add part types over time.
    """
    chars = images = 0
    stack = [node]
    while stack:
        node = stack.pop()
        if isinstance(node, str):
            chars += len(node)
        elif isinstance(node, list):
            stack.extend(node)
        elif isinstance(node, dict):
            if node.get("type") in _IMAGE_PART_TYPES:
                images += 1
            for key in _RECURSE_KEYS:
                value = node.get(key)
                if value is not None:
                    stack.append(value)
            for key in _TOOL_INPUT_KEYS:
                value = node.get(key)
                if isinstance(value, str):
                    chars += len(value)
                elif value is not None:
                    chars += len(str(value))
    return chars, images


def estimate_input_tokens(*nodes: Any) -> int:
    """Estimate input tokens across one or more message structures."""
    chars = images = 0
    for node in nodes:
        c, i = _scan(node)
        chars += c
        images += i
    return max(1, chars // CHARS_PER_TOKEN + images * IMAGE_TOKEN_ESTIMATE)


def estimate_tool_tokens(tools: Any) -> int:
    """Estimate tokens consumed by tool/function definitions.

    Tool schemas are arbitrarily nested JSON that the whitelist walker cannot
    see inside of (keys like "parameters"/"input_schema" carry the bulk).
    Unlike messages they contain no base64 blobs, so measuring the repr is
    safe, cheap, and tracks the serialized size the chat template actually
    renders into the prompt.
    """
    if not tools:
        return 0
    return len(str(tools)) // CHARS_PER_TOKEN


def estimate_total_tokens(
    input_tokens: int, max_context: int | None, max_output: int | None
) -> int:
    """Combine input estimate with an output estimate.

    Output is the client's cap if given, else DEFAULT_OUTPUT_ESTIMATE, and in
    either case no more than the context window leaves room for.
    """
    if max_output is not None:
        output_tokens = min(max_output, MAX_OUTPUT_ESTIMATE)
    else:
        output_tokens = DEFAULT_OUTPUT_ESTIMATE

    if max_context is not None:
        output_tokens = min(output_tokens, max(0, max_context - input_tokens))

    return input_tokens + output_tokens
