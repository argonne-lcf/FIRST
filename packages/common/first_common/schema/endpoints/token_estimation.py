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


def count_input(*nodes: Any) -> tuple[int, int]:
    """Return raw ``(text_chars, image_count)`` across message structures.

    The unweighted measurements behind ``estimate_input_tokens``, exposed so
    settlement can recover the character basis a request was estimated from and
    calibrate the chars-per-token ratio against the real input token count.
    """
    chars = images = 0
    for node in nodes:
        c, i = _scan(node)
        chars += c
        images += i
    return chars, images


def count_tool_chars(tools: Any) -> int:
    """Serialized character count of tool/function definitions (see below)."""
    return len(str(tools)) if tools else 0


def estimate_input_tokens(*nodes: Any, chars_per_token: float | None = None) -> int:
    """Estimate input tokens across one or more message structures.

    ``chars_per_token`` overrides the default ratio with a value learned from
    this user+model's recent traffic; ``None`` falls back to CHARS_PER_TOKEN.
    """
    cpt = chars_per_token or CHARS_PER_TOKEN
    chars, images = count_input(*nodes)
    return max(1, int(chars / cpt) + images * IMAGE_TOKEN_ESTIMATE)


def estimate_tool_tokens(tools: Any, chars_per_token: float | None = None) -> int:
    """Estimate tokens consumed by tool/function definitions.

    Tool schemas are arbitrarily nested JSON that the whitelist walker cannot
    see inside of (keys like "parameters"/"input_schema" carry the bulk).
    Unlike messages they contain no base64 blobs, so measuring the repr is
    safe, cheap, and tracks the serialized size the chat template actually
    renders into the prompt.
    """
    cpt = chars_per_token or CHARS_PER_TOKEN
    return int(count_tool_chars(tools) / cpt)


def estimate_total_tokens(
    input_tokens: int,
    max_context: int | None,
    max_output: int | None,
    output_estimate: int | None = None,
) -> int:
    """Combine input estimate with an output estimate.

    The output estimate is, in order of preference: ``output_estimate`` (learned
    from this user+model's recent completions), else the client's cap, else
    DEFAULT_OUTPUT_ESTIMATE.  Whatever the source, it is never taken above the
    client's own cap, the global MAX_OUTPUT_ESTIMATE, or the room the context
    window leaves -- so a learned estimate only ever shrinks the modest
    over-reservation the client's cap would otherwise cause.
    """
    if output_estimate is not None:
        output_tokens = output_estimate
    elif max_output is not None:
        output_tokens = max_output
    else:
        output_tokens = DEFAULT_OUTPUT_ESTIMATE

    if max_output is not None:
        output_tokens = min(output_tokens, max_output)
    output_tokens = min(output_tokens, MAX_OUTPUT_ESTIMATE)

    if max_context is not None:
        output_tokens = min(output_tokens, max(0, max_context - input_tokens))

    return input_tokens + max(0, output_tokens)
