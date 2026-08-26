"""Token-usage extraction validated against real captured upstream responses.

Six cases: 3 dialects (chat/completions / responses / messages) x {unary, streaming}. The
streaming fixtures are byte-for-byte SSE wire captures; the unary fixtures are
the JSON response bodies.
"""

from pathlib import Path

import pytest

from first_gateway.services.usage import (
    USAGE_PARSERS,
    TokenUsage,
    UsageTap,
)

RESULTS = Path(__file__).parent / "sample-prompts"

# The captured prompt that exercises long-output streaming (real token counts).
_STREAM_CASE = "short_isl_long_osl"
_UNARY_CASE = "long_isl_short_osl"


def _read_unary(subdir: str) -> bytes:
    return (RESULTS / subdir / f"{_UNARY_CASE}_output.json").read_bytes()


def _read_stream(subdir: str) -> bytes:
    return (RESULTS / subdir / f"{_STREAM_CASE}_stream_output.txt").read_bytes()


# --------------------------------------------------------------------------
# Unary
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "protocol, expected",
    [
        (
            "chat/completions",
            TokenUsage(
                input_tokens=307,
                output_tokens=60,
                cache_read_tokens=None,
                reasoning_tokens=None,
                total_tokens=367,
            ),
        ),
        (
            "responses",
            TokenUsage(
                input_tokens=307,
                output_tokens=60,
                cache_read_tokens=288,
                reasoning_tokens=59,
                total_tokens=367,
            ),
        ),
        (
            "messages",
            TokenUsage(input_tokens=307, output_tokens=60),
        ),
    ],
)
def test_unary_usage(protocol: str, expected: TokenUsage) -> None:
    body = _read_unary(protocol)
    usage = USAGE_PARSERS[protocol].parse_unary(body)
    assert usage == expected


# --------------------------------------------------------------------------
# Streaming
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "protocol, expected",
    [
        (
            "chat/completions",
            TokenUsage(input_tokens=44, output_tokens=1200, total_tokens=1244),
        ),
        (
            "responses",
            TokenUsage(
                input_tokens=44,
                output_tokens=1200,
                cache_read_tokens=32,
                reasoning_tokens=818,
                total_tokens=1244,
            ),
        ),
        (
            "messages",
            TokenUsage(input_tokens=44, output_tokens=1200),
        ),
    ],
)
def test_stream_usage(protocol: str, expected: TokenUsage) -> None:
    raw = _read_stream(protocol)

    tap = UsageTap()
    tap.feed(raw)
    tap.close()

    usage = USAGE_PARSERS[protocol].parse_stream(tap.first, tap.last)
    assert usage == expected


# --------------------------------------------------------------------------
# The tap works chunk-by-chunk regardless of how bytes are split
# --------------------------------------------------------------------------


def test_tap_survives_arbitrary_chunk_boundaries() -> None:
    """A live relay feeds bytes in transport-sized pieces that split SSE
    events mid-line; framing must reassemble them identically."""
    raw = _read_stream("chat/completions")
    tap = UsageTap()
    for i in range(0, len(raw), 7):  # deliberately ugly chunk size
        tap.feed(raw[i : i + 7])
    tap.close()
    usage = USAGE_PARSERS["chat/completions"].parse_stream(tap.first, tap.last)
    assert usage == TokenUsage(input_tokens=44, output_tokens=1200, total_tokens=1244)
