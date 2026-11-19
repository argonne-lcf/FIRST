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
    usage_events,
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
            TokenUsage(input_tokens=307, output_tokens=60, total_tokens=367),
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
            TokenUsage(input_tokens=44, output_tokens=1200, total_tokens=1244),
        ),
    ],
)
def test_stream_usage(protocol: str, expected: TokenUsage) -> None:
    raw = _read_stream(protocol)
    usage = USAGE_PARSERS[protocol].parse_stream(*usage_events(raw))
    assert usage == expected


# --------------------------------------------------------------------------
# usage_events: one pass over the whole retained stream
# --------------------------------------------------------------------------

_CHAT_STREAM_USAGE = TokenUsage(input_tokens=44, output_tokens=1200, total_tokens=1244)


def test_usage_events_accepts_the_relay_bytearray() -> None:
    """The relay hands over its ``bytearray`` as is; events come back as bytes."""
    first, last = usage_events(bytearray(_read_stream("chat/completions")))
    assert type(first) is bytes and type(last) is bytes
    usage = USAGE_PARSERS["chat/completions"].parse_stream(first, last)
    assert usage == _CHAT_STREAM_USAGE


@pytest.mark.parametrize("protocol", ["chat/completions", "responses", "messages"])
def test_usage_events_are_whole_events(protocol: str) -> None:
    """Each returned slice is exactly one SSE event, delimiter excluded."""
    raw = _read_stream(protocol)
    events = raw.split(b"\n\n")
    first, last = usage_events(raw)
    assert first in events and last in events
    assert first == next(e for e in events if b"usage" in e)
    assert last == next(e for e in reversed(events) if b"usage" in e)


@pytest.mark.parametrize(
    "protocol, expected",
    [
        ("chat/completions", _CHAT_STREAM_USAGE),
        (
            "messages",
            TokenUsage(input_tokens=44, output_tokens=1200, total_tokens=1244),
        ),
    ],
)
def test_usage_events_crlf_delimiters(protocol: str, expected: TokenUsage) -> None:
    raw = _read_stream(protocol).replace(b"\n", b"\r\n")
    first, last = usage_events(raw)
    assert first is not None and last is not None
    assert b"\r\n\r\n" not in first and b"\r\n\r\n" not in last
    assert USAGE_PARSERS[protocol].parse_stream(first, last) == expected


@pytest.mark.parametrize("protocol", ["chat/completions", "responses", "messages"])
def test_usage_events_first_only_when_needed(protocol: str) -> None:
    """The relay skips the forward search for adapters that ignore ``first``;
    each adapter must parse identically either way it is called."""
    raw = _read_stream(protocol)
    adapter = USAGE_PARSERS[protocol]
    full = usage_events(raw)
    lean = usage_events(raw, first=adapter.needs_first)
    assert lean[1] == full[1]
    assert adapter.parse_stream(*lean) == adapter.parse_stream(*full)
    if not adapter.needs_first:
        assert lean[0] is None


def test_usage_events_none_without_usage() -> None:
    raw = b'data: {"choices":[{"delta":{"content":"hi"}}]}\n\ndata: [DONE]\n\n'
    assert usage_events(raw) == (None, None)
    assert usage_events(b"") == (None, None)


def test_usage_events_single_event_is_both_first_and_last() -> None:
    """Anthropic's adapter skips ``last`` when it is ``first``; a stream with
    a single usage-bearing event must satisfy that identity."""
    raw = b'data: {"a":1}\n\ndata: {"usage":{"total_tokens":3}}\n\ndata: [DONE]\n\n'
    first, last = usage_events(raw)
    assert first == b'data: {"usage":{"total_tokens":3}}'
    assert last is first


def test_usage_events_unterminated_final_event() -> None:
    """A stream that ends without its trailing blank line still yields the
    final event, as the tap's close() flush used to."""
    raw = _read_stream("chat/completions").rstrip(b"\n")
    raw = raw.removesuffix(b"data: [DONE]").rstrip(b"\n")
    usage = USAGE_PARSERS["chat/completions"].parse_stream(*usage_events(raw))
    assert usage == _CHAT_STREAM_USAGE


@pytest.mark.parametrize("protocol", ["chat/completions", "responses", "messages"])
def test_usage_events_truncated_stream(protocol: str) -> None:
    """A backend that breaks the stream mid-event leaves a partial final event;
    it parses to nothing rather than raising, and settlement treats the
    usage as unknown."""
    raw = _read_stream(protocol)
    cut = raw[: raw.rfind(b"usage") + 10]  # inside the last usage event
    usage = USAGE_PARSERS[protocol].parse_stream(*usage_events(cut))
    if protocol == "messages":
        # message_start (input tokens) arrived intact before the cut.
        assert usage.input_tokens == 44
    # No final count, so no total: the relay keeps the estimate charged.
    assert usage.total_tokens is None


# --------------------------------------------------------------------------
# Anthropic: total_tokens is computed, cache tokens included
# --------------------------------------------------------------------------


def _sse(event: str, data: str) -> bytes:
    return f"event: {event}\ndata: {data}\n\n".encode()


def test_messages_unary_total_includes_cache_tokens() -> None:
    """Anthropic's input_tokens excludes cache reads and writes; the total
    adds them back so it means what OpenAI's total_tokens means."""
    body = (
        b'{"usage": {"input_tokens": 10, "output_tokens": 5,'
        b' "cache_read_input_tokens": 100, "cache_creation_input_tokens": 20}}'
    )
    usage = USAGE_PARSERS["messages"].parse_unary(body)
    assert usage == TokenUsage(
        input_tokens=10,
        output_tokens=5,
        cache_read_tokens=100,
        cache_write_tokens=20,
        total_tokens=135,
    )


def test_messages_unary_total_unknown_without_counts() -> None:
    usage = USAGE_PARSERS["messages"].parse_unary(b'{"usage": {"input_tokens": 10}}')
    assert usage.total_tokens is None


def test_messages_stream_total_includes_cache_tokens() -> None:
    raw = (
        _sse(
            "message_start",
            '{"type":"message_start","message":{"usage":{"input_tokens":10,'
            '"cache_read_input_tokens":100,"cache_creation_input_tokens":20,'
            '"output_tokens":1}}}',
        )
        + _sse("content_block_delta", '{"type":"content_block_delta"}')
        + _sse(
            "message_delta",
            '{"type":"message_delta","usage":{"output_tokens":5}}',
        )
        + _sse("message_stop", '{"type":"message_stop"}')
    )
    usage = USAGE_PARSERS["messages"].parse_stream(*usage_events(raw))
    assert usage == TokenUsage(
        input_tokens=10,
        output_tokens=5,
        cache_read_tokens=100,
        cache_write_tokens=20,
        total_tokens=135,
    )


def test_messages_stream_delta_counts_are_cumulative() -> None:
    """message_delta may restate every count (cumulative); its values win."""
    raw = _sse(
        "message_start",
        '{"type":"message_start","message":{"usage":{"input_tokens":10,'
        '"cache_read_input_tokens":0,"output_tokens":1}}}',
    ) + _sse(
        "message_delta",
        '{"type":"message_delta","usage":{"input_tokens":12,'
        '"cache_read_input_tokens":50,"cache_creation_input_tokens":3,'
        '"output_tokens":7}}',
    )
    usage = USAGE_PARSERS["messages"].parse_stream(*usage_events(raw))
    assert usage == TokenUsage(
        input_tokens=12,
        output_tokens=7,
        cache_read_tokens=50,
        cache_write_tokens=3,
        total_tokens=72,
    )


def test_messages_stream_without_final_delta_has_no_total() -> None:
    """message_start's output_tokens is provisional; a stream that never
    reached message_delta must not report a total."""
    raw = _sse(
        "message_start",
        '{"type":"message_start","message":{"usage":{"input_tokens":10,'
        '"output_tokens":1}}}',
    ) + _sse("content_block_delta", '{"type":"content_block_delta"}')
    usage = USAGE_PARSERS["messages"].parse_stream(*usage_events(raw))
    assert usage.input_tokens == 10
    assert usage.total_tokens is None
