"""Token-usage extraction for the three upstream LLM dialects the gateway
proxies:

    OpenAI Chat Completions   /v1/chat/completions   streaming + unary
    OpenAI Responses API      /v1/responses          streaming + unary
    Anthropic Messages        /v1/messages           streaming + unary

Each dialect reports usage differently; this module normalizes all six cases
into a single :class:`TokenUsage`.

Where usage lives, per dialect:

  Chat Completions  unary:  top-level ``usage`` {prompt_tokens,
                    completion_tokens, prompt_tokens_details.cached_tokens,
                    completion_tokens_details.reasoning_tokens}.
                    stream: a final usage-bearing SSE chunk (choices == [])
                    before ``data: [DONE]`` -- but ONLY if the request set
                    stream_options.include_usage. Intermediate chunks carry
                    ``"usage": null``. ``prepare_request`` injects the option.

  Responses API     unary:  top-level ``usage`` {input_tokens, output_tokens,
                    input_tokens_details.cached_tokens,
                    output_tokens_details.reasoning_tokens}.
                    stream: a terminal event (response.completed / .incomplete
                    / .failed) whose data embeds the full response object,
                    usage included. Earlier events carry ``"usage": null``.

  Anthropic         unary:  top-level ``usage`` {input_tokens, output_tokens,
                    cache_read_input_tokens, cache_creation_input_tokens}.
                    stream: input_tokens (+ cache reads/writes) arrive in the
                    FIRST event (message_start); the cumulative output_tokens
                    arrive in the LAST usage-bearing event (message_delta).
                    NOTE: Anthropic's input_tokens EXCLUDES cached tokens,
                    and there is no total: :func:`_anthropic_total` adds it up.

Streaming extraction does no per-chunk work: the relay already keeps the whole
stream for the InferenceLog, so :func:`usage_events` runs once when the stream
ends. It cuts out the first and last SSE events whose raw bytes contain
``b"usage"`` with a few C-level ``find``/``rfind`` calls. That one test covers
all three dialects: message_start, message_delta, the chat usage chunk and the
Responses terminal event all contain it, and bulk token-delta events contain at
most ``"usage": null``. The adapter then JSON-parses just those two events.

Model output can contain the word "usage" too, but never outside the real usage
events: the first real one (Anthropic's message_start) comes before any content
and the last comes after it.
"""

import json
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class TokenUsage:
    """Normalized token counts, unified across all three dialects.

    - ``input_tokens`` excludes cache reads for Anthropic (reported separately in
      ``cache_read_tokens``).
    - ``reasoning_tokens`` is a subset of ``output_tokens``.
    - ``total_tokens`` counts every input token, cached or not, plus output, as
      OpenAI's does. Anthropic reports no total, so it is computed there.
    - Any field may be ``None`` when the upstream dialect does not report it.
    """

    input_tokens: int | None = None
    output_tokens: int | None = None
    cache_read_tokens: int | None = None
    cache_write_tokens: int | None = None
    reasoning_tokens: int | None = None
    total_tokens: int | None = None


# --------------------------------------------------------------------------
# SSE framing
# --------------------------------------------------------------------------


def usage_events(
    buf: bytes | bytearray, *, first: bool = True
) -> tuple[bytes | None, bytes | None]:
    """Return the first and last SSE events in a whole stream that contain
    ``b"usage"``, or ``(None, None)`` if none does.

    The search starts from the end: the last usage event is at or near the end
    of every dialect, so ``rfind`` reaches it at once. Searching forward for
    the first one would scan the whole stream when only the final event
    carries usage, as in a vLLM Chat Completions stream: about 0.4 ms per MB
    with the event loop blocked. Pass ``first=False`` (an adapter whose
    ``needs_first`` is False) to skip that search and get ``(None, last)``.

    When both are the same event, the same object is returned twice, so
    ``last is first`` holds. An unterminated final event, as in a stream cut
    off mid-event, runs to the end of ``buf``.

    The delimiter (``\\n\\n`` or ``\\r\\n\\r\\n``) is taken from the first line
    ending. Backends do not mix line endings within a stream.
    """
    last = buf.rfind(b"usage")
    if last == -1:
        return None, None

    nl = buf.find(b"\n")
    delim = b"\r\n\r\n" if nl > 0 and buf[nl - 1] == 0x0D else b"\n\n"

    def span(i: int) -> tuple[int, int]:
        s = buf.rfind(delim, 0, i)
        e = buf.find(delim, i)
        return (0 if s == -1 else s + len(delim)), (len(buf) if e == -1 else e)

    ls, le = span(last)
    tail = bytes(buf[ls:le])
    if not first:
        return None, tail
    head = buf.find(b"usage", 0, ls)
    if head == -1:  # only one usage-bearing event
        return tail, tail
    fs, fe = span(head)
    return bytes(buf[fs:fe]), tail


def _data_payload(evt: bytes) -> bytes | None:
    """Concatenate the ``data:`` lines of one SSE event (spec: join with \\n)."""
    out = []
    for line in evt.split(b"\n"):
        line = line.rstrip(b"\r")
        if line.startswith(b"data:"):
            v = line[5:]
            if v[:1] == b" ":
                v = v[1:]
            out.append(v)
    return b"\n".join(out) if out else None


def _event_json(evt: bytes | None) -> dict[str, Any] | None:
    """Parse one retained SSE event to a dict; tolerant of [DONE]/garbage."""
    if evt is None:
        return None
    data = _data_payload(evt)
    if data is None or data.strip() == b"[DONE]":
        return None
    try:
        obj = json.loads(data)
    except (ValueError, UnicodeDecodeError):
        return None
    return obj if isinstance(obj, dict) else None


# --------------------------------------------------------------------------
# Adapters
# --------------------------------------------------------------------------


class BaseAdapter:
    # Whether parse_stream reads its ``first`` argument; see usage_events.
    needs_first: bool = False

    def prepare_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Mutate/augment the upstream request so usage will be reported."""
        return payload

    def parse_stream(self, first: bytes | None, last: bytes | None) -> TokenUsage:
        raise NotImplementedError

    def parse_unary(self, body: str | bytes | dict[str, Any]) -> TokenUsage:
        raise NotImplementedError


def _from_chat(u: Any) -> TokenUsage:
    if not isinstance(u, dict):
        return TokenUsage()

    ptd = u.get("prompt_tokens_details") or {}
    ctd = u.get("completion_tokens_details") or {}

    return TokenUsage(
        input_tokens=u.get("prompt_tokens"),
        output_tokens=u.get("completion_tokens"),
        cache_read_tokens=ptd.get("cached_tokens") if isinstance(ptd, dict) else None,
        reasoning_tokens=ctd.get("reasoning_tokens") if isinstance(ctd, dict) else None,
        total_tokens=u.get("total_tokens"),
    )


class ChatCompletionsAdapter(BaseAdapter):
    def prepare_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        # Without include_usage a Chat Completions stream carries no usage at
        # all (OpenAI and vLLM alike). The injected final chunk has an empty
        # choices list.
        if payload.get("stream"):
            so = payload.setdefault("stream_options", {})
            if isinstance(so, dict):
                so["include_usage"] = True
        return payload

    def parse_stream(self, _first: bytes | None, last: bytes | None) -> TokenUsage:
        # Intermediate chunks carry "usage": null; use `last`.
        obj = _event_json(last)
        return _from_chat(obj.get("usage")) if obj else TokenUsage()

    def parse_unary(self, body: str | bytes | dict[str, Any]) -> TokenUsage:
        try:
            obj = json.loads(body) if isinstance(body, (bytes, str)) else body
        except (ValueError, UnicodeDecodeError):
            return TokenUsage()
        return _from_chat(obj.get("usage")) if isinstance(obj, dict) else TokenUsage()


def _from_responses(u: Any) -> TokenUsage:
    if not isinstance(u, dict):
        return TokenUsage()

    itd = u.get("input_tokens_details") or {}
    otd = u.get("output_tokens_details") or {}

    return TokenUsage(
        input_tokens=u.get("input_tokens"),
        output_tokens=u.get("output_tokens"),
        cache_read_tokens=itd.get("cached_tokens") if isinstance(itd, dict) else None,
        reasoning_tokens=otd.get("reasoning_tokens") if isinstance(otd, dict) else None,
        total_tokens=u.get("total_tokens"),
    )


class ResponsesAdapter(BaseAdapter):
    def parse_stream(self, _first: bytes | None, last: bytes | None) -> TokenUsage:
        # Terminal events (response.completed / .incomplete / .failed) embed
        # the full response object; usage lives inside it. Early events
        # (response.created) carry "usage": null and parse to None here.
        obj = _event_json(last)
        if not obj:
            return TokenUsage()

        resp = obj.get("response")
        return (
            _from_responses(resp.get("usage"))
            if isinstance(resp, dict)
            else TokenUsage()
        )

    def parse_unary(self, body: str | bytes | dict[str, Any]) -> TokenUsage:
        try:
            obj = json.loads(body) if isinstance(body, (bytes, str)) else body
        except (ValueError, UnicodeDecodeError):
            return TokenUsage()
        return (
            _from_responses(obj.get("usage")) if isinstance(obj, dict) else TokenUsage()
        )


def _anthropic_total(u: TokenUsage) -> int | None:
    """OpenAI-equivalent total: Anthropic's ``input_tokens`` leaves out cache
    reads and writes, so add them back. None unless input and output are both
    known."""
    if u.input_tokens is None or u.output_tokens is None:
        return None
    return (
        u.input_tokens
        + (u.cache_read_tokens or 0)
        + (u.cache_write_tokens or 0)
        + u.output_tokens
    )


# message_delta's usage keys -> TokenUsage fields. The counts are cumulative.
_ANTHROPIC_DELTA_FIELDS = (
    ("output_tokens", "output_tokens"),
    ("input_tokens", "input_tokens"),
    ("cache_read_input_tokens", "cache_read_tokens"),
    ("cache_creation_input_tokens", "cache_write_tokens"),
)


class AnthropicMessagesAdapter(BaseAdapter):
    needs_first = True  # input tokens arrive in message_start

    def parse_stream(self, first: bytes | None, last: bytes | None) -> TokenUsage:
        u = TokenUsage()

        fobj = _event_json(first)
        if isinstance(fobj, dict) and fobj.get("type") == "message_start":
            mu = (fobj.get("message") or {}).get("usage")
            if isinstance(mu, dict):
                u.input_tokens = mu.get("input_tokens")
                u.cache_read_tokens = mu.get("cache_read_input_tokens")
                u.cache_write_tokens = mu.get("cache_creation_input_tokens")
                u.output_tokens = mu.get("output_tokens")  # provisional (~0)

        if last is not None and last is not first:
            lobj = _event_json(last)
            if isinstance(lobj, dict) and lobj.get("type") == "message_delta":
                du = lobj.get("usage")
                if isinstance(du, dict) and du.get("output_tokens") is not None:
                    # cumulative counts; last delta wins
                    for key, field in _ANTHROPIC_DELTA_FIELDS:
                        if (v := du.get(key)) is not None:
                            setattr(u, field, v)
                    # Only the final output count makes a total. A stream cut
                    # off before message_delta keeps None, so the relay keeps
                    # the estimate charged rather than charging input alone.
                    u.total_tokens = _anthropic_total(u)
        return u

    def parse_unary(self, body: str | bytes | dict[str, Any]) -> TokenUsage:
        try:
            obj = json.loads(body) if isinstance(body, (bytes, str)) else body
        except (ValueError, UnicodeDecodeError):
            return TokenUsage()

        if not isinstance(obj, dict):
            return TokenUsage()

        mu = obj.get("usage")
        if not isinstance(mu, dict):
            return TokenUsage()

        u = TokenUsage(
            input_tokens=mu.get("input_tokens"),
            output_tokens=mu.get("output_tokens"),
            cache_read_tokens=mu.get("cache_read_input_tokens"),
            cache_write_tokens=mu.get("cache_creation_input_tokens"),
        )
        u.total_tokens = _anthropic_total(u)
        return u


class EmbeddingsAdapter(BaseAdapter):
    def parse_unary(self, body: str | bytes | dict[str, Any]) -> TokenUsage:
        try:
            obj = json.loads(body) if isinstance(body, (bytes, str)) else body
        except (ValueError, UnicodeDecodeError):
            return TokenUsage()
        return _from_chat(obj.get("usage")) if isinstance(obj, dict) else TokenUsage()


USAGE_PARSERS: dict[str, BaseAdapter] = {
    "chat/completions": ChatCompletionsAdapter(),
    "responses": ResponsesAdapter(),
    "messages": AnthropicMessagesAdapter(),
    "embeddings": EmbeddingsAdapter(),
}
