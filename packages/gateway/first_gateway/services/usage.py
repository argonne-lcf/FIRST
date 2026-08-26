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
                    NOTE: Anthropic's input_tokens EXCLUDES cached tokens.

Streaming extraction avoids parsing every chunk: :class:`UsageTap` frames SSE
events and retains only the first and most-recent event whose raw bytes contain
``b'"usage"'`` -- a cheap containment test that covers all three dialects
(message_start / message_delta / the chat usage chunk / the Responses terminal
event all contain it; bulk token-delta events do not). The retained events are
JSON-parsed once, at the end, by the adapter.
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


def _next_event(buf: bytearray) -> bytes | None:
    """Pop one complete SSE event off the front of ``buf``, handling both
    ``\\n\\n`` and ``\\r\\n\\r\\n`` delimiters. Returns None if incomplete."""
    i = buf.find(b"\n\n")
    j = buf.find(b"\r\n\r\n")
    if i == -1 and j == -1:
        return None
    if j != -1 and (i == -1 or j < i):
        evt = bytes(buf[:j])
        del buf[: j + 4]
    else:
        evt = bytes(buf[:i])
        del buf[: i + 2]
    return evt


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


class UsageTap:
    """Frames a relayed SSE stream and retains the first and most-recent
    usage-bearing events.

    Feed each relayed chunk to :meth:`feed`; call :meth:`close` once the stream
    ends to flush a trailing partial event. Then hand ``first`` / ``last`` to an
    adapter's ``parse_stream``.
    """

    __slots__ = ("buf", "first", "last")

    def __init__(self) -> None:
        self.buf = bytearray()
        self.first: bytes | None = None
        self.last: bytes | None = None

    def feed(self, chunk: bytes) -> None:
        self.buf += chunk
        while (evt := _next_event(self.buf)) is not None:
            self._observe(evt)

    def close(self) -> None:
        """Flush a final event not terminated by a blank line."""
        if self.buf:
            self._observe(bytes(self.buf))
            del self.buf[:]

    def _observe(self, evt: bytes) -> None:
        if b"usage" in evt:
            if self.first is None:
                self.first = evt
            self.last = evt


# --------------------------------------------------------------------------
# Adapters
# --------------------------------------------------------------------------


class BaseAdapter:
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


class AnthropicMessagesAdapter(BaseAdapter):
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
                if isinstance(du, dict):
                    # cumulative counts; last delta wins
                    if (v := du.get("output_tokens")) is not None:
                        u.output_tokens = v
                    if (v := du.get("input_tokens")) is not None:
                        u.input_tokens = v
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
        return u


USAGE_PARSERS: dict[str, BaseAdapter] = {
    "chat/completions": ChatCompletionsAdapter(),
    "responses": ResponsesAdapter(),
    "messages": AnthropicMessagesAdapter(),
}
