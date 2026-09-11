"""
HTTP plumbing shared by the SDK: typed error mapping and SSE framing.
"""

import json
from dataclasses import dataclass
from typing import Any, Iterator

from httpx import Response

from first_common.errors import FirstError


def _error_registry() -> dict[str, type[FirstError]]:
    """
    Map every error ``code`` declared in first_common.errors back to its class,
    so the SDK raises the same typed exception the gateway raised.

    Only classes that declare their own ``code`` are registered: subclasses
    that merely inherit ``internal_error`` from FirstError must not shadow it.
    """
    registry: dict[str, type[FirstError]] = {}
    stack: list[type[FirstError]] = [FirstError]
    while stack:
        cls = stack.pop()
        for sub in cls.__subclasses__():
            if "code" in sub.__dict__:
                registry.setdefault(sub.code, sub)
            stack.append(sub)
    return registry


_ERRORS = _error_registry()


def raise_for_status(response: Response) -> None:
    """
    Raise a typed FirstError carrying the server's error message, regardless
    of which error shape the server returned.

    Recognized shapes:
      - {"error": {"code", "message", "info"}}  (FirstError exception handler)
      - {"detail": [ {"loc","msg",...}, ... ]}  (FastAPI RequestValidationError)
      - {"detail": "..."}                        (FastAPI HTTPException)
      - anything else: falls back to raw body text
    """
    if response.status_code < 400:
        return

    message, code, info = _extract(_json_or_none(response), response)
    error_cls = _ERRORS.get(code, FirstError)
    raise error_cls(
        message,
        status_code=response.status_code,
        info=info,
        retry_after_sec=_retry_after(response),
    )


def _json_or_none(response: Response) -> Any:
    try:
        return response.json()
    except Exception:
        return None


def _retry_after(response: Response) -> int | None:
    value = response.headers.get("Retry-After")
    try:
        return int(value) if value else None
    except ValueError:
        return None


def _extract(payload: Any, response: Response) -> tuple[str, str, dict[str, Any]]:
    if isinstance(payload, dict):
        err = payload.get("error")
        if isinstance(err, dict):
            message = err.get("message") or err.get("code") or response.reason_phrase
            raw_info = err.get("info")
            info: dict[str, Any] = raw_info if isinstance(raw_info, dict) else {}
            return str(message), str(err.get("code") or ""), info

        detail = payload.get("detail")
        if isinstance(detail, list):
            return _format_validation_detail(detail), "validation_error", {}
        if isinstance(detail, str):
            return detail, "", {}

    text = (response.text or "").strip()
    return text or response.reason_phrase, "", {}


def _format_validation_detail(items: list[Any]) -> str:
    lines = ["Request validation failed:"]
    for item in items:
        if isinstance(item, dict):
            loc = ".".join(str(p) for p in item.get("loc", []) if p != "body")
            msg = item.get("msg", "")
            lines.append(f" - {loc}: {msg}" if loc else f" - {msg}")
        else:
            lines.append(f" - {item}")
    return "\n".join(lines)


# --- Server-Sent Events -------------------------------------------------------


@dataclass(slots=True)
class SSEEvent:
    """One server-sent event: the optional ``event:`` name and joined ``data:``."""

    data: str
    event: str | None = None

    def json(self) -> Any:
        return json.loads(self.data)


def iter_sse(response: Response) -> Iterator[SSEEvent]:
    """
    Frame a streaming response into SSE events.  The OpenAI ``data: [DONE]``
    sentinel terminates the stream and is never yielded.
    """
    event: str | None = None
    data: list[str] = []
    for line in response.iter_lines():
        line = line.rstrip("\r\n")
        if not line:
            if data:
                payload = "\n".join(data)
                if payload.strip() == "[DONE]":
                    return
                yield SSEEvent(data=payload, event=event)
            event, data = None, []
        elif line.startswith(":"):
            continue
        elif line.startswith("event:"):
            event = line[6:].strip()
        elif line.startswith("data:"):
            data.append(line[5:].lstrip())
    if data and "\n".join(data).strip() != "[DONE]":
        yield SSEEvent(data="\n".join(data), event=event)
