from typing import Any

from httpx import Response

from first_common.errors import FirstError


def raise_for_status(response: Response) -> None:
    """
    Raise a FirstError carrying the server's error message, regardless of which
    error shape the server returned.

    Recognized shapes:
      - {"error": {"code", "message", "info"}}  (FirstError exception handler)
      - {"detail": [ {"loc","msg",...}, ... ]}  (FastAPI RequestValidationError)
      - {"detail": "..."}                        (FastAPI HTTPException)
      - anything else: falls back to raw body text
    """
    if response.status_code < 400:
        return

    try:
        payload = response.json()
    except Exception:
        payload = None

    message, info = _extract(payload, response)
    raise FirstError(message, status_code=response.status_code, info=info)


def _extract(payload: Any, response: Response) -> tuple[str, dict[str, Any]]:
    if isinstance(payload, dict):
        err = payload.get("error")
        if isinstance(err, dict):
            message = err.get("message") or err.get("code") or response.reason_phrase
            raw_info = err.get("info")
            info: dict[str, Any] = raw_info if isinstance(raw_info, dict) else {}
            return str(message), info

        detail = payload.get("detail")
        if isinstance(detail, list):
            return _format_validation_detail(detail), {}
        if isinstance(detail, str):
            return detail, {}

    text = (response.text or "").strip()
    return text or response.reason_phrase, {}


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
