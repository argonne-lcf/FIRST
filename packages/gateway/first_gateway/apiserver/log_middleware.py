"""Pure-ASGI middleware that emits the Response event and owns the correlation id.

It generates one UUID per request into ``_request_id_ctx`` (propagating down to
deps, the route, ``InferenceService``, and the logging ``RequestIdFilter``),
observes the real ``http.response.start``/``http.response.body`` messages, and
emits a :class:`ResponseLog` after the response is sent.

It buffers a response body ONLY for error responses (``status_code >= 400``,
which are small); success bodies — unary and streaming alike — are never
buffered here (their content is persisted by ``InferenceService``).
"""

import time
import uuid

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from first_common.schema.structured_logs import ResponseLog

from .context import _request_id_ctx

# Constant probe traffic; the only anonymous route worth suppressing.
_SKIP_PATHS = {"/health"}


class ResponseLogMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        cid = str(uuid.uuid4())
        token = _request_id_ctx.set(cid)
        t0 = time.perf_counter()

        status_code = 500
        streaming = False
        error_body = bytearray()

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code, streaming
            if message["type"] == "http.response.start":
                status_code = message["status"]
                headers = message.get("headers") or []
                for name, value in headers:
                    if name.lower() == b"content-type":
                        streaming = value.lower().startswith(b"text/event-stream")
                        break
            elif message["type"] == "http.response.body":
                # Only small error bodies are captured here; success/streaming
                # content is persisted by InferenceService.
                if status_code >= 400 and not streaming:
                    error_body.extend(message.get("body", b""))
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            # An uncaught exception propagates un-statused (ServerErrorMiddleware,
            # which emits the 500, is above us). Log our own ResponseLog(500) so
            # the request_id is preserved, then re-raise.
            self._emit(scope, cid, 500, t0, streaming=False, raw_body=None)
            raise
        else:
            if scope["path"] not in _SKIP_PATHS:
                raw = bytes(error_body) if status_code >= 400 else None
                self._emit(scope, cid, status_code, t0, streaming, raw)
        finally:
            _request_id_ctx.reset(token)

    @staticmethod
    def _emit(
        scope: Scope,
        cid: str,
        status_code: int,
        t0: float,
        streaming: bool,
        raw_body: bytes | None,
    ) -> None:
        storage_dir = scope["app"].state.client_state.settings.prompt_storage_dir
        ResponseLog(
            request_id=cid,
            status_code=status_code,
            duration_ms=(time.perf_counter() - t0) * 1000,
            streaming=streaming,
        ).emit(raw_body=raw_body, storage_dir=storage_dir)
