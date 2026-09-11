"""
Inference API: the gateway's V2 data-plane routes.

LLM endpoints (all support streaming via SSE):

    chat/completions   OpenAI Chat Completions
    responses          OpenAI Responses
    messages           Anthropic Messages
    embeddings         OpenAI Embeddings (unary)

Non-LLM models are reached through the generic ``tasks`` route, where the
request body names the backend ``endpoint`` to invoke.

Federated routes let the gateway pick a backend for the requested ``model``;
deployment-scoped routes pin the request to one deployment.
"""

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Iterator, Literal, overload

from first_common.errors import FirstError, ServiceUnavailable, TooManyRequests
from first_common.schema.endpoints.models import ModelInfo, ModelListResponse

from ._http import SSEEvent, iter_sse, raise_for_status

if TYPE_CHECKING:
    from .client import InferenceClient

LLM_ENDPOINTS = ("chat/completions", "responses", "messages", "embeddings")

# Minimal requests used to trigger and observe a cold start.
_PROBE_PAYLOADS: dict[str, dict[str, Any]] = {
    "chat/completions": {
        "messages": [{"role": "user", "content": "Hi"}],
        "max_tokens": 1,
    },
    "messages": {"messages": [{"role": "user", "content": "Hi"}], "max_tokens": 1},
    "responses": {"input": "Hi", "max_output_tokens": 16},
    "embeddings": {"input": "Hi"},
}
_DEFAULT_POLL_SEC = 10
_MAX_POLL_SEC = 30


def slugify(name: str) -> str:
    """Resource name -> URL slug ('/' is replaced by '~', bijectively)."""
    return name.replace("/", "~")


@dataclass(slots=True)
class StartupStatus:
    """
    One observation of a model that cannot serve yet, taken from a cold-start
    503 (or a 429 hit while polling).  ``startup_eta_sec`` is the gateway's
    estimate at ``observed_at``; ``eta_remaining_sec`` counts it down locally.
    """

    message: str
    startup_eta_sec: int | None = None
    typical_startup_sec: int | None = None
    retry_after_sec: int | None = None
    cold_start: bool = True
    observed_at: float = field(default_factory=time.monotonic)

    @classmethod
    def from_error(cls, exc: FirstError) -> "StartupStatus | None":
        """The startup status carried by a cold-start 503, else None."""
        if not isinstance(exc, ServiceUnavailable) or "startup_eta_sec" not in exc.info:
            return None
        return cls(
            message=str(exc),
            startup_eta_sec=exc.info.get("startup_eta_sec"),
            typical_startup_sec=exc.info.get("typical_startup_sec"),
            retry_after_sec=exc.retry_after_sec,
        )

    @property
    def eta_remaining_sec(self) -> int | None:
        if self.startup_eta_sec is None:
            return None
        return max(
            0, round(self.startup_eta_sec - (time.monotonic() - self.observed_at))
        )


class InferenceAPI:
    """
    Thin, dialect-agnostic access to the inference routes.  Request bodies are
    passed through to the gateway as-is, so any parameter the upstream API
    accepts can be given as a keyword argument.
    """

    def __init__(self, client: "InferenceClient", deployment: str | None = None):
        self._client = client
        self.deployment = deployment
        self.prefix = (
            f"/deployments/{slugify(deployment)}/v1" if deployment else "/federated/v1"
        )

    def __repr__(self) -> str:
        return f"InferenceAPI({self.prefix})"

    # --- Low level --------------------------------------------------------------

    def post(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        """POST a unary request to ``{prefix}/{endpoint}`` and return its JSON."""
        resp = self._client.post(f"{self.prefix}/{endpoint}", json=payload)
        raise_for_status(resp)
        body: dict[str, Any] = resp.json()
        return body

    def stream(self, endpoint: str, payload: dict[str, Any]) -> Iterator[SSEEvent]:
        """POST a streaming request and yield its server-sent events."""
        payload = {**payload, "stream": True}
        with self._client.stream(
            "POST", f"{self.prefix}/{endpoint}", json=payload
        ) as resp:
            if resp.status_code >= 400 or resp.status_code == 202:
                resp.read()
                raise_for_status(resp)
            yield from iter_sse(resp)

    def _request(
        self, endpoint: str, payload: dict[str, Any], stream: bool
    ) -> dict[str, Any] | Iterator[SSEEvent]:
        return (
            self.stream(endpoint, payload) if stream else self.post(endpoint, payload)
        )

    def models(self) -> list[ModelInfo]:
        """OpenAI-style model listing (id, display_name, capabilities, max_tokens)."""
        resp = self._client.get(f"{self.prefix}/models")
        raise_for_status(resp)
        return ModelListResponse.model_validate(resp.json()).data

    # --- LLM endpoints ----------------------------------------------------------

    @overload
    def chat_completions(
        self,
        model: str,
        messages: list[Any],
        *,
        stream: Literal[False] = False,
        **params: Any,
    ) -> dict[str, Any]: ...

    @overload
    def chat_completions(
        self, model: str, messages: list[Any], *, stream: Literal[True], **params: Any
    ) -> Iterator[SSEEvent]: ...

    def chat_completions(
        self, model: str, messages: list[Any], *, stream: bool = False, **params: Any
    ) -> dict[str, Any] | Iterator[SSEEvent]:
        """OpenAI Chat Completions."""
        payload = {"model": model, "messages": messages, **params}
        return self._request("chat/completions", payload, stream)

    @overload
    def responses(
        self, model: str, input: Any, *, stream: Literal[False] = False, **params: Any
    ) -> dict[str, Any]: ...

    @overload
    def responses(
        self, model: str, input: Any, *, stream: Literal[True], **params: Any
    ) -> Iterator[SSEEvent]: ...

    def responses(
        self, model: str, input: Any, *, stream: bool = False, **params: Any
    ) -> dict[str, Any] | Iterator[SSEEvent]:
        """OpenAI Responses."""
        payload = {"model": model, "input": input, **params}
        return self._request("responses", payload, stream)

    @overload
    def messages(
        self,
        model: str,
        messages: list[Any],
        *,
        max_tokens: int = 1024,
        stream: Literal[False] = False,
        **params: Any,
    ) -> dict[str, Any]: ...

    @overload
    def messages(
        self,
        model: str,
        messages: list[Any],
        *,
        max_tokens: int = 1024,
        stream: Literal[True],
        **params: Any,
    ) -> Iterator[SSEEvent]: ...

    def messages(
        self,
        model: str,
        messages: list[Any],
        *,
        max_tokens: int = 1024,
        stream: bool = False,
        **params: Any,
    ) -> dict[str, Any] | Iterator[SSEEvent]:
        """Anthropic Messages (``max_tokens`` is mandatory in this dialect)."""
        payload = {
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens,
            **params,
        }
        return self._request("messages", payload, stream)

    def embeddings(self, model: str, input: Any, **params: Any) -> dict[str, Any]:
        """OpenAI Embeddings."""
        return self.post("embeddings", {"model": model, "input": input, **params})

    # --- Generic (non-LLM) tasks ------------------------------------------------

    def task(self, model: str, endpoint: str, **params: Any) -> dict[str, Any]:
        """
        Invoke ``/v1/{endpoint}`` on a backend serving ``model``, passing
        ``params`` through verbatim, and return the backend's JSON result.
        """
        return self.post("tasks", {"model": model, "endpoint": endpoint, **params})

    # --- Cold start -------------------------------------------------------------

    def probe_request(
        self, model: str, endpoint: str | None = None
    ) -> tuple[str, dict[str, Any]]:
        """
        A minimal request that exercises ``model``: the endpoint is chosen from
        the Model's supported endpoints unless given.  Non-LLM models have no
        generic probe; call ``warmup`` with an explicit ``payload``.
        """
        supported = self._client.catalog.get_model(model).supported_endpoints
        if endpoint is None:
            endpoint = next((e for e in LLM_ENDPOINTS if e in supported), None)
        if endpoint is None or endpoint not in _PROBE_PAYLOADS:
            raise ValueError(
                f"No default probe request for {model!r} (endpoints: {supported}); "
                "pass an explicit endpoint and payload."
            )
        return endpoint, {"model": model, **_PROBE_PAYLOADS[endpoint]}

    def warmup(
        self,
        model: str,
        *,
        endpoint: str | None = None,
        payload: dict[str, Any] | None = None,
        timeout: float = 1800,
        on_status: Callable[[StartupStatus], None] | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> float:
        """
        Request a cold start for ``model`` and wait until a backend serves it.

        A minimal request is sent repeatedly: the first attempt registers demand
        with the autoscaler, and each cold-start 503 reports the startup ETA via
        ``on_status``.  Returns the seconds waited once a request is admitted.
        A 503 without startup hints (saturated backend) also counts as ready.
        ``sleep`` is called between polls (injectable for live countdowns).
        """
        if payload is None:
            endpoint, payload = self.probe_request(model, endpoint)
        assert endpoint is not None

        start = time.monotonic()
        while True:
            try:
                self.post(endpoint, payload)
                return time.monotonic() - start
            except ServiceUnavailable as exc:
                status = StartupStatus.from_error(exc)
                if status is None:
                    return time.monotonic() - start
            except TooManyRequests as exc:
                status = StartupStatus(
                    message=str(exc),
                    retry_after_sec=exc.retry_after_sec,
                    cold_start=False,
                )

            if on_status is not None:
                on_status(status)

            remaining = timeout - (time.monotonic() - start)
            if remaining <= 0:
                raise TimeoutError(f"{model!r} was not ready after {timeout:.0f}s")
            delay = min(status.retry_after_sec or _DEFAULT_POLL_SEC, _MAX_POLL_SEC)
            sleep(max(1, min(delay, remaining)))


# --- Dialect-aware text extraction (for rendering) --------------------------------

DeltaKind = Literal["text", "reasoning"]


def response_text(endpoint: str, body: dict[str, Any]) -> str:
    """The assistant text of a unary response, across all three LLM dialects."""
    if endpoint == "chat/completions":
        choices = body.get("choices") or []
        message = choices[0].get("message", {}) if choices else {}
        return str(message.get("content") or "")
    if endpoint == "responses":
        return "".join(
            str(part.get("text", ""))
            for item in body.get("output", [])
            if item.get("type") == "message"
            for part in item.get("content", [])
            if part.get("type") == "output_text"
        )
    if endpoint == "messages":
        return "".join(
            str(block.get("text", ""))
            for block in body.get("content", [])
            if block.get("type") == "text"
        )
    raise ValueError(f"Unsupported endpoint {endpoint!r}")


def response_reasoning(endpoint: str, body: dict[str, Any]) -> str:
    """Any reasoning/thinking text of a unary response (empty when absent)."""
    if endpoint == "chat/completions":
        choices = body.get("choices") or []
        message = choices[0].get("message", {}) if choices else {}
        return str(message.get("reasoning_content") or message.get("reasoning") or "")
    if endpoint == "responses":
        return "".join(
            str(part.get("text", ""))
            for item in body.get("output", [])
            if item.get("type") == "reasoning"
            for part in item.get("summary", []) + item.get("content", [])
        )
    if endpoint == "messages":
        return "".join(
            str(block.get("thinking", ""))
            for block in body.get("content", [])
            if block.get("type") == "thinking"
        )
    return ""


def stream_delta(endpoint: str, event: SSEEvent) -> tuple[DeltaKind, str] | None:
    """
    The incremental text carried by one streaming event, tagged as assistant
    ``text`` or ``reasoning``; None for bookkeeping events.
    """
    data = event.json()
    if endpoint == "chat/completions":
        choices = data.get("choices") or []
        delta = choices[0].get("delta", {}) if choices else {}
        if text := delta.get("content"):
            return "text", str(text)
        if reasoning := delta.get("reasoning_content") or delta.get("reasoning"):
            return "reasoning", str(reasoning)
        return None
    if endpoint == "responses":
        kind = data.get("type")
        if kind == "response.output_text.delta":
            return "text", str(data.get("delta", ""))
        if kind in (
            "response.reasoning_text.delta",
            "response.reasoning_summary_text.delta",
        ):
            return "reasoning", str(data.get("delta", ""))
        return None
    if endpoint == "messages":
        if data.get("type") != "content_block_delta":
            return None
        delta = data.get("delta", {})
        if delta.get("type") == "text_delta":
            return "text", str(delta.get("text", ""))
        if delta.get("type") == "thinking_delta":
            return "reasoning", str(delta.get("thinking", ""))
        return None
    raise ValueError(f"Unsupported endpoint {endpoint!r}")


def response_usage(endpoint: str, body: dict[str, Any]) -> dict[str, Any] | None:
    """Token usage of a unary response, or of a terminal streaming event."""
    if endpoint == "responses" and "response" in body:
        body = body["response"]
    usage = body.get("usage")
    return usage if isinstance(usage, dict) else None
