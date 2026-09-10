import logging
import time
import uuid
from typing import Annotated, Any, AsyncIterator, Awaitable, Callable, NoReturn, cast

import anyio
import httpx
from fastapi import Depends, Request
from fastapi.responses import JSONResponse, StreamingResponse
from httpx import AsyncClient

from first_common.errors import (
    FirstError,
    InvalidSpecError,
    NotFound,
    ServiceUnavailable,
)
from first_common.schema.endpoints.base import BasePayload
from first_common.schema.structured_logs import InferenceLog, InferenceOutcome

from ..database.redis.admission import AdmissionController
from ..database.redis.router_config import (
    BackendConfig,
    DeploymentConfig,
    ModelConfig,
)
from ..services.orchestration import (
    admit_request,
    get_deployment_from_backend_id,
    get_shuffled_backends,
)
from ..services.usage import USAGE_PARSERS, TokenUsage, UsageTap
from .auth import enforce_permission
from .backend_client_manager import BackendClientManager
from .context import get_request_id
from .dependencies import AuthUser
from .router_config_manager import RouterConfigManager

logger = logging.getLogger(__name__)

MAX_ATTEMPTS = 3
_RETRYABLE_STATUS = (502, 503, 504)

UpstreamHandler = Callable[
    [AsyncClient, BasePayload, ModelConfig, DeploymentConfig, BackendConfig],
    Awaitable[StreamingResponse | JSONResponse],
]


class _UpstreamFailure(Exception):
    """
    A single upstream attempt failed.

    `summary` is a short, user-safe description (e.g. "HTTP 503") suitable for
    aggregating into the final error message; `retryable` decides whether
    another backend may be tried.
    """

    def __init__(self, summary: str, *, retryable: bool) -> None:
        super().__init__(summary)
        self.summary = summary
        self.retryable = retryable


class InferenceService:
    """
    Selects a backend, submits the request, and relays the response.
    """

    def __init__(self, request: Request, user: AuthUser) -> None:
        self.user = user
        self.request_id = get_request_id() or str(uuid.uuid4())
        self.attempt = 1

        state = request.app.state
        self.storage_dir = state.client_state.settings.prompt_storage_dir
        self.admission_controller = cast(
            AdmissionController, state.admission_controller
        )
        self.backend_client_manager = cast(
            BackendClientManager, state.backend_client_manager
        )
        self.router_config = cast(
            RouterConfigManager, state.router_config_manager
        ).current

    def _resolve_model(self, payload: BasePayload) -> ModelConfig:
        """Look up, authorize, and endpoint-check the payload's model."""
        model = self.router_config.models_by_name.get(payload.model)
        if model is None:
            model = self.router_config.models_by_alias.get(payload.model)
            if model is None:
                raise NotFound(f"Model {payload.model} does not exist.")

        enforce_permission(self.user, model)

        if payload.endpoint not in model.supported_endpoints:
            raise InvalidSpecError(
                f"Endpoint {payload.endpoint} not supported for {model.name}."
            )
        return model

    async def submit_inference(
        self, payload: BasePayload, deployment_name: str | None = None
    ) -> StreamingResponse | JSONResponse:
        """
        Send the request to a backend, falling over to other backends on
        retryable failures.
        """

        model = self._resolve_model(payload)

        # Only collect backend candidates that have a httpx client ready
        backend_candidates = get_shuffled_backends(
            model, deployment_name=deployment_name
        )
        backend_candidates = [
            b
            for b in backend_candidates
            if b.uid in self.backend_client_manager.clients
        ]

        stats = await self.admission_controller.get_token_stats(
            model.name, self.user.id
        )
        estimated_tokens = payload.estimate_tokens(
            model.max_model_len,
            chars_per_token=stats.chars_per_token,
            output_estimate=(
                round(stats.output_tokens) if stats.output_tokens is not None else None
            ),
        )
        handler = self._get_upstream_handler(payload)

        attempted = 0
        failures: list[str] = []

        # Attempt at least once to make sure admit_request is called.
        for _ in range(max(1, min(MAX_ATTEMPTS, len(backend_candidates)))):
            backend_id = await admit_request(
                self.user,
                model,
                self.admission_controller,
                backend_candidates,
                self.request_id,
                estimated_tokens=estimated_tokens,
                deployment_name=deployment_name,
            )

            client = self.backend_client_manager.get(backend_id)
            assert client is not None, "Should be filtered by existing clients"

            deployment = get_deployment_from_backend_id(model.deployments, backend_id)
            backend = next(b for b in deployment.backends if b.id == backend_id)
            payload.model = backend.backend_model_name

            attempted += 1
            self.attempt = attempted
            t0 = time.perf_counter()
            try:
                return await handler(client, payload, model, deployment, backend)
            except _UpstreamFailure as exc:
                await self._release_failed_backend(backend_id, deployment)
                self._emit_inference_log(
                    payload,
                    model,
                    deployment,
                    backend,
                    outcome="upstream_error",
                    latency_sec=time.perf_counter() - t0,
                    error=exc.summary,
                )
                if not exc.retryable:
                    raise ServiceUnavailable(
                        "Upstream model server returned an unexpected error."
                    )
                failures.append(exc.summary)
                backend_candidates = [
                    b for b in backend_candidates if b.uid != backend_id
                ]
                logger.warning(
                    f"Backend {backend_id} failed ({exc.summary}); trying next."
                )
            except FirstError as exc:
                # Upstream 4xx: settle & propagate response without penalising the backend
                await self.admission_controller.settle(self.request_id, actual_tokens=0)
                self._emit_inference_log(
                    payload,
                    model,
                    deployment,
                    backend,
                    outcome="upstream_rejected",
                    latency_sec=time.perf_counter() - t0,
                    error=str(exc),
                )
                raise
            except Exception as exc:
                await self._release_failed_backend(backend_id, deployment)
                self._emit_inference_log(
                    payload,
                    model,
                    deployment,
                    backend,
                    outcome="upstream_error",
                    latency_sec=time.perf_counter() - t0,
                    error=type(exc).__name__,
                )
                logger.error(
                    f"Unexpected error from backend {backend_id}.", exc_info=True
                )
                raise ServiceUnavailable(
                    "Upstream model server returned an unexpected error."
                )

        # Error if none of the retryable attempts worked.
        raise ServiceUnavailable(
            f"All {attempted} backend(s) failed for model {model.name}. "
            f"Encountered: {', '.join(failures)}."
        )

    async def _release_failed_backend(
        self, backend_id: str, deployment: DeploymentConfig
    ) -> None:
        """Record a backend fault and release the request's reservation."""
        await self.admission_controller.record_error(
            backend_id, deployment.router_params
        )
        await self.admission_controller.settle(self.request_id, actual_tokens=0)

    async def _record_token_stats(
        self, payload: BasePayload, model: ModelConfig, usage: TokenUsage
    ) -> None:
        """Feed one successful request's real usage into the estimation EWMAs.

        chars-per-token is only learnable from requests with no images and no
        cache activity: images cost tokens with no characters, and caching
        decouples the reported input tokens from the prompt's character count
        (Anthropic omits cache reads from ``input_tokens``; OpenAI folds them
        in).  Output tokens are learned from any completion that reports them.
        """
        input_chars, images = payload.input_basis()
        cached = bool(usage.cache_read_tokens or usage.cache_write_tokens)
        chars_per_token_sample = (
            input_chars / usage.input_tokens
            if input_chars > 0 and images == 0 and not cached and usage.input_tokens
            else None
        )
        output_tokens_sample = (
            float(usage.output_tokens) if usage.output_tokens else None
        )
        await self.admission_controller.record_token_stats(
            model.name,
            self.user.id,
            chars_per_token_sample=chars_per_token_sample,
            output_tokens_sample=output_tokens_sample,
        )

    def _get_upstream_handler(self, payload: BasePayload) -> UpstreamHandler:
        streaming = getattr(payload, "stream", False) or False
        return self._handle_streaming if streaming else self._handle_unary

    def _emit_inference_log(
        self,
        payload: BasePayload,
        model: ModelConfig,
        deployment: DeploymentConfig,
        backend: BackendConfig,
        *,
        latency_sec: float,
        outcome: InferenceOutcome = "success",
        usage: TokenUsage | None = None,
        raw_body: bytes | None = None,
        error: str | None = None,
    ) -> None:
        """Emit one per-attempt InferenceLog. Success attempts pass ``usage`` and
        ``raw_body``; failed attempts pass ``outcome`` + ``error`` and leave
        usage null (a failed attempt rarely reports tokens)."""
        usage = usage or TokenUsage()
        InferenceLog(
            request_id=self.request_id,
            user_id=self.user.id,
            endpoint=payload.endpoint,
            model=model.name,
            deployment=deployment.name,
            cluster=deployment.cluster_name,
            backend_id=backend.id,
            backend_model_url=backend.model_url,
            outcome=outcome,
            attempt=self.attempt,
            error=error,
            latency_sec=latency_sec,
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            total_tokens=usage.total_tokens,
            cache_read_tokens=usage.cache_read_tokens,
            cache_write_tokens=usage.cache_write_tokens,
            reasoning_tokens=usage.reasoning_tokens,
        ).emit(raw_body=raw_body, storage_dir=self.storage_dir)

    def _raise_for_upstream_status(
        self, status_code: int, body: str, model_name: str
    ) -> NoReturn:
        """Classify a non-200 upstream response and raise the right error."""
        logger.warning(f"Backend error {status_code} for model {model_name}: {body}")
        if 400 <= status_code < 500:
            # Propagate the backend's status and content to the caller as-is.
            raise FirstError(
                body or f"Upstream model server returned status {status_code}.",
                status_code=status_code,
            )
        raise _UpstreamFailure(
            f"HTTP {status_code}", retryable=status_code in _RETRYABLE_STATUS
        )

    async def _handle_unary(
        self,
        client: AsyncClient,
        payload: BasePayload,
        model: ModelConfig,
        deployment: DeploymentConfig,
        backend: BackendConfig,
    ) -> JSONResponse:
        """POST to an inference backend."""

        parser = USAGE_PARSERS.get(payload.endpoint)

        upstream_payload = payload.model_dump(
            exclude_unset=True, mode="json", exclude={"endpoint"}
        )
        if parser:
            upstream_payload = parser.prepare_request(upstream_payload)

        t0 = time.perf_counter()
        try:
            response = await client.post(
                f"/v1/{payload.endpoint}", json=upstream_payload
            )
        except httpx.RequestError as exc:
            logger.warning(
                f"Request error contacting backend for model {model.name}.",
                exc_info=True,
            )
            raise _UpstreamFailure(type(exc).__name__, retryable=True)

        if response.status_code != 200:
            body = (await response.aread()).decode(errors="replace")
            await response.aclose()
            self._raise_for_upstream_status(response.status_code, body, model.name)

        latency_sec = time.perf_counter() - t0
        json_body: dict[str, Any] = response.json()
        assert isinstance(json_body, dict)
        usage = parser.parse_unary(json_body) if parser else TokenUsage()

        await self.admission_controller.settle(
            self.request_id, actual_tokens=usage.total_tokens or 0
        )
        await self._record_token_stats(payload, model, usage)
        self._emit_inference_log(
            payload,
            model,
            deployment,
            backend,
            outcome="success",
            usage=usage,
            latency_sec=latency_sec,
            raw_body=response.content,
        )
        return JSONResponse(json_body, status_code=response.status_code)

    async def _handle_streaming(
        self,
        client: AsyncClient,
        payload: BasePayload,
        model: ModelConfig,
        deployment: DeploymentConfig,
        backend: BackendConfig,
    ) -> StreamingResponse:
        """POST to an inference backend and relay the SSE stream to the caller."""

        parser = USAGE_PARSERS.get(payload.endpoint)

        upstream_payload = payload.model_dump(
            exclude_unset=True, mode="json", exclude={"endpoint"}
        )
        upstream_payload["stream"] = True
        if parser:
            upstream_payload = parser.prepare_request(upstream_payload)

        request = client.build_request(
            "POST",
            f"/v1/{payload.endpoint}",
            json=upstream_payload,
        )

        t0 = time.perf_counter()
        try:
            response = await client.send(request, stream=True)
        except httpx.RequestError as exc:
            logger.warning(
                f"Request error contacting backend for model {model.name}.",
                exc_info=True,
            )
            raise _UpstreamFailure(type(exc).__name__, retryable=True)

        if response.status_code != 200:
            body = (await response.aread()).decode(errors="replace")
            await response.aclose()
            self._raise_for_upstream_status(response.status_code, body, model.name)

        async def _relay() -> AsyncIterator[bytes]:
            tap = UsageTap()
            content = bytearray()
            try:
                async for chunk in response.aiter_raw():
                    tap.feed(chunk)
                    content += chunk
                    yield chunk
            finally:
                tap.close()
                with anyio.CancelScope(shield=True):
                    await response.aclose()
                usage = (
                    parser.parse_stream(tap.first, tap.last) if parser else TokenUsage()
                )
                await self.admission_controller.settle(
                    self.request_id, actual_tokens=usage.total_tokens or 0
                )
                self._emit_inference_log(
                    payload,
                    model,
                    deployment,
                    backend,
                    outcome="success",
                    usage=usage,
                    latency_sec=time.perf_counter() - t0,
                    raw_body=bytes(content),
                )
                await self._record_token_stats(payload, model, usage)

        return StreamingResponse(
            _relay(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )


InferenceServiceDep = Annotated[InferenceService, Depends(InferenceService)]
