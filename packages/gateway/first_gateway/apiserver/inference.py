import logging
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

from ..database.redis.admission import AdmissionController
from ..database.redis.router_config import DeploymentConfig, ModelConfig
from ..services.orchestration import (
    admit_request,
    get_deployment_from_backend_id,
    get_shuffled_backends,
)
from ..services.usage import USAGE_PARSERS, TokenUsage, UsageTap
from .auth import enforce_permission
from .backend_client_manager import BackendClientManager
from .dependencies import AuthUser
from .router_config_manager import RouterConfigManager

logger = logging.getLogger(__name__)

MAX_ATTEMPTS = 3
_RETRYABLE_STATUS = (502, 503, 504)

UpstreamHandler = Callable[
    [AsyncClient, BasePayload, ModelConfig],
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
        self.request_id = str(uuid.uuid4())

        state = request.app.state
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

        estimated_tokens = payload.estimate_tokens(model.max_model_len)
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
            )

            client = self.backend_client_manager.get(backend_id)
            assert client is not None, "Should be filtered by existing clients"

            deployment = get_deployment_from_backend_id(model.deployments, backend_id)
            payload.model = next(
                b.backend_model_name for b in deployment.backends if b.id == backend_id
            )

            attempted += 1
            try:
                return await handler(client, payload, model)
            except _UpstreamFailure as exc:
                await self._release_failed_backend(backend_id, deployment)
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
            except FirstError:
                # Upstream 4xx: settle & propagate response without penalising the backend
                await self.admission_controller.settle(self.request_id, actual_tokens=0)
                raise
            except Exception:
                await self._release_failed_backend(backend_id, deployment)
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

    def _get_upstream_handler(self, payload: BasePayload) -> UpstreamHandler:
        streaming = getattr(payload, "stream", False) or False
        return self._handle_streaming if streaming else self._handle_unary

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
        self, client: AsyncClient, payload: BasePayload, model: ModelConfig
    ) -> JSONResponse:
        """POST to an inference backend."""

        parser = USAGE_PARSERS.get(payload.endpoint)

        upstream_payload = payload.model_dump(
            exclude_unset=True, mode="json", exclude={"endpoint"}
        )
        if parser:
            upstream_payload = parser.prepare_request(upstream_payload)

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

        json_body: dict[str, Any] = response.json()
        assert isinstance(json_body, dict)
        usage = parser.parse_unary(json_body) if parser else TokenUsage()

        # TODO: emit structured log events (this is a placeholder for visibility):
        logger.info(
            f"{payload.endpoint} - {model.name} - {self.user.username} - {usage}"
        )
        await self.admission_controller.settle(
            self.request_id, actual_tokens=usage.total_tokens or 0
        )
        return JSONResponse(json_body, status_code=response.status_code)

    async def _handle_streaming(
        self, client: AsyncClient, payload: BasePayload, model: ModelConfig
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
            try:
                async for chunk in response.aiter_raw():
                    tap.feed(chunk)
                    yield chunk
            finally:
                tap.close()
                with anyio.CancelScope(shield=True):
                    await response.aclose()
                usage = (
                    parser.parse_stream(tap.first, tap.last) if parser else TokenUsage()
                )
                total_tokens = usage.total_tokens or 0
                logger.info(
                    f"{payload.endpoint} - {model.name} - {self.user.username} - {usage}"
                )
                await self.admission_controller.settle(
                    self.request_id, actual_tokens=total_tokens
                )

        return StreamingResponse(
            _relay(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )


InferenceServiceDep = Annotated[InferenceService, Depends(InferenceService)]
