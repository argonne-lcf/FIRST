import logging
from typing import Any, AsyncIterator, cast

import anyio
from fastapi.responses import StreamingResponse
from httpx import AsyncClient

from first_common.errors import ServiceUnavailable
from first_common.schema.endpoints.base import BasePayload
from first_gateway.apiserver.dependencies import AuthUser

from ..apiserver.backend_client_manager import BackendClientManager
from ..database.redis.admission import AdmissionController
from ..database.redis.router_config import ModelConfig
from .orchestration import (
    admit_request,
    get_backend_candidates,
    get_deployment_from_backend_id,
)
from .usage import USAGE_PARSERS, TokenUsage, UsageTap

logger = logging.getLogger(__name__)


async def submit_inference_with_retry(
    user: AuthUser,
    model: ModelConfig,
    admission_controller: AdmissionController,
    backend_client_manager: BackendClientManager,
    payload: BasePayload,
    request_id: str,
    deployment_name: str | None = None,
) -> StreamingResponse | dict[str, Any]:
    """
    Send inference requests to targeted backends using the admission
    controler to select the appropriate backends. This function tries
    to submit the request and return the response if successful, but
    fallover to the next most appropriate backends in case of failures.
    """

    # Only collect backend candidates that have a httpx client ready
    backend_candidates = get_backend_candidates(model, deployment_name=deployment_name)
    backend_candidates = [
        b for b in backend_candidates if b.uid in backend_client_manager.clients
    ]

    estimated_tokens = payload.estimate_tokens(model.max_model_len)
    streaming = getattr(payload, "stream", False) or False

    # Attempt at least once to make sure admit_request is called
    for attempt in range(max(1, min(3, len(backend_candidates)))):
        backend_id = await admit_request(
            user,
            model,
            admission_controller,
            backend_candidates,
            request_id,
            estimated_tokens=estimated_tokens,
        )

        client = backend_client_manager.get(backend_id)
        assert client is not None  # To mute make mypy error

        try:
            response: StreamingResponse | dict[str, Any]
            if streaming:
                response = await submit_inference_streaming(
                    client,
                    payload,
                    admission_controller,
                    request_id,
                    user,
                    model,
                )
            else:
                response = await submit_inference(
                    client,
                    payload,
                    admission_controller,
                    request_id,
                    user,
                    model,
                )
        except:
            # TODO: figure out a way to gather and report errors
            deployment = get_deployment_from_backend_id(model.deployments, backend_id)
            await admission_controller.record_error(
                backend_id, deployment.router_params
            )
            await admission_controller.settle(request_id, actual_tokens=0)
            backend_candidates = [b for b in backend_candidates if b.uid != backend_id]
            logger.error(
                f"Backend {backend_id} failed, trying next one.", exc_info=True
            )
        else:
            return response

    # Error if none of the attempts worked
    raise ServiceUnavailable(
        f"Too many failed attempts on backends for model {model.name}."
    )


async def submit_inference(
    client: AsyncClient,
    payload: BasePayload,
    admission_controller: AdmissionController,
    request_id: str,
    user: AuthUser,
    model: ModelConfig,
) -> dict[str, Any]:
    """POST to an inference backend."""

    response = await client.post(
        f"/v1/{payload.endpoint}",
        json=payload.model_dump(exclude_unset=True, mode="json"),
    )

    if response.status_code != 200:
        body = await response.aread()
        await response.aclose()
        # TODO: streaming error handling
        logger.warning(
            f"Received error from backend: {body[-512:].decode(errors='replace')}"
        )
        response.raise_for_status()

    json_body = cast(dict[str, Any], response.json())
    parser = USAGE_PARSERS.get(payload.endpoint)
    usage = parser.parse_unary(json_body) if parser else TokenUsage()
    # TODO: emit structured log events (this is a placeholder for visibility):
    logger.info(f"{payload.endpoint} - {model.name} - {user.username} - {usage}")
    await admission_controller.settle(request_id, actual_tokens=usage.total_tokens or 0)
    return json_body


async def submit_inference_streaming(
    client: AsyncClient,
    payload: BasePayload,
    admission_controller: AdmissionController,
    request_id: str,
    user: AuthUser,
    model: ModelConfig,
) -> StreamingResponse:
    """POST to an inference backend and relay the SSE stream to the caller."""

    setattr(payload, "stream", True)
    request = client.build_request(
        "POST",
        f"/v1/{payload.endpoint}",
        json=payload.model_dump(exclude_unset=True, mode="json"),
    )

    response = await client.send(request, stream=True)
    response.raise_for_status()

    parser = USAGE_PARSERS.get(payload.endpoint)

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
            usage = parser.parse_stream(tap.first, tap.last) if parser else TokenUsage()
            total_tokens = usage.total_tokens or 0
            logger.info(
                f"{payload.endpoint} - {model.name} - {user.username} - {usage}"
            )
            await admission_controller.settle(request_id, actual_tokens=total_tokens)

    return StreamingResponse(
        _relay(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
