import logging
from typing import Any

from fastapi.responses import StreamingResponse

from first_common.errors import ServiceUnavailable
from first_common.schema.endpoints.base import BasePayload
from first_common.schema.endpoints.openai import OpenAIEndpoints
from first_gateway.apiserver.dependencies import AuthUser

from ..apiserver.backend_client_manager import BackendClientManager
from ..database.redis.admission import AdmissionController
from ..database.redis.router_config import ModelConfig
from .orchestration import (
    get_backend_candidates,
    get_backend_id,
    get_deployment_from_backend_id,
)

logger = logging.getLogger(__name__)


async def submit_inference_with_retry(
    user: AuthUser,
    model: ModelConfig,
    admission_controller: AdmissionController,
    backend_client_manager: BackendClientManager,
    payload: BasePayload,
    endpoint: OpenAIEndpoints,
    deployment_name: str | None = None,
) -> StreamingResponse | dict[str, Any]:
    """
    Send inference requests to targeted backends using the admission
    controler to select the appropriate backends. This function tries
    to submit the request and return the response if successful, but
    fallover to the next most appropriate backends in case of failures.
    """

    backend_candidates = get_backend_candidates(model, deployment_name=deployment_name)
    if not backend_candidates:
        raise ServiceUnavailable(f"No backend available for model {model}.")

    # TODO: figure out a better token estimate and max_output_tokens
    # estimated_tokens = len(json.dumps(payload)) // 4 + max_output_tokens
    estimated_tokens = len(payload.model_dump()) // 2

    for attempt in range(min(3, len(backend_candidates))):
        backend_id = await get_backend_id(
            user,
            model,
            admission_controller,
            backend_candidates,
            estimated_tokens=estimated_tokens,
        )

        try:
            response = await submit_inference(
                backend_id, backend_client_manager, payload, endpoint
            )
        except:
            # TODO: figure out a way to gather and report errors
            deployment = get_deployment_from_backend_id(model.deployments, backend_id)
            await admission_controller.record_error(
                backend_id, deployment.router_params
            )
            backend_candidates = [b for b in backend_candidates if b.uid != backend_id]
            token_usage = 0
            logger.warning(f"Backend {backend_id} failed, trying next one.")
        else:
            # TODO: implement parse_token_usage
            # token_usage = parse_token_usage(response)
            token_usage = 1  # TEMPORARY
            return response
        finally:
            # TODO: Incorporate request ID
            await admission_controller.settle(
                "temporary_request_id", actual_tokens=token_usage
            )

    # Error if none of the attempts worked
    raise ServiceUnavailable(
        f"Too many failed attempts on backends for model {model.name}."
    )


async def submit_inference(
    backend_id: str,
    backend_client_manager: BackendClientManager,
    payload: BasePayload,
    endpoint: OpenAIEndpoints,
) -> dict[str, Any]:
    """POST to an inference backend."""

    client = backend_client_manager.get(backend_id)
    if client is None:
        raise ServiceUnavailable(f"No client exist for backend ID {backend_id}.")

    # TODO: handle streaming
    # TODO: make endpoint generic and not specific to openai
    response = await client.post(
        f"/v1/{endpoint.value}",
        json=payload.model_dump(exclude_unset=True, mode="json"),
    )
    response.raise_for_status()
    return response.json()
