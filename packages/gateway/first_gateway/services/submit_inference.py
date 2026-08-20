import logging
from typing import Any

from fastapi.responses import StreamingResponse

from first_common.errors import ServiceUnavailable
from first_common.schema.endpoints.base import BasePayload
from first_gateway.apiserver.dependencies import AuthUser

from ..database.redis.admission import AdmissionController
from ..database.redis.router_config import BackendConfig, ModelConfig
from .orchestration import (
    get_backend,
    get_backend_candidates,
    get_deployment_from_backend,
)

logger = logging.getLogger(__name__)


async def submit_inference_with_retry(
    user: AuthUser,
    model: ModelConfig,
    admission_controller: AdmissionController,
    payload: BasePayload,
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

    for attempt in range(len(backend_candidates)):
        backend = await get_backend(
            user,
            model,
            admission_controller,
            backend_candidates,
            estimated_tokens=estimated_tokens,
        )

        try:
            response = await submit_inference(backend, payload)
        except:
            deployment = get_deployment_from_backend(model.deployments, backend)
            admission_controller.record_error(backend.id, deployment.router_params)
            backend_candidates = [b for b in backend_candidates if b.uid != backend.id]
            token_usage = 0
            logger.warning(f"Backend {backend.id} failed, trying next one.")
        else:
            # TODO: implement parse_token_usage
            # token_usage = parse_token_usage(response)
            token_usage = 1  # TEMPORARY
            return response
        finally:
            # TODO: Incorporate request ID
            admission_controller.settle(
                "temporary_request_id", actual_tokens=token_usage
            )


async def submit_inference(
    backend: BackendConfig,
    payload: BasePayload,
) -> StreamingResponse | dict[str, Any]:
    """POST to an inference backend."""

    # TODO: Submit and handle streaming / non-streaming
    return {"Mock response": True}

    payload = payload.model_dump(exclude_unset=True, mode="json")
    backend = backend  # This is just to mute lint-fix error

    # headers = {"Content-Type": "application/json"}
    # if backend.api_key:
    #    headers = {"Authorization": f"Bearer {backend.api_key}"}
