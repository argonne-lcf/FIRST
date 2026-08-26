import logging
from typing import Any, cast

from fastapi.responses import StreamingResponse

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
from .usage import USAGE_PARSERS

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

    backend_candidates = get_backend_candidates(model, deployment_name=deployment_name)
    if not backend_candidates:
        raise ServiceUnavailable(f"No backend available for model {model}.")

    estimated_tokens = payload.estimate_tokens(model.max_model_len)

    for attempt in range(min(3, len(backend_candidates))):
        backend_id = await admit_request(
            user,
            model,
            admission_controller,
            backend_candidates,
            request_id,
            estimated_tokens=estimated_tokens,
        )
        total_tokens = 0

        try:
            response = await submit_inference(
                backend_id, backend_client_manager, payload
            )
        except:
            # TODO: figure out a way to gather and report errors
            deployment = get_deployment_from_backend_id(model.deployments, backend_id)
            await admission_controller.record_error(
                backend_id, deployment.router_params
            )
            backend_candidates = [b for b in backend_candidates if b.uid != backend_id]
            logger.warning(f"Backend {backend_id} failed, trying next one.")
        else:
            usage = USAGE_PARSERS[payload.endpoint].parse_unary(response)
            total_tokens = usage.total_tokens or 0
            # TODO: emit structured log events (this is a placeholder for visibility):
            logger.info(
                f"{payload.endpoint} - {model.name} - {user.username} - {usage}"
            )
            return response
        finally:
            await admission_controller.settle(request_id, actual_tokens=total_tokens)

    # Error if none of the attempts worked
    raise ServiceUnavailable(
        f"Too many failed attempts on backends for model {model.name}."
    )


async def submit_inference(
    backend_id: str,
    backend_client_manager: BackendClientManager,
    payload: BasePayload,
) -> dict[str, Any]:
    """POST to an inference backend."""

    client = backend_client_manager.get(backend_id)
    if client is None:
        raise ServiceUnavailable(f"No client exist for backend ID {backend_id}.")

    # TODO: handle streaming
    response = await client.post(
        f"/v1/{payload.endpoint}",
        json=payload.model_dump(exclude_unset=True, mode="json"),
    )
    response.raise_for_status()
    return cast(dict[str, Any], response.json())
