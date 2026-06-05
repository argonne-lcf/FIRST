import logging
from typing import Any

from ninja import Router

from ..errors import UnsupportedEndpoint
from ..logging import get_request_context
from ..schemas.anthropic_messages import AnthropicMessagesPydantic
from ..schemas.auth import AuthedRequest
from ..services import (
    submit_openai_inference_request,
)

router = Router()
log = logging.getLogger(__name__)


@router.post("/{cluster_name}/{framework}/v1/messages")
async def create_message(
    request: AuthedRequest,
    cluster_name: str,
    framework: str,
    payload: AnthropicMessagesPydantic,
) -> Any:

    if payload.stream:
        raise UnsupportedEndpoint(
            "Streaming is not supported for the Anthropic Messages API on "
            "this gateway. Re-issue the request with 'stream': false."
        )

    return await submit_openai_inference_request(
        get_request_context(), cluster_name, framework, payload
    )
