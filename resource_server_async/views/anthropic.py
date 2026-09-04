import logging
from typing import Any

from ninja import Body, Router

from ..logging import get_request_context
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
    payload: Body[dict[str, Any]],
) -> Any:
    return await submit_openai_inference_request(
        get_request_context(),
        cluster_name,
        framework,
        payload,
        openai_endpoint="messages",
    )
