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


@router.post("/{cluster_name}/{framework}/v1/chat/completions")
async def create_chat_completion(
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
        openai_endpoint="chat/completions",
    )


@router.post("/{cluster_name}/{framework}/v1/completions")
async def create_completion(
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
        openai_endpoint="completions",
    )


@router.post("/{cluster_name}/{framework}/v1/embeddings")
async def create_embedding(
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
        openai_endpoint="embeddings",
    )


@router.post("/{cluster_name}/{framework}/v1/responses")
async def create_response(
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
        openai_endpoint="responses",
    )
