import logging
from typing import Any

from ninja import Router

from ..errors import UnsupportedEndpoint
from ..logging import get_request_context
from ..schemas.auth import AuthedRequest
from ..schemas.openai_chat_completions import (
    OpenAIChatCompletionsPydantic,
)
from ..schemas.openai_completions import OpenAICompletionsPydantic
from ..schemas.openai_embeddings import OpenAIEmbeddingsPydantic
from ..schemas.openai_responses import (
    OpenAIResponsesPydantic,
)
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
    payload: OpenAIChatCompletionsPydantic,
) -> Any:
    return await submit_openai_inference_request(
        get_request_context(), cluster_name, framework, payload
    )


@router.post("/{cluster_name}/{framework}/v1/completions")
async def create_completion(
    request: AuthedRequest,
    cluster_name: str,
    framework: str,
    payload: OpenAICompletionsPydantic,
) -> Any:
    return await submit_openai_inference_request(
        get_request_context(), cluster_name, framework, payload
    )


@router.post("/{cluster_name}/{framework}/v1/embeddings")
async def create_embedding(
    request: AuthedRequest,
    cluster_name: str,
    framework: str,
    payload: OpenAIEmbeddingsPydantic,
) -> Any:
    return await submit_openai_inference_request(
        get_request_context(), cluster_name, framework, payload
    )


@router.post("/{cluster_name}/{framework}/v1/responses")
async def create_response(
    request: AuthedRequest,
    cluster_name: str,
    framework: str,
    payload: OpenAIResponsesPydantic,
) -> Any:
    if payload.stream:
        raise UnsupportedEndpoint(
            "Streaming is not supported for the OpenAI Responses API on this "
            "gateway. Re-issue the request with 'stream': false."
        )
    return await submit_openai_inference_request(
        get_request_context(), cluster_name, framework, payload
    )
