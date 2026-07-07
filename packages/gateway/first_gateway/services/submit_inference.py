from typing import Any, TypeVar

from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from ..database.redis.router_config import BackendConfig

PayloadT = TypeVar("PayloadT", bound=BaseModel)


async def submit_inference(
    backend: BackendConfig,
    payload: PayloadT,
) -> StreamingResponse | dict[str, Any]:
    """POST to an inference backend."""

    # TODO: Submit and handle streaming / non-streaming
    return {"Mock response": True}

    payload = payload.model_dump(exclude_none=True)
    backend = backend  # This is just to mute lint-fix error

    # headers = {"Content-Type": "application/json"}
    # if backend.api_key:
    #    headers = {"Authorization": f"Bearer {backend.api_key}"}
