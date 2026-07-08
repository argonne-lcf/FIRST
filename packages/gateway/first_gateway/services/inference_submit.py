from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from fastapi.responses import StreamingResponse
from pydantic import BaseModel

PayloadT = TypeVar("PayloadT", bound=BaseModel)


async def submit_openai_request(
    payload: PayloadT,
    call: Callable[..., Awaitable[Any]],
) -> StreamingResponse | dict[str, Any]:

    kwargs = payload.model_dump(exclude_none=True)

    if kwargs.get("stream"):

        async def stream():
            response = await call(**kwargs)
            async for chunk in response:
                yield f"data: {chunk.model_dump_json()}\n\n"
            yield "data: [DONE]\n\n"

        return StreamingResponse(stream(), media_type="text/event-stream")

    response = await call(**kwargs)
    return response.model_dump()
