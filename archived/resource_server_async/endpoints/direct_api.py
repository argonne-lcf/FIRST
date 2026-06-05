import asyncio
import json
import logging
import os
import time
from typing import Any, AsyncGenerator, TypedDict

import httpx
from django.http import StreamingHttpResponse
from pydantic import BaseModel

from resource_server_async.endpoints.endpoint import (
    BaseEndpoint,
)
from resource_server_async.httpx_client import AsyncHttpClient
from resource_server_async.streaming import create_streaming_response_headers

from ..errors import EndpointError
from ..logging import RequestContext, get_request_context
from ..schemas.endpoints import (
    SubmitStreamingTaskResponse,
    SubmitTaskResult,
)

log = logging.getLogger(__name__)


class DirectAPIEndpointConfig(BaseModel):
    api_url: str
    api_key_env_name: str
    api_request_timeout: int = 120


class StreamingState(TypedDict):
    chunks: list[str]
    total_chunks: int
    completed: bool
    error: str | None
    start_time: float


# DirectAPI endpoint implementation of a BaseEndpoint
class DirectAPIEndpoint(BaseEndpoint):
    """Direct API endpoint implementation of BaseEndpoint."""

    # Class initialization
    def __init__(
        self,
        id: str,
        endpoint_slug: str,
        cluster: str,
        framework: str,
        model: str,
        endpoint_adapter: str,
        tpm_model: int,
        tpm_user: int,
        config: dict[str, Any],
        allowed_globus_groups: list[str] | None = None,
        allowed_domains: list[str] | None = None,
    ):
        # Validate and assign endpoint configuration
        self.__config = DirectAPIEndpointConfig(**config)

        # Create HTTPx async client
        self.__httpx_client = AsyncHttpClient(
            timeout=self.__config.api_request_timeout,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {os.environ.get(self.__config.api_key_env_name, None)}",
            },
        )

        # Initialize the rest of the common attributes
        super().__init__(
            id,
            endpoint_slug,
            cluster,
            framework,
            model,
            endpoint_adapter,
            tpm_model,
            tpm_user,
            allowed_globus_groups,
            allowed_domains,
        )

    # Submit task
    async def submit_task(self, data: dict[str, Any]) -> SubmitTaskResult:
        """Submits a single interactive task to the compute resource."""
        endpoint = data.pop("openai_endpoint", "chat/completions").strip("/")
        url = f"{self.config.api_url.rstrip('/')}/{endpoint}"

        # Submit POST call and wait for the response
        try:
            response = await self.httpx_client.post(url, data=data)
        except httpx.HTTPStatusError as e:
            raise EndpointError(
                f"Upstream endpoint returned {e.response.status_code}: {e.response.content[:256]!r}.",
                status_code=e.response.status_code,
            )
        except httpx.TimeoutException:
            raise EndpointError(
                f"Timeout calling {url}.",
                status_code=504,
                info={"timeout": self.config.api_request_timeout},
            )
        except httpx.HTTPError as e:
            raise EndpointError(
                f"HTTP error calling API at {url}: {e}", status_code=500
            )

        return SubmitTaskResult(result=response, task_id=None)

    # Call stream API
    async def submit_streaming_task(
        self, data: dict[str, Any]
    ) -> SubmitStreamingTaskResponse:
        """Submits a single interactive task to the compute resource with streaming enabled."""

        # Shared state for tracking streaming (optimized - minimal memory)
        streaming_state: StreamingState = {
            "chunks": [],  # Limited to 100 chunks
            "total_chunks": 0,
            "completed": False,
            "error": None,
            "start_time": time.time(),
        }

        # SSE generator
        async def sse_generator() -> AsyncGenerator[str, None]:
            """Stream SSE chunks from API."""

            # For each streaming chunk ...
            try:
                async for chunk in self.__get_stream_chunks(data):
                    if chunk:
                        # Send chunk
                        streaming_state["total_chunks"] += 1
                        yield chunk  # Pass through SSE format

                        # Collect limited chunks for logging (optimize memory)
                        if chunk.startswith("data: ") and not chunk.startswith(
                            "data: [DONE]"
                        ):
                            if len(streaming_state["chunks"]) < 100:
                                try:
                                    streaming_state["chunks"].append(chunk[6:].strip())
                                except:
                                    pass

                streaming_state["completed"] = True

            # Send error as OpenAI streaming chunk format (compatible with OpenAI clients)
            except Exception as e:
                error_str = str(e)
                streaming_state["error"] = error_str
                streaming_state["completed"] = True
                error_chunk = {
                    "id": "chatcmpl-api-error",
                    "object": "chat.completion.chunk",
                    "created": int(time.time()),
                    "model": self.model,
                    "choices": [
                        {
                            "index": 0,
                            "delta": {
                                "role": "assistant",
                                "content": f"\n\n[ERROR] {error_str}",
                            },
                            "finish_reason": "stop",
                        }
                    ],
                }
                yield f"data: {json.dumps(error_chunk)}\n\n"
                yield "data: [DONE]\n\n"

        try:
            context = get_request_context()
            asyncio.create_task(self.__update_streaming_log(context, streaming_state))
        except LookupError:
            pass

        # Create streaming response
        response = StreamingHttpResponse(
            streaming_content=sse_generator(), content_type="text/event-stream"
        )

        # Set SSE headers
        for key, value in create_streaming_response_headers().items():
            response[key] = value

        # Return streaming response
        return SubmitStreamingTaskResponse(response=response, task_id=None)

    # Get stream chunks
    async def __get_stream_chunks(
        self, data: dict[str, Any]
    ) -> AsyncGenerator[str, None]:
        """Make a direct API streaming call to the endpoint."""
        endpoint = data.pop("openai_endpoint", "chat/completions").strip("/")
        url = f"{self.config.api_url.rstrip('/')}/{endpoint}"

        # Create an async HTTPx client
        try:
            async with httpx.AsyncClient(
                timeout=self.config.api_request_timeout
            ) as client:
                # Create a streaming client
                async with client.stream(
                    "POST",
                    url,
                    json=data,
                    headers=self.httpx_client.headers,
                ) as response:
                    # Return error if something went wrong
                    if response.status_code != 200:
                        error_text = await response.aread()
                        raise ValueError(
                            f"Error: Could not send stream API call to {url}: {error_text.decode().strip()}"
                        )

                    # Stream the response
                    async for chunk in response.aiter_text():
                        if chunk:
                            yield chunk

        # Errors
        except httpx.TimeoutException:
            raise ValueError(
                f"Error: Timeout calling stream API at {url} (timeout: {self.config.api_request_timeout})"
            )
        except httpx.HTTPError as e:
            raise ValueError(f"Error: HTTP error calling stream API at {url}: {e}")
        except Exception as e:
            raise ValueError(f"Error: Unexpected error calling stream API: {e}")

    # Update streaming log
    async def __update_streaming_log(
        self, context: RequestContext, streaming_state: StreamingState
    ) -> None:
        """Background task to log after streaming completes."""
        try:
            # Wait for completion (efficient polling with timeout)
            max_wait = 600  # 10 minutes
            waited = 0.0
            poll_interval = 0.5  # 500ms
            while not streaming_state["completed"] and waited < max_wait:
                await asyncio.sleep(poll_interval)
                waited += poll_interval

            # Get metrics
            duration = time.time() - streaming_state["start_time"]
            total_chunks = streaming_state["total_chunks"]

            # Log error if something went wrong
            if streaming_state["error"]:
                result = f"error: {streaming_state['error']}"
                log.error(
                    f"API streaming failed for {self.endpoint_slug}: {streaming_state['error']}"
                )

            # Store limited chunks or completion marker
            else:
                result = (
                    "\n".join(streaming_state["chunks"])
                    if streaming_state["chunks"]
                    else "streaming_completed"
                )
                log.info(
                    f"Metis streaming completed for {self.endpoint_slug}: {total_chunks} chunks in {duration:.2f}s"
                )

            if context.request_log:
                context.request_log.emit(result, status_code=None)

        # Log error if something went wrong
        except Exception as e:
            log.error(f"Error in update_streaming_log: {e}")

    # Read-only access to the configuration
    @property
    def config(self) -> DirectAPIEndpointConfig:
        return self.__config

    # Read-only access to HTTPx client
    @property
    def httpx_client(self) -> AsyncHttpClient:
        return self.__httpx_client

    # Overwrite function
    def set_api_url(self, api_url: str) -> None:
        self.__config.api_url = api_url
