"""Isolated endpoint adapter for V2-managed (pilot) backends."""

import asyncio
import logging
import random
from typing import Any, AsyncGenerator

import httpx
from django.http import StreamingHttpResponse
from pydantic import BaseModel

from resource_server_async.endpoints.direct_api import (
    DirectAPIEndpoint,
    new_streaming_state,
)
from resource_server_async.endpoints.endpoint import BaseEndpoint
from resource_server_async.httpx_client import create_ssl_context
from resource_server_async.streaming import (
    create_streaming_response_headers,
    include_streaming_usage,
)

from ..errors import EndpointError
from ..logging import get_request_context
from ..schemas.endpoints import (
    SubmitStreamingTaskResponse,
    SubmitTaskResult,
)

log = logging.getLogger(__name__)

# Large-context pilot requests can spend several minutes in prefill. This is a
# read-inactivity limit, not a total request/stream duration limit.
_REQUEST_TIMEOUT = 900


class FirstV2EndpointConfig(BaseModel):
    model_urls: list[str]
    backend_model_name: str
    ca_cert_path: str | None = None
    client_cert_path: str | None = None
    client_key_path: str | None = None
    proxy_url: str | None = None
    check_hostname: bool = False
    trust_env: bool = False


class FirstV2Endpoint(DirectAPIEndpoint):
    """Endpoint adapter proxying to V2-managed pilot backends over mTLS."""

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
        self._cfg = FirstV2EndpointConfig(**config)

        # Isolated client: mTLS via client cert, optional proxy, no bearer.
        verify = create_ssl_context(
            ca_cert_path=self._cfg.ca_cert_path,
            client_cert_path=self._cfg.client_cert_path,
            client_key_path=self._cfg.client_key_path,
            check_hostname=self._cfg.check_hostname,
        )
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=5.0, read=_REQUEST_TIMEOUT, write=60.0, pool=5.0
            ),
            headers={"Content-Type": "application/json"},
            verify=verify,
            proxy=self._cfg.proxy_url or None,
            trust_env=self._cfg.trust_env,
        )

        # Initialize common attrs/permissions/token-limiter only; bypass
        # DirectAPIEndpoint.__init__ so its own client/config are not built.
        BaseEndpoint.__init__(
            self,
            id,
            endpoint_slug,
            cluster,
            framework,
            model,
            endpoint_adapter,
            tpm_model,
            tpm_user,
            config,
            allowed_globus_groups,
            allowed_domains,
        )

    def _pick_url(self) -> str:
        return random.choice(self._cfg.model_urls)

    def _build_request(
        self, data: dict[str, Any], *, stream: bool
    ) -> tuple[str, dict[str, Any]]:
        """Unwrap model_params (Metis/Minerva pattern) and build the target URL."""
        body = {**data["model_params"]}
        body["stream"] = stream
        body.pop("api_port", None)
        openai_endpoint = str(body.pop("openai_endpoint", "chat/completions")).strip(
            "/"
        )
        # Backends expect their own model name, not the alias.
        body["model"] = self._cfg.backend_model_name
        if stream:
            include_streaming_usage(body, openai_endpoint)
        url = f"{self._pick_url().rstrip('/')}/v1/{openai_endpoint}"
        return url, body

    async def submit_task(self, data: dict[str, Any]) -> SubmitTaskResult:
        url, body = self._build_request(data, stream=False)
        log.info(f"Making First V2 API call for model {self.model} (stream=False)")
        try:
            response = await self._client.post(url, json=body)
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise EndpointError(
                f"Upstream endpoint returned {e.response.status_code}: {e.response.content[:256]!r}.",
                status_code=e.response.status_code,
            )
        except httpx.TimeoutException:
            raise EndpointError(
                f"Timeout calling {url}.",
                status_code=504,
                info={"timeout": _REQUEST_TIMEOUT},
            )
        except httpx.HTTPError as e:
            raise EndpointError(
                f"HTTP error calling API at {url}: {e}", status_code=500
            )

        return SubmitTaskResult(result=response.json(), task_id=None)

    async def submit_streaming_task(
        self, data: dict[str, Any]
    ) -> SubmitStreamingTaskResponse:
        url, body = self._build_request(data, stream=True)
        openai_endpoint = url.rsplit("/v1/", 1)[-1]
        log.info(f"Making First V2 API call for model {self.model} (stream=True)")

        streaming_state = new_streaming_state()
        try:
            # Capture the request's logs before the lazy response outlives its
            # ContextVar scope. An unconsumed response needs no final-log task.
            context = get_request_context()
        except LookupError:
            context = None

        async def upstream_chunks() -> AsyncGenerator[str, None]:
            try:
                async with self._client.stream("POST", url, json=body) as response:
                    if response.status_code != 200:
                        error_text = await response.aread()
                        log.warning(
                            "FIRST V2 upstream stream returned status %s: %s",
                            response.status_code,
                            error_text.decode(errors="replace")[:512],
                        )
                        raise EndpointError(
                            "Upstream inference request failed.",
                            status_code=response.status_code,
                        )
                    async for chunk in response.aiter_text():
                        if chunk:
                            yield chunk
            except EndpointError:
                raise
            except httpx.TimeoutException as e:
                raise EndpointError(
                    "Timeout calling the upstream inference endpoint.",
                    status_code=504,
                ) from e
            except httpx.HTTPError as e:
                raise EndpointError(
                    "HTTP error calling the upstream inference endpoint.",
                    status_code=502,
                ) from e
            except Exception as e:
                raise EndpointError(
                    "Unexpected error calling the upstream inference endpoint.",
                    status_code=502,
                ) from e

        upstream = upstream_chunks()
        try:
            first_chunk = await anext(upstream)
        except StopAsyncIteration:
            first_chunk = ""

        async def sse_generator() -> AsyncGenerator[str, None]:
            try:
                if first_chunk:
                    streaming_state["total_chunks"] += 1
                    self._collect_streaming_chunk(streaming_state, first_chunk)
                    yield first_chunk
                async for chunk in upstream:
                    streaming_state["total_chunks"] += 1
                    self._collect_streaming_chunk(streaming_state, chunk)
                    yield chunk
            except Exception as e:
                streaming_state["error"] = str(e)
                log.exception("FIRST V2 stream failed")
                yield self._sse_error_chunk(
                    self.model,
                    openai_endpoint,
                    "The upstream inference request failed while streaming.",
                )
            finally:
                self._finalize_stream(streaming_state)
                if context is not None:
                    # Start accounting only once the stream ends, not while a
                    # valid long prefill/stream could exceed the logger's wait.
                    asyncio.create_task(
                        self._update_streaming_log(context, streaming_state)
                    )

        response = StreamingHttpResponse(
            streaming_content=sse_generator(), content_type="text/event-stream"
        )
        for key, value in create_streaming_response_headers().items():
            response[key] = value

        return SubmitStreamingTaskResponse(response=response, task_id=None)
