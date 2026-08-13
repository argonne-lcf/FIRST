"""Isolated endpoint adapter for V2-managed (pilot) backends."""

import json
import logging
import random
import time
from typing import Any, AsyncGenerator

import httpx
from django.http import StreamingHttpResponse
from pydantic import BaseModel

from resource_server_async.endpoints.direct_api import DirectAPIEndpoint
from resource_server_async.endpoints.endpoint import BaseEndpoint
from resource_server_async.httpx_client import create_ssl_context
from resource_server_async.streaming import create_streaming_response_headers

from ..errors import EndpointError
from ..schemas.endpoints import (
    SubmitStreamingTaskResponse,
    SubmitTaskResult,
)

log = logging.getLogger(__name__)

# Bridge backends can serve long generations; match the DirectAPI default.
_REQUEST_TIMEOUT = 120


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
            timeout=_REQUEST_TIMEOUT,
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
        log.info(f"Making First V2 API call for model {self.model} (stream=True)")

        async def sse_generator() -> AsyncGenerator[str, None]:
            try:
                async with self._client.stream("POST", url, json=body) as response:
                    if response.status_code != 200:
                        error_text = await response.aread()
                        raise ValueError(
                            f"Upstream endpoint returned {response.status_code}: "
                            f"{error_text.decode(errors='replace').strip()[:256]}"
                        )
                    async for chunk in response.aiter_text():
                        if chunk:
                            yield chunk
            except Exception as e:
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
                                "content": f"\n\n[ERROR] {e}",
                            },
                            "finish_reason": "stop",
                        }
                    ],
                }
                yield f"data: {json.dumps(error_chunk)}\n\n"
                yield "data: [DONE]\n\n"

        response = StreamingHttpResponse(
            streaming_content=sse_generator(), content_type="text/event-stream"
        )
        for key, value in create_streaming_response_headers().items():
            response[key] = value

        return SubmitStreamingTaskResponse(response=response, task_id=None)
