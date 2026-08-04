import logging
from typing import Any

from resource_server_async.clusters.minerva import MinervaCluster
from resource_server_async.endpoints.direct_api import DirectAPIEndpoint
from resource_server_async.logging import get_request_context
from resource_server_async.minerva_affinity import (
    CACHE_SALT_ENDPOINTS,
    MinervaAffinityConfigurationError,
    derive_minerva_request_values,
)

from ..errors import EndpointError
from ..schemas.endpoints import (
    SubmitStreamingTaskResponse,
    SubmitTaskResult,
)

log = logging.getLogger(__name__)


class MinervaEndpoint(DirectAPIEndpoint):
    """Minerva Direct API endpoint backed by login-node NGINX routes."""

    async def check_endpoint_status(self) -> bool:
        cluster = await MinervaCluster.load_adapter("minerva")
        jobs = await cluster.get_jobs(None)
        live_models: list[str] = []
        for running in jobs.running:
            models = running.Models
            if isinstance(models, str):
                live_models.extend([model.strip() for model in models.split(",")])
            else:
                live_models.extend(models)  # type: ignore[unreachable]

        if self.model not in live_models:
            raise EndpointError(
                f"{self.model!r} is not currently live on Minerva.", status_code=503
            )
        return True

    async def submit_task(self, data: dict[str, Any]) -> SubmitTaskResult:
        await self.check_endpoint_status()
        api_request_data, request_headers = self._prepare_request(data, stream=False)
        log.info(
            f"Making Minerva Direct API call for model {self.model} (stream=False)"
        )
        return await self._submit_task_with_headers(
            api_request_data, request_headers=request_headers
        )

    async def submit_streaming_task(
        self, data: dict[str, Any]
    ) -> SubmitStreamingTaskResponse:
        await self.check_endpoint_status()
        api_request_data, request_headers = self._prepare_request(data, stream=True)
        log.info(f"Making Minerva Direct API call for model {self.model} (stream=True)")
        return await self._submit_streaming_task_with_headers(
            api_request_data, request_headers=request_headers
        )

    def _prepare_request(
        self,
        data: dict[str, Any],
        *,
        stream: bool,
    ) -> tuple[dict[str, Any], dict[str, str]]:
        api_request_data = {**data["model_params"]}
        api_request_data["stream"] = stream
        api_request_data.pop("api_port", None)
        endpoint = str(
            api_request_data.get("openai_endpoint", "chat/completions")
        ).strip("/")

        try:
            request_headers, cache_salt = derive_minerva_request_values(
                get_request_context(), self.model
            )
        except MinervaAffinityConfigurationError as exc:
            raise EndpointError(
                f"Minerva affinity configuration error: {exc}", status_code=500
            ) from exc

        if endpoint in CACHE_SALT_ENDPOINTS:
            # A caller value, if its public request schema ever accepts one,
            # cannot replace the server-derived namespace.
            api_request_data["cache_salt"] = cache_salt
        else:
            api_request_data.pop("cache_salt", None)
        return api_request_data, request_headers
