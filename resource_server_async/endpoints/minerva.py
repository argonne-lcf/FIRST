import logging
from typing import Any

from resource_server_async.clusters.minerva import MinervaCluster
from resource_server_async.endpoints.direct_api import DirectAPIEndpoint

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
        api_request_data = {**data["model_params"]}
        api_request_data["stream"] = False
        api_request_data.pop("api_port", None)
        log.info(
            f"Making Minerva Direct API call for model {self.model} (stream=False)"
        )
        return await super().submit_task(api_request_data)

    async def submit_streaming_task(
        self, data: dict[str, Any]
    ) -> SubmitStreamingTaskResponse:
        await self.check_endpoint_status()
        api_request_data = {**data["model_params"]}
        api_request_data["stream"] = True
        api_request_data.pop("api_port", None)
        log.info(f"Making Minerva Direct API call for model {self.model} (stream=True)")
        return await super().submit_streaming_task(api_request_data)
