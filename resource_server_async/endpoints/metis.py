import logging
from typing import Any

from resource_server_async.clusters.metis import MetisCluster
from resource_server_async.endpoints.direct_api import DirectAPIEndpoint

from ..errors import EndpointError
from ..schemas.endpoints import (
    SubmitStreamingTaskResponse,
    SubmitTaskResult,
)

log = logging.getLogger(__name__)


# Metis endpoint implementation of a DirectAPIEndpoint
class MetisEndpoint(DirectAPIEndpoint):
    """Metis endpoint implementation of DirectAPIEndpoint."""

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
        # Initialize the rest of the common attributes
        # Also pass config since it is using DirectAPIEndpoint to manage API calls
        super().__init__(
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

    # Check endpoint status
    async def check_endpoint_status(self) -> bool:
        """Return endpoint status or an error is the endpoint cannot receive requests."""

        # Get Metis cluster wrapper from database
        cluster = await MetisCluster.load_adapter("metis")

        # Get Metis cluster status
        metis_status = await cluster.get_jobs(None)

        # Extract list of running models
        model_list = []
        for running in metis_status.running:
            models = running.Models
            if isinstance(models, str):
                model_list.extend([model.strip() for model in models.split(",")])
            else:
                model_list.extend(models)  # type: ignore[unreachable]

        # Error if model not available
        if self.model not in model_list:
            raise EndpointError(
                f"{self.model!r} is not currently live on Metis.", status_code=503
            )

        # Return that the model is available
        return True

    # Submit task
    async def submit_task(self, data: dict[str, Any]) -> SubmitTaskResult:
        """Submits a single interactive task to the compute resource."""

        await self.check_endpoint_status()

        api_request_data = self._prepare_request_body(data, stream=False)

        # Log model and Metis endpoint ID
        log.info(f"Making Metis API call for model {self.model} (stream=False)")

        return await self._submit_task_with_headers(api_request_data)

    # Submit streaming task
    async def submit_streaming_task(
        self, data: dict[str, Any]
    ) -> SubmitStreamingTaskResponse:
        """Submits a single interactive task to the compute resource with streaming enabled."""

        # Check endpoint status
        await self.check_endpoint_status()

        api_request_data = self._prepare_request_body(data, stream=True)

        # Log model and Metis endpoint ID
        log.info(f"Making Metis API call for model {self.model} (stream=True)")

        return await self._submit_streaming_task_with_headers(api_request_data)
