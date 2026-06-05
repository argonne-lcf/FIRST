import logging
from typing import Any

from asgiref.sync import sync_to_async
from django.core.cache import cache
from django.utils.text import slugify
from pydantic import BaseModel

from resource_server_async import globus_utils
from resource_server_async.clusters.cluster import BaseCluster

from ..errors import EndpointError, GetJobsError
from ..models import Endpoint
from ..schemas.clusters import JobsByStatus
from ..schemas.structured_logs import UserPydantic

log = logging.getLogger(__name__)


# Custom configuration for Globus Compute Cluster
class ClusterConfig(BaseModel):
    qstat_endpoint_uuid: str
    qstat_function_uuid: str


# Globus Compute implementation of a BaseCluster
class GlobusComputeCluster(BaseCluster):
    """Globus Compute implementation of BaseCluster."""

    # Class initialization
    def __init__(
        self,
        id: str,
        cluster_name: str,
        cluster_adapter: str,
        frameworks: list[str],
        openai_endpoints: list[str],
        config: dict[str, Any],
        allowed_globus_groups: list[str] = [],
        allowed_domains: list[str] = [],
    ):
        # Validate endpoint configuration
        self.__config = ClusterConfig(**config)

        # Initialize the rest of the common attributes
        super().__init__(
            id,
            cluster_name,
            cluster_adapter,
            frameworks,
            openai_endpoints,
            allowed_globus_groups,
            allowed_domains,
        )

    # Get jobs
    async def get_jobs(self, auth: UserPydantic) -> JobsByStatus:
        """Provides a status of the cluster as a whole, including which models are running."""

        # Redis cache key
        cache_key = f"qstat_details:{auth.username}:{auth.id}:{self.cluster_name}"

        # Try to get qstat details from Redis
        cached_result: JobsByStatus | None = cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        # Get Globus Compute client and executor
        try:
            gcc = globus_utils.get_compute_client_from_globus_app()
            gce = globus_utils.get_compute_executor(client=gcc)
        except Exception as e:
            raise GetJobsError(str(e))

        # Build temporary qstat endpoint slug
        endpoint_slug = f"{self.cluster_name}/jobs"

        # Get the status of the qstat endpoint
        # NOTE: Do not await here, cache the "first" request to avoid too-many-requests Globus error
        endpoint_status, error_message = globus_utils.get_endpoint_status(
            endpoint_uuid=self.config.qstat_endpoint_uuid,
            client=gcc,
            endpoint_slug=endpoint_slug,
        )
        if error_message:
            raise EndpointError(error_message)

        # Return error message if endpoint is not online
        if not (endpoint_status and endpoint_status.get("status") == "online"):
            raise EndpointError(f"Error: Endpoint {endpoint_slug} is offline.")

        # Submit task and wait for result
        task_result = await globus_utils.submit_and_get_result(
            gce,
            self.config.qstat_endpoint_uuid,
            self.config.qstat_function_uuid,
            timeout=60,
        )
        result = task_result.result

        # Try to refine the status of each endpoint (in case Globus Compute managers are lost)
        try:
            # For each running endpoint ...
            for i, running in enumerate(result["running"]):
                # If the model is in a "running" state (not "starting")
                if running["Model Status"] == "running":
                    # Get compute endpoint ID from database
                    running_framework = running["Framework"]
                    running_model = running["Models"].split(",")[0]
                    running_cluster = running["Cluster"]
                    endpoint_slug = slugify(
                        " ".join([running_cluster, running_framework, running_model])
                    )
                    endpoint = await sync_to_async(Endpoint.objects.get)(
                        endpoint_slug=endpoint_slug
                    )
                    endpoint_config = globus_utils.unwrap_json(endpoint.config)
                    endpoint_uuid = endpoint_config["endpoint_uuid"]

                    # Turn the model to "disconnected" if managers are lost
                    endpoint_status, error_message = globus_utils.get_endpoint_status(
                        endpoint_uuid=endpoint_uuid,
                        client=gcc,
                        endpoint_slug=endpoint_slug,
                    )
                    if (
                        not endpoint_status
                        or int(endpoint_status["details"].get("managers", 0)) == 0
                    ):
                        result["running"][i]["Model Status"] = "disconnected"

        except Exception as e:
            log.warning(f"Failed to refine qstat model status: {e}")

        # Convert dashes into underscores
        result["private_batch_running"] = result["private-batch-running"]
        result["private_batch_queued"] = result["private-batch-queued"]

        # Build response
        response = JobsByStatus(**result)

        # Cache the result for 60 seconds
        cache.set(cache_key, response, 60)

        # Return qstat result
        return response

    # Read-only access to the configuration
    @property
    def config(self) -> ClusterConfig:
        return self.__config
