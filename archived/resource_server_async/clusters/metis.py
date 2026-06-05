# Tool to log access requests
import logging
from typing import Any, List, override

from django.core.cache import cache
from httpx import HTTPError, TimeoutException

from resource_server_async.clusters.direct_api import DirectAPICluster

from ..errors import GetJobsError
from ..schemas.clusters import JobInfo, JobsByStatus
from ..schemas.structured_logs import UserPydantic

log = logging.getLogger(__name__)


# Metis implementation of a BaseCluster
class MetisCluster(DirectAPICluster):
    """Metis implementation of BaseCluster."""

    # Class initialization
    def __init__(
        self,
        id: str,
        cluster_name: str,
        cluster_adapter: str,
        frameworks: List[str],
        openai_endpoints: List[str],
        config: dict[str, Any],
        allowed_globus_groups: List[str] = [],
        allowed_domains: List[str] = [],
    ):
        # Initialize the rest of the common attributes
        super().__init__(
            id,
            cluster_name,
            cluster_adapter,
            frameworks,
            openai_endpoints,
            config=config,
            allowed_globus_groups=allowed_globus_groups,
            allowed_domains=allowed_domains,
        )

    # Get formatted cluster status
    @override
    async def get_jobs(self, _auth: UserPydantic | None) -> JobsByStatus:
        """Fetch and return cluster status. Can be overwritten to format output."""

        # Redis cache key
        cache_key = "metis_status_response"

        cached_result: JobsByStatus | None = cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        metis_status = await self._fetch_metis_status()
        if not isinstance(metis_status, dict):
            raise GetJobsError("Unexpected response type from Metis status URL")

        # Declare data structure
        formatted = JobsByStatus()
        formatted.cluster_status = {
            "cluster": "metis",
            "total_models": len(metis_status),
            "live_models": 0,
            "stopped_models": 0,
        }

        # For each model in the Metis cluster status
        for model_info in metis_status.values():
            if not isinstance(model_info, dict):
                raise GetJobsError("Unexpected response type from Metis status URL")

            status = model_info.get("status", "Unknown")

            # Extract model name and description
            model_name = model_info.get("model", "")
            description = model_info.get("description", "")
            full_description = f"{model_name} - {description}"

            # Do not expose sensitive fields like model_key, endpoint_id, or url to users
            # Format consistently with Sophia/Polaris jobs output
            job_entry = JobInfo(
                **{
                    "Models": model_name,
                    "Framework": "api",
                    "Cluster": "metis",
                    "Model Status": "running" if status == "Live" else status.lower(),
                    "Description": full_description,
                    "Model Version": model_info.get("model_version", ""),
                }
            )

            if status == "Live":
                formatted.running.append(job_entry)
                formatted.cluster_status["live_models"] += 1
            elif status == "Stopped":
                formatted.stopped.append(job_entry)
                formatted.cluster_status["stopped_models"] += 1
            else:
                # Any other status goes to queued
                formatted.queued.append(job_entry)

        # Cache the result for 60 seconds
        try:
            cache.set(cache_key, formatted, 60)
        except Exception as e:
            log.warning(f"Failed to cache metis_status_response: {e}")

        # Return jobs result
        return formatted

    async def _fetch_metis_status(self) -> Any:
        """Get the raw status data."""
        try:
            return await self.httpx_client.get(self.config.status_url)
        except TimeoutException:
            raise GetJobsError(
                f"Timeout calling {self.config.status_url!r}", status_code=504
            )
        except HTTPError as e:
            raise GetJobsError(
                f"Unexpected error calling {self.config.status_url!r}: {e}"
            )
