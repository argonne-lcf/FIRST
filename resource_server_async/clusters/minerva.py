import logging
from typing import Any, List, override

from httpx import HTTPError, TimeoutException

from resource_server_async.cache import cache_item_async, get_item_from_cache_async
from resource_server_async.clusters.direct_api import DirectAPICluster

from ..errors import GetJobsError
from ..schemas.clusters import JobInfo, JobsByStatus
from ..schemas.structured_logs import UserPydantic

log = logging.getLogger(__name__)


class MinervaCluster(DirectAPICluster):
    """Minerva Direct API cluster backed by the login-node status endpoint."""

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

    @override
    async def get_jobs(self, _auth: UserPydantic | None) -> JobsByStatus:
        cache_key = f"{self.cluster_name}_status_response"

        cached_result: JobsByStatus | None = await get_item_from_cache_async(cache_key)
        if cached_result is not None:
            return cached_result

        status = await self._fetch_minerva_status()
        formatted = self._format_status(status)

        try:
            await cache_item_async(cache_key, formatted, ttl=60)
        except Exception as e:
            log.warning("Failed to cache %s: %s", cache_key, e)

        return formatted

    async def _fetch_minerva_status(self) -> Any:
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

    def _format_status(self, status: Any) -> JobsByStatus:
        if not isinstance(status, dict):
            raise GetJobsError("Unexpected response type from Minerva status URL")

        if isinstance(status.get("models"), list):
            model_entries: list[dict[str, Any]] = [
                {"status": "Live", "model": model}
                for model in status["models"]
                if isinstance(model, str)
            ]
        else:
            model_entries = []
            for model_info in status.values():
                if not isinstance(model_info, dict):
                    raise GetJobsError(
                        "Unexpected model entry type from Minerva status URL"
                    )
                model_entries.append(model_info)

        formatted = JobsByStatus()
        formatted.cluster_status = {
            "cluster": self.cluster_name,
            "total_models": len(model_entries),
            "live_models": 0,
            "stopped_models": 0,
        }

        for model_info in model_entries:
            raw_status = str(model_info.get("status", "Unknown"))
            model_name = str(model_info.get("model") or "").strip()
            if not model_name:
                continue

            model_version = str(model_info.get("model_version") or "")
            description = str(
                model_info.get("description")
                or self._build_description(model_name, model_version, model_info)
            )
            normalized_status = (
                "running" if raw_status.lower() == "live" else raw_status.lower()
            )

            job_entry = JobInfo(
                **{
                    "Models": model_name,
                    "Framework": "api",
                    "Cluster": self.cluster_name,
                    "Model Status": normalized_status,
                    "Description": description,
                    "Model Version": model_version,
                }
            )

            if raw_status.lower() == "live":
                formatted.running.append(job_entry)
                formatted.cluster_status["live_models"] += 1
            elif raw_status.lower() == "stopped":
                formatted.stopped.append(job_entry)
                formatted.cluster_status["stopped_models"] += 1
            else:
                formatted.queued.append(job_entry)

        return formatted

    def _build_description(
        self, model_name: str, model_version: str, model_info: dict[str, Any]
    ) -> str:
        route_prefix = model_info.get("route_prefix")
        pbs_job_id = model_info.get("pbs_job_id")
        node = model_info.get("node")

        parts = [model_version or model_name, "on Minerva"]
        safe_details = []
        if isinstance(route_prefix, str) and route_prefix.startswith("/models/"):
            safe_details.append(route_prefix)
        if isinstance(pbs_job_id, str):
            safe_details.append(f"job {pbs_job_id}")
        if isinstance(node, str):
            safe_details.append(node)
        if safe_details:
            parts.append(f"({', '.join(safe_details)})")
        return " ".join(parts)
