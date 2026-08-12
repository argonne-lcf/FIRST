import logging
from typing import Any, List, override

from asgiref.sync import sync_to_async

from resource_server_async.clusters.cluster import BaseCluster
from resource_server_async.first_v2_bridge.mapping import desired_endpoints
from resource_server_async.first_v2_bridge.router_config import (
    RouterConfig,
    get_bridge_redis_client,
)
from resource_server_async.first_v2_bridge.settings import BridgeSettings

from ..errors import GetJobsError
from ..schemas.clusters import JobInfo, JobsByStatus
from ..schemas.structured_logs import UserPydantic

log = logging.getLogger(__name__)


class TaraCluster(BaseCluster):
    """Cluster adapter backed by the V2 RouterConfig blob."""

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
        # config is accepted for fixture compatibility but unused: get_jobs
        # reads the RouterConfig blob directly.
        super().__init__(
            id,
            cluster_name,
            cluster_adapter,
            frameworks,
            openai_endpoints,
            allowed_globus_groups=allowed_globus_groups,
            allowed_domains=allowed_domains,
        )

    @override
    async def get_jobs(self, _auth: UserPydantic | None) -> JobsByStatus:
        return await sync_to_async(self._load_status)()

    def _load_status(self) -> JobsByStatus:
        settings = BridgeSettings()
        try:
            redis_client = get_bridge_redis_client(settings.redis_url)
            cfg = RouterConfig.load(redis_client)
        except Exception as e:
            raise GetJobsError(f"Failed to read Tara router config: {e}")

        desired = [
            d
            for d in desired_endpoints(cfg, settings)
            if d.cluster == self.cluster_name
        ]

        formatted = JobsByStatus()
        for d in desired:
            formatted.running.append(
                JobInfo(
                    **{
                        "Models": d.model,
                        "Framework": d.framework,
                        "Cluster": d.cluster,
                        "Model Status": "running",
                        "Description": f"{d.model} on {self.cluster_name}",
                        "Model Version": d.model,
                    }
                )
            )

        formatted.cluster_status = {
            "cluster": self.cluster_name,
            "total_models": len(desired),
            "live_models": len(desired),
            "stopped_models": 0,
        }
        return formatted
