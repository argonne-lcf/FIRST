import asyncio
import logging

from first_common.schema.types import (
    HealthCheckResult,
    OverloadPolicy,
    ReplicaState,
    RouterParams,
    UsagePolicy,
)

from ..database import models as db
from ..database.redis.router_config import (
    BackendConfig,
    DeploymentConfig,
    ModelConfig,
    RouterConfig,
)
from .worker import Worker

logger = logging.getLogger(__name__)


class RouterConfigObserver(Worker):
    """
    Rewrites RouterConfig periodically to inform the data plane of changes in available model backends.
    """

    poll_interval: float = 10.0

    async def run(self) -> None:
        hb = self.register_heartbeat("poll")

        current_config = await RouterConfig.load(self.client_state.redis)

        while True:
            hb.beat()
            new_models = await self.rebuild()
            if current_config.models != new_models:
                logger.info("Detected RouterConfig change; publishing new version")
                current_config.models = new_models
                await current_config.publish(self.client_state.redis)

            await asyncio.sleep(self.poll_interval)

    async def rebuild(self) -> list[ModelConfig]:
        async with self.client_state.db_sessionmaker() as sess:
            models = await db.Model.list(sess, load_pilot_replicas=True)

        return [
            ModelConfig(
                name=model.name,
                aliases=model.aliases,
                allowed_groups=model.access_group.allowed_groups,
                allowed_domains=model.access_group.allowed_domains,
                supported_endpoints=model.supported_endpoints,
                usage_limits=UsagePolicy.model_validate(model.usage_limits),
                overload=OverloadPolicy.model_validate(model.overload),
                deployments=self._build_deployments(
                    model.pilot_deployments, model.static_deployments
                ),
            )
            for model in sorted(models, key=lambda m: m.uid)
        ]

    @staticmethod
    def _build_deployments(
        pilots: list[db.PilotDeployment], statics: list[db.StaticDeployment]
    ) -> list[DeploymentConfig]:
        result = []

        dep: db.StaticDeployment | db.PilotDeployment
        for dep in sorted(statics, key=lambda d: d.uid):
            if dep.health == HealthCheckResult.healthy:
                result.append(
                    DeploymentConfig(
                        kind="static",
                        name=dep.name,
                        router_params=RouterParams.model_validate(dep.router_params),
                        prometheus_metrics_path=dep.prometheus_metrics_path,
                        prometheus_scrape_interval_sec=dep.prometheus_scrape_interval_sec,
                        backends=[
                            BackendConfig(
                                id=dep.backend_id,
                                model_url=dep.api_url,
                                backend_model_name=dep.upstream_model_name,
                                api_key=dep.api_key,
                            )
                        ],
                    )
                )

        for dep in sorted(pilots, key=lambda d: d.uid):
            healthy_replicas = sorted(
                (r for r in dep.replicas if r.state == ReplicaState.ready.value),
                key=lambda r: r.uid,
            )
            if healthy_replicas:
                result.append(
                    DeploymentConfig(
                        kind="pilot",
                        name=dep.name,
                        router_params=RouterParams.model_validate(dep.router_params),
                        prometheus_metrics_path=dep.prometheus_metrics_path,
                        prometheus_scrape_interval_sec=dep.prometheus_scrape_interval_sec,
                        backends=[
                            BackendConfig(
                                id=rep.backend_id,
                                model_url=str(rep.model_url),
                                backend_model_name=str(rep.observed_served_name),
                                api_key=None,
                            )
                            for rep in healthy_replicas
                        ],
                    )
                )
        return result
