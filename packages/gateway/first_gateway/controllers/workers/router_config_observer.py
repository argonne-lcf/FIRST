import logging

from first_common.schema.types import (
    HealthCheckResult,
    OverloadPolicy,
    ReplicaState,
    RouterParams,
    UsagePolicy,
)

from ...database import models as db
from ...database.redis.pubsub import Channel
from ...database.redis.router_config import (
    BackendConfig,
    DeploymentConfig,
    ModelConfig,
    RouterConfig,
)
from ..worker import Worker

logger = logging.getLogger(__name__)

_INCOMING = {ReplicaState.pending, ReplicaState.placed, ReplicaState.launching}


class RouterConfigObserver(Worker):
    """
    Rewrites RouterConfig periodically to inform the data plane of changes in available model backends.
    """

    poll_interval = 10.0
    wakeup_channels = [Channel.replica_started, Channel.replica_drain]

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

            await self.wait_for_wake()

    async def rebuild(self) -> list[ModelConfig]:
        async with self.client_state.db_sessionmaker() as sess:
            models = await db.Model.list(sess, load_pilot_replicas=True)
            notices = await db.Cluster.maintenance_notices(sess)

        return [
            ModelConfig(
                name=model.name,
                aliases=model.aliases,
                allowed_groups=model.access_group.allowed_groups,
                allowed_domains=model.access_group.allowed_domains,
                supported_endpoints=model.supported_endpoints,
                max_model_len=model.max_model_len,
                created_at=model.created_at,
                display_name=model.display_name,
                capabilities=model.capabilities,
                usage_limits=UsagePolicy.model_validate(model.usage_limits),
                overload=OverloadPolicy.model_validate(model.overload),
                deployments=self._build_deployments(
                    model.pilot_deployments, model.static_deployments, notices
                ),
            )
            for model in sorted(models, key=lambda m: m.uid)
        ]

    @staticmethod
    def _build_deployments(
        pilots: list[db.PilotDeployment],
        statics: list[db.StaticDeployment],
        maintenance_notices: dict[str, str],
    ) -> list[DeploymentConfig]:
        result = []

        dep: db.StaticDeployment | db.PilotDeployment
        for dep in sorted(statics, key=lambda d: d.uid):
            healthy = dep.health == HealthCheckResult.healthy
            notice = maintenance_notices.get(dep.cluster_name)
            if not (healthy or notice):
                continue
            backends = (
                [
                    BackendConfig(
                        id=dep.backend_id,
                        model_url=dep.api_url,
                        backend_model_name=dep.upstream_model_name,
                        api_key=dep.api_key,
                    )
                ]
                if healthy
                else []
            )
            result.append(
                DeploymentConfig(
                    kind="static",
                    name=dep.name,
                    cluster_name=dep.cluster_name,
                    router_params=RouterParams.model_validate(dep.router_params),
                    prometheus_metrics_path=dep.prometheus_metrics_path,
                    prometheus_scrape_interval_sec=dep.prometheus_scrape_interval_sec,
                    backends=backends,
                    maintenance_notice=notice,
                )
            )

        # Pilot deployments are always listed (possibly with no backends) so the
        # data plane can report a startup ETA when nothing is routable yet.
        for dep in sorted(pilots, key=lambda d: d.uid):
            notice = maintenance_notices.get(dep.cluster_name)
            retained = [r for r in dep.replicas if not r.is_draining]
            healthy_replicas = sorted(
                (r for r in retained if r.state == ReplicaState.ready),
                key=lambda r: r.uid,
            )
            # During maintenance, only a replica already launching on a live
            # pilot is starting; the rest wait for maintenance to end.
            incoming_states = {ReplicaState.launching} if notice else _INCOMING
            incoming = [r for r in retained if r.state in incoming_states]
            result.append(
                DeploymentConfig(
                    kind="pilot",
                    name=dep.name,
                    cluster_name=dep.cluster_name,
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
                    incoming=bool(incoming),
                    earliest_placed_at=min(
                        (r.placed_at for r in incoming if r.placed_at is not None),
                        default=None,
                    ),
                    last_startup_sec=dep.last_startup_sec,
                    maintenance_notice=notice,
                )
            )
        return result
