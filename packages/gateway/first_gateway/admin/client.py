import logging
import os
from typing import Any

from dotenv import load_dotenv
from httpx import Client, Timeout

from alcf_ai._http import raise_for_status
from alcf_ai.client import AutoGlobusAuth
from first_common.errors import NotFound
from first_common.schema.resources import (
    ConfigVersion,
    ConfigVersionSummary,
    ResourceChangePlan,
    ResourceManifest,
)
from first_common.schema.resources.read import (
    AccessGroup,
    ClusterDetail,
    ClusterSummary,
    LaunchTemplate,
    ModelSummary,
    PilotDeploymentDetail,
    PilotDeploymentSummary,
    StaticDeploymentDetail,
    SystemHealth,
)

logger = logging.getLogger(__name__)
load_dotenv()
DEFAULT_BASE_URL = os.environ.get("admin_base_url", "http://localhost:9100")


class AdminClient(Client):
    def __init__(self, base_url: str | None = None) -> None:
        if base_url is None:
            base_url = DEFAULT_BASE_URL

        super().__init__(
            auth=AutoGlobusAuth(),
            base_url=base_url,
            timeout=Timeout(10.0),
        )

    def plan(self, resources: list[ResourceManifest]) -> ResourceChangePlan:
        resp = self.post(
            "/control/v1/plan",
            json={"resources": [r.model_dump(mode="json") for r in resources]},
        )
        raise_for_status(resp)
        return ResourceChangePlan.model_validate(resp.json())

    def apply(
        self, resources: list[ResourceManifest], approved_plan: ResourceChangePlan
    ) -> ConfigVersion | None:
        resp = self.post(
            "/control/v1/apply",
            json={
                "resources": [r.model_dump(mode="json") for r in resources],
                "approved_plan": approved_plan.model_dump(mode="json"),
            },
        )
        raise_for_status(resp)
        return ConfigVersion.model_validate(resp.json()) if resp.json() else None

    def list_config_versions(self) -> list[ConfigVersionSummary]:
        resp = self.get("/control/v1/config-versions")
        raise_for_status(resp)
        return [ConfigVersionSummary.model_validate(v) for v in resp.json()]

    def get_config_version(self, uid: int) -> ConfigVersion:
        resp = self.get(f"/control/v1/config-versions/{uid}")
        raise_for_status(resp)
        return ConfigVersion.model_validate(resp.json())

    def reconcile_reset(self, resource: str) -> None:
        resp = self.post(
            "/control/v1/reconcile-reset",
            json={"resource": resource},
        )
        raise_for_status(resp)

    def set_desired_pilot_deployment_replicas(
        self, name: str, num_replicas: int
    ) -> PilotDeploymentSummary:
        resp = self.put(
            f"/control/v1/deployments/pilot/{name}/desired-replicas",
            json={"num_replicas": num_replicas},
        )
        raise_for_status(resp)
        return PilotDeploymentSummary.model_validate(resp.json())

    # --- Admin catalog: every resource, in full ------------------------------

    def _catalog(self, path: str) -> Any:
        resp = self.get(f"/catalog/v1/{path}")
        raise_for_status(resp)
        return resp.json()

    def models(self) -> list[ModelSummary]:
        """Every Model with its deployments and live runtime counters."""
        return [ModelSummary.model_validate(o) for o in self._catalog("models")]

    def get_model(self, name: str) -> ModelSummary:
        """Look up one Model by name or alias."""
        for model in self.models():
            if name == model.name or name in model.aliases:
                return model
        raise NotFound(f"Model {name!r} not found.")

    def access_groups(self) -> list[AccessGroup]:
        return [AccessGroup.model_validate(o) for o in self._catalog("access-groups")]

    def launch_templates(self) -> list[LaunchTemplate]:
        return [
            LaunchTemplate.model_validate(o) for o in self._catalog("launch-templates")
        ]

    def pilot_deployments(self) -> list[PilotDeploymentSummary]:
        return [
            PilotDeploymentSummary.model_validate(o)
            for o in self._catalog("deployments/pilot")
        ]

    def get_pilot_deployment(self, name: str) -> PilotDeploymentDetail:
        """A PilotDeployment with its replicas and their live runtime counters."""
        return PilotDeploymentDetail.model_validate(
            self._catalog(f"deployments/pilot/{name}")
        )

    def static_deployments(self) -> list[StaticDeploymentDetail]:
        return [
            StaticDeploymentDetail.model_validate(o)
            for o in self._catalog("deployments/static")
        ]

    def clusters(self) -> list[ClusterSummary]:
        return [ClusterSummary.model_validate(o) for o in self._catalog("clusters")]

    def get_cluster(self, name: str) -> ClusterDetail:
        """A Cluster with its pilot jobs."""
        return ClusterDetail.model_validate(self._catalog(f"clusters/{name}"))

    def system_health(self) -> SystemHealth:
        """One-glance health of every operational resource."""
        return SystemHealth.model_validate(self._catalog("system-health"))

    def replica_logs(self, replica_slug: str) -> str:
        """Tail of a live PilotReplica's log."""
        logs: str = self._catalog(f"pilot-replicas/{replica_slug}/logs")
        return logs
