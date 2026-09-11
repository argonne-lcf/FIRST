"""
Catalog API: read-only discovery of the gateway's declarative resources.

Ordinary users see the resources their AccessGroups grant; admins see all.
"""

from typing import TYPE_CHECKING, Any

from first_common.errors import NotFound
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

from ._http import raise_for_status

if TYPE_CHECKING:
    from .client import InferenceClient

PREFIX = "/catalog/v1"


class CatalogAPI:
    def __init__(self, client: "InferenceClient") -> None:
        self._client = client

    def _get(self, path: str) -> Any:
        resp = self._client.get(f"{PREFIX}/{path}")
        raise_for_status(resp)
        return resp.json()

    # --- Models ---------------------------------------------------------------

    def models(self) -> list[ModelSummary]:
        """Visible Models with their deployments and live runtime counters."""
        return [ModelSummary.model_validate(o) for o in self._get("models")]

    def get_model(self, name: str) -> ModelSummary:
        """Look up one Model by name or alias."""
        for model in self.models():
            if name == model.name or name in model.aliases:
                return model
        raise NotFound(f"Model {name!r} not found (or not visible to you).")

    def access_groups(self) -> list[AccessGroup]:
        return [AccessGroup.model_validate(o) for o in self._get("access-groups")]

    # --- Deployments ----------------------------------------------------------

    def pilot_deployments(self) -> list[PilotDeploymentSummary]:
        return [
            PilotDeploymentSummary.model_validate(o)
            for o in self._get("deployments/pilot")
        ]

    def get_pilot_deployment(self, name: str) -> PilotDeploymentDetail:
        """A PilotDeployment with its replicas and their live runtime counters."""
        return PilotDeploymentDetail.model_validate(
            self._get(f"deployments/pilot/{name}")
        )

    def static_deployments(self) -> list[StaticDeploymentDetail]:
        return [
            StaticDeploymentDetail.model_validate(o)
            for o in self._get("deployments/static")
        ]

    # --- Clusters -------------------------------------------------------------

    def clusters(self) -> list[ClusterSummary]:
        return [ClusterSummary.model_validate(o) for o in self._get("clusters")]

    def get_cluster(self, name: str) -> ClusterDetail:
        """A Cluster with its pilot jobs.  Admin-only."""
        return ClusterDetail.model_validate(self._get(f"clusters/{name}"))

    # --- Admin-only operational reads ----------------------------------------

    def launch_templates(self) -> list[LaunchTemplate]:
        """Admin-only."""
        return [LaunchTemplate.model_validate(o) for o in self._get("launch-templates")]

    def system_health(self) -> SystemHealth:
        """Admin-only one-glance health of every operational resource."""
        return SystemHealth.model_validate(self._get("system-health"))

    def replica_logs(self, replica_slug: str) -> str:
        """Tail of a PilotReplica's log.  Admin-only."""
        logs: str = self._get(f"pilot-replicas/{replica_slug}/logs")
        return logs
