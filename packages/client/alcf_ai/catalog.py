"""
Catalog API: read-only discovery of the resources your AccessGroups grant, as
the gateway's public views (``*Public``).

Operators read the full resources from the admin API instead (`first-admin`).
"""

from typing import TYPE_CHECKING, Any

from first_common.errors import NotFound
from first_common.schema.resources.read import (
    AccessGroup,
    ClusterPublic,
    ModelPublic,
    PilotDeploymentPublic,
    PilotDeploymentPublicDetail,
    StaticDeploymentPublic,
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

    def models(self) -> list[ModelPublic]:
        """Visible Models with their deployments and live runtime counters."""
        return [ModelPublic.model_validate(o) for o in self._get("models")]

    def get_model(self, name: str) -> ModelPublic:
        """Look up one Model by name or alias."""
        for model in self.models():
            if name == model.name or name in model.aliases:
                return model
        raise NotFound(f"Model {name!r} not found (or not visible to you).")

    def access_groups(self) -> list[AccessGroup]:
        return [AccessGroup.model_validate(o) for o in self._get("access-groups")]

    # --- Deployments ----------------------------------------------------------

    def pilot_deployments(self) -> list[PilotDeploymentPublic]:
        return [
            PilotDeploymentPublic.model_validate(o)
            for o in self._get("deployments/pilot")
        ]

    def get_pilot_deployment(self, name: str) -> PilotDeploymentPublicDetail:
        """A PilotDeployment with the number of its live replicas in each state."""
        return PilotDeploymentPublicDetail.model_validate(
            self._get(f"deployments/pilot/{name}")
        )

    def static_deployments(self) -> list[StaticDeploymentPublic]:
        return [
            StaticDeploymentPublic.model_validate(o)
            for o in self._get("deployments/static")
        ]

    # --- Clusters -------------------------------------------------------------

    def clusters(self) -> list[ClusterPublic]:
        return [ClusterPublic.model_validate(o) for o in self._get("clusters")]
