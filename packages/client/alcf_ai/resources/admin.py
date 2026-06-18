import logging

from first_common.schema.resources import (
    ConfigVersion,
    ResourceChangePlan,
    ResourceManifest,
)

from .resource import ClientResource, raise_for_status

logger = logging.getLogger(__name__)


class AdminResource(ClientResource):
    def plan_resources(self, resources: list[ResourceManifest]) -> ResourceChangePlan:
        resp = self._client.post(
            "/resources/plan",
            json={"resources": [r.model_dump(mode="json") for r in resources]},
        )
        raise_for_status(resp)
        return ResourceChangePlan.model_validate(resp.json())

    def apply_resources(
        self, resources: list[ResourceManifest], approved_plan: ResourceChangePlan
    ) -> ConfigVersion | None:
        resp = self._client.post(
            "/resources/apply",
            json={
                "resources": [r.model_dump(mode="json") for r in resources],
                "approved_plan": approved_plan.model_dump(mode="json"),
            },
        )
        raise_for_status(resp)
        return ConfigVersion.model_validate(resp.json()) if resp.json() else None
