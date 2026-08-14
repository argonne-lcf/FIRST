import logging
import os

from dotenv import load_dotenv
from httpx import Client, Timeout

from alcf_ai._http import raise_for_status
from alcf_ai.client import AutoGlobusAuth
from first_common.schema.resources import (
    ConfigVersion,
    ConfigVersionSummary,
    ResourceChangePlan,
    ResourceManifest,
)
from first_common.schema.resources.read import (
    PilotDeploymentSummary,
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
