import logging
from typing import TYPE_CHECKING

from first_common.schema.resources import (
    ConfigVersion,
    ResourceChangePlan,
    ResourceManifest,
)

from .._http import raise_for_status

if TYPE_CHECKING:
    from ..client import InferenceClient

logger = logging.getLogger(__name__)


class AdminAPI:
    def __init__(self, client: "InferenceClient") -> None:
        self._client = client

    def plan(self, resources: list[ResourceManifest]) -> ResourceChangePlan:
        resp = self._client.post(
            "/resources/plan",
            json={"resources": [r.model_dump(mode="json") for r in resources]},
        )
        raise_for_status(resp)
        return ResourceChangePlan.model_validate(resp.json())

    def apply(
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
