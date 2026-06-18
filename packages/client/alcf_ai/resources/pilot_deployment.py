from typing import TYPE_CHECKING

from first_common.schema.resources.read import (
    PilotDeploymentDetail,
    PilotDeploymentSummary,
)

from .._http import raise_for_status

if TYPE_CHECKING:
    from ..client import InferenceClient


class PilotDeploymentsResource:
    def __init__(self, client: "InferenceClient") -> None:
        self._client = client

    def list(self) -> list[PilotDeploymentSummary]:
        resp = self._client.get("/resources/pilot-deployments")
        raise_for_status(resp)
        return [PilotDeploymentSummary.model_validate(o) for o in resp.json()]

    def get(self, name: str) -> PilotDeploymentDetail:
        resp = self._client.get(f"/resources/pilot-deployments/{name}")
        raise_for_status(resp)
        return PilotDeploymentDetail.model_validate(resp.json())
