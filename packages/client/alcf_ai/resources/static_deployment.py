from typing import TYPE_CHECKING

from first_common.schema.resources.read import StaticDeployment

from .._http import raise_for_status

if TYPE_CHECKING:
    from ..client import InferenceClient


class StaticDeploymentsResource:
    def __init__(self, client: "InferenceClient") -> None:
        self._client = client

    def list(self) -> list[StaticDeployment]:
        resp = self._client.get("/resources/static-deployments")
        raise_for_status(resp)
        return [StaticDeployment.model_validate(o) for o in resp.json()]
