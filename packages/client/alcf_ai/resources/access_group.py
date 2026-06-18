from typing import TYPE_CHECKING

from first_common.schema.resources.read import AccessGroup

from .._http import raise_for_status

if TYPE_CHECKING:
    from ..client import InferenceClient


class AccessGroupsResource:
    def __init__(self, client: "InferenceClient") -> None:
        self._client = client

    def list(self) -> list[AccessGroup]:
        resp = self._client.get("/resources/access-groups")
        raise_for_status(resp)
        return [AccessGroup.model_validate(o) for o in resp.json()]
