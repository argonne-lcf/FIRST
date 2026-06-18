from typing import TYPE_CHECKING

from first_common.schema.resources.read import ModelSummary

from .._http import raise_for_status

if TYPE_CHECKING:
    from ..client import InferenceClient


class ModelsResource:
    def __init__(self, client: "InferenceClient") -> None:
        self._client = client

    def list(self) -> list[ModelSummary]:
        resp = self._client.get("/resources/models")
        raise_for_status(resp)
        return [ModelSummary.model_validate(o) for o in resp.json()]
