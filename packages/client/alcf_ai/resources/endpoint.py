from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..client import InferenceClient


class EndpointsResource:
    def __init__(self, client: "InferenceClient") -> None:
        self._client = client

    def list(self) -> dict[str, Any]:
        resp = self._client.get("list-endpoints")
        resp.raise_for_status()
        result: dict[str, Any] = resp.json()
        return result
