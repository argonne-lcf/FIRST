from functools import cached_property
from typing import TYPE_CHECKING, Any

from openai import OpenAI

if TYPE_CHECKING:
    from ..client import InferenceClient


class ClusterClient:
    def __init__(self, name: str, client: "InferenceClient") -> None:
        self.name = name
        self._client = client

    def __repr__(self) -> str:
        return f"ClusterClient(name={self.name})"

    def get_jobs(self) -> Any:
        resp = self._client.get(f"/{self.name}/jobs")
        resp.raise_for_status()
        return resp.json()

    @cached_property
    def openai(self) -> OpenAI:
        framework = "vllm" if self.name == "sophia" else "api"
        return OpenAI(
            api_key="unused",
            base_url=f"{self._client.base_url}{self.name}/{framework}/v1",
            http_client=self._client,
        )


class ClustersResource:
    def __init__(self, client: "InferenceClient") -> None:
        self._client = client
        self._handles: dict[str, ClusterClient] = {}

    def get(self, name: str) -> ClusterClient:
        return self._handles.setdefault(name, ClusterClient(name, self._client))
