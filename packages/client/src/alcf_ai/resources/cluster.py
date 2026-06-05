from functools import cached_property
from typing import Any

from openai import OpenAI

from .resource import ClientResource


class ClusterResource(ClientResource):
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
