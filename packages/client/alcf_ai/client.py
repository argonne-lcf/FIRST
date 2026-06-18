import os
from typing import Generator

from httpx import Auth, Client, Request, Response, Timeout

from .api import AdminAPI, Sam3API, StagingAPI
from .auth import get_inference_authorizer
from .resources import ClustersResource, EndpointsResource

DEFAULT_BASE_URL = os.environ.get(
    "inference_base_url", "https://inference-api.alcf.anl.gov/resource_server/"
)


class AutoGlobusAuth(Auth):
    def auth_flow(self, request: Request) -> Generator[Request, Response, None]:
        auth = get_inference_authorizer()
        auth.ensure_valid_token()  # type: ignore[attr-defined]
        assert auth.access_token, "Empty access token"  # type: ignore[attr-defined]

        request.headers["Authorization"] = f"Bearer {auth.access_token}"  # type: ignore[attr-defined]
        yield request


class InferenceClient(Client):
    def __init__(
        self,
        base_url: str | None = None,
        timeout: Timeout = Timeout(10.0, read=30.0),
    ) -> None:
        if base_url is None:
            base_url = DEFAULT_BASE_URL

        super().__init__(
            auth=AutoGlobusAuth(),
            base_url=base_url,
            timeout=timeout,
        )
        self.admin = AdminAPI(self)
        self.sam3 = Sam3API(self)
        self.staging = StagingAPI(self)
        self.clusters = ClustersResource(self)
        self.endpoints = EndpointsResource(self)

    def __repr__(self) -> str:
        return f"InferenceClient({self.base_url})"
