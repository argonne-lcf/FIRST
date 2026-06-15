import os
from pathlib import Path
from typing import Any, Generator

from httpx import Auth, Client, Request, Response, Timeout
from pydantic import BaseModel

from .auth import get_inference_authorizer
from .resources import ClientResource, ClusterResource, Sam3Resource
from .transfer import TransferResult, https_put_to_collection, run_globus_transfer

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


class StagingAreaResponse(BaseModel):
    collection_id: str
    path: str


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
        self._resources: dict[str, ClientResource] = {}
        self._staging_area: StagingAreaResponse | None = None

    def __repr__(self) -> str:
        return f"InferenceClient({self.base_url})"

    def clusters(self, name: str) -> "ClusterResource":
        key = f"cluster:{name}"
        return self._resources.setdefault(key, ClusterResource(name, self))  # type: ignore[return-value]

    @property
    def sam3(self) -> "Sam3Resource":
        return self._resources.setdefault(  # type: ignore[return-value]
            "sam3", Sam3Resource("sophia/sam3service", self)
        )

    def list_endpoints(self) -> dict[str, Any]:
        resp = self.get("list-endpoints")
        resp.raise_for_status()
        result: dict[str, Any] = resp.json()
        return result

    def ensure_staging_area(self) -> StagingAreaResponse:
        resp = self.put("data/staging")
        resp.raise_for_status()
        return StagingAreaResponse.model_validate(resp.json())

    def stage_in(
        self, src: Path, dst: Path, *, from_collection_id: str | None = None
    ) -> TransferResult:
        if self._staging_area is None:
            self._staging_area = self.ensure_staging_area()

        src = Path(src)
        dst = Path(dst)
        if dst.is_absolute():
            raise ValueError(
                f"Destination path must be relative to staging area; got absolute path: {dst}"
            )
        dst = Path(self._staging_area.path) / dst

        if from_collection_id is not None:
            return run_globus_transfer(
                source_collection_id=from_collection_id,
                source_path=src.as_posix(),
                destination_collection_id=self._staging_area.collection_id,
                destination_path=dst.as_posix(),
            )
        else:
            src = Path(src).expanduser().resolve()
            assert src.is_file()
            return https_put_to_collection(src, dst)

    def stage_out(self, to_collection_id: str, src: Path, dst: Path) -> TransferResult:
        if self._staging_area is None:
            self._staging_area = self.ensure_staging_area()

        src = Path(src)
        dst = Path(dst)
        if src.is_absolute():
            raise ValueError(
                f"Source path must be relative to staging area; got absolute path: {src}"
            )
        src = Path(self._staging_area.path) / src

        return run_globus_transfer(
            source_collection_id=self._staging_area.collection_id,
            source_path=Path(src).as_posix(),
            destination_collection_id=to_collection_id,
            destination_path=Path(dst).as_posix(),
        )
