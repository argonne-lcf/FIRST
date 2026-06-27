from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel

from .._http import raise_for_status
from ..transfer import TransferResult, https_put_to_collection, run_globus_transfer

if TYPE_CHECKING:
    from ..client import InferenceClient


class StagingAreaResponse(BaseModel):
    collection_id: str
    path: str


class StagingAPI:
    def __init__(self, client: "InferenceClient") -> None:
        self._client = client
        self._staging_area: StagingAreaResponse | None = None

    def ensure_staging_area(self) -> StagingAreaResponse:
        if self._staging_area is None:
            resp = self._client.put("data/staging")
            raise_for_status(resp)
            self._staging_area = StagingAreaResponse.model_validate(resp.json())
        return self._staging_area

    def stage_in(
        self, src: Path, dst: Path, *, from_collection_id: str | None = None
    ) -> TransferResult:
        staging = self.ensure_staging_area()

        src = Path(src)
        dst = Path(dst)
        if dst.is_absolute():
            raise ValueError(
                f"Destination path must be relative to staging area; got absolute path: {dst}"
            )
        dst = Path(staging.path) / dst

        if from_collection_id is not None:
            return run_globus_transfer(
                source_collection_id=from_collection_id,
                source_path=src.as_posix(),
                destination_collection_id=staging.collection_id,
                destination_path=dst.as_posix(),
            )
        else:
            src = Path(src).expanduser().resolve()
            assert src.is_file()
            return https_put_to_collection(src, dst)

    def stage_out(self, to_collection_id: str, src: Path, dst: Path) -> TransferResult:
        staging = self.ensure_staging_area()

        src = Path(src)
        dst = Path(dst)
        if src.is_absolute():
            raise ValueError(
                f"Source path must be relative to staging area; got absolute path: {src}"
            )
        src = Path(staging.path) / src

        return run_globus_transfer(
            source_collection_id=staging.collection_id,
            source_path=src.as_posix(),
            destination_collection_id=to_collection_id,
            destination_path=dst.as_posix(),
        )
