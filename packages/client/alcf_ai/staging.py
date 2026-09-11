"""
Data staging: move inputs into (and results out of) the per-user directory the
gateway provisions on its Globus data-staging collection.
"""

from pathlib import Path
from typing import TYPE_CHECKING

from first_common.schema.endpoints.staging import StagingArea

from ._http import raise_for_status
from .transfer import TransferResult, https_put_to_collection, run_globus_transfer

if TYPE_CHECKING:
    from .client import InferenceClient

# Where the staging collection is mounted, as seen from the clusters:
STAGING_COLLECTION_ROOT = "/eagle/IRIBeta/inference_data_staging/"


class StagingAPI:
    def __init__(self, client: "InferenceClient") -> None:
        self._client = client
        self._staging_area: StagingArea | None = None

    def ensure_staging_area(self) -> StagingArea:
        """Idempotently create (and cache) the caller's staging area."""
        if self._staging_area is None:
            resp = self._client.put("/data/v1/staging")
            raise_for_status(resp)
            self._staging_area = StagingArea.model_validate(resp.json())
        return self._staging_area

    @staticmethod
    def remote_path(result: TransferResult) -> str:
        """
        The filesystem path, as seen from the cluster, of data staged in by
        ``stage_in`` -- what task payloads such as SAM3's ``data_uri`` expect.
        """
        return STAGING_COLLECTION_ROOT + result.destination_path.lstrip("/")

    def stage_in(
        self,
        src: Path,
        dst: Path,
        *,
        from_collection_id: str | None = None,
        recursive: bool = False,
    ) -> TransferResult:
        """
        Copy ``src`` into the staging area at the relative path ``dst``.

        With ``from_collection_id`` the copy is a Globus Transfer from that
        collection (directories need ``recursive=True``); otherwise ``src`` is
        a local file uploaded over HTTPS.
        """
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
                recursive=recursive,
            )

        if recursive:
            raise ValueError(
                "Recursive (directory) stage-in requires a from_collection_id; "
                "the HTTPS upload path only supports single files."
            )
        src = src.expanduser().resolve()
        if not src.is_file():
            raise FileNotFoundError(f"{src} is not a local file")
        return https_put_to_collection(src, dst)

    def stage_out(
        self,
        to_collection_id: str,
        src: Path,
        dst: Path,
        *,
        recursive: bool = False,
    ) -> TransferResult:
        """
        Copy ``src`` (relative to the staging area) to ``dst`` on
        ``to_collection_id`` with Globus Transfer.
        """
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
            recursive=recursive,
        )
