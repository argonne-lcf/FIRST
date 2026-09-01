"""Pull large request payloads out of the squashfs images in a dataset.

first-slogs bundles the requests whose payload outgrew the request_log
parquet into a squashfs image next to it, named
``<stem>.request_log.large_requests.squashfs``, with each payload stored at
``<id[0..2]>/<id[2..4]>/<id>.json``.
"""

import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import polars as pl


def _find_request_log(
    load_data: Callable[..., pl.LazyFrame], dataset_dir: str, request_id: str
) -> tuple[Path, str] | None:
    """Find the request_log parquet containing ``request_id``.

    Ids are matched case-insensitively; the returned spelling is the one
    stored in the parquet, which keys the request's file in the squashfs.
    """
    match = (
        load_data(dataset_dir, "request_log", include_file_paths="_source_file")
        .filter(pl.col("id").str.to_lowercase() == request_id.lower())
        .select("id", "_source_file")
        .collect(engine="streaming")
    )
    if match.height:
        return Path(str(match["_source_file"][0])), str(match["id"][0])
    return None


def pull_request(
    load_data: Callable[..., pl.LazyFrame], dataset_dir: str, request_id: str
) -> None:
    """Print the large request payload bundled for ``request_id``."""
    found = _find_request_log(load_data, dataset_dir, request_id)
    if found is None:
        raise SystemExit(f"error: request {request_id} is not in the dataset")
    parquet, request_id = found

    squashfs = parquet.with_suffix(".large_requests.squashfs")
    if not squashfs.is_file():
        raise SystemExit(
            f"error: request {request_id} has no large request payload; "
            f"nothing was bundled from {parquet.name}"
        )

    inner_path = f"{request_id[:2]}/{request_id[2:4]}/{request_id}.json"
    try:
        proc = subprocess.run(
            ["unsquashfs", "-cat", str(squashfs), inner_path],
            capture_output=True,
            check=False,
        )
    except FileNotFoundError:
        raise SystemExit(
            "error: unsquashfs not found (install squashfs-tools)",
        ) from None

    if proc.returncode != 0:
        raise SystemExit(f"error: request {request_id} is missing from {squashfs.name}")
    sys.stdout.buffer.write(proc.stdout)
    sys.stdout.buffer.flush()
