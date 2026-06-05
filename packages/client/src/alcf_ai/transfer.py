import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from time import perf_counter

import globus_sdk
import httpx

from .auth import STAGING_COLLECTION_ID, get_https_authorizer, get_transfer_authorizer

logger = logging.getLogger(__name__)


class TransferError(Exception):
    """Transfer task failed."""


class TransferTimeout(TransferError):
    """Transfer task did not complete within the allowed time."""


@dataclass
class TransferResult:
    elapsed_seconds: float
    source_path: str
    destination_collection_id: str
    destination_path: str
    task_id: str | None = None
    bytes_transferred: int | None = None
    effective_gbps: float | None = None
    source_collection_id: str | None = None


def run_globus_transfer(
    source_collection_id: str,
    source_path: str,
    destination_collection_id: str,
    destination_path: str,
    *,
    timeout: int = 300,
    polling_interval: int = 5,
    verify_checksum: bool = False,
) -> TransferResult:
    """
    Submit a Globus Transfer task and block until it completes.
    """
    auth_collection_id = (
        source_collection_id
        if source_collection_id != STAGING_COLLECTION_ID
        else destination_collection_id
    )
    auth = get_transfer_authorizer(auth_collection_id)
    tc = globus_sdk.TransferClient(authorizer=auth)

    tdata = globus_sdk.TransferData(
        source_endpoint=source_collection_id,
        destination_endpoint=destination_collection_id,
        label=f"transfer {source_path}",
        verify_checksum=verify_checksum,
    )
    tdata.add_item(source_path, destination_path)

    submit_result = tc.submit_transfer(tdata)
    task_id = submit_result["task_id"]

    completed = tc.task_wait(
        task_id,
        timeout=timeout,
        polling_interval=max(1, polling_interval),
    )

    task = tc.get_task(task_id)
    status = task["status"]

    if not completed:
        raise TransferTimeout(f"Task {task_id} still {status} after {timeout}s")

    if status == "FAILED":
        fatal = task.get("fatal_error") or {}
        raise TransferError(
            f"Task {task_id} failed: "
            f"{fatal.get('description', task.get('nice_status', 'unknown error'))}"
        )

    if status != "SUCCEEDED":
        raise TransferError(f"Task {task_id} ended with unexpected status: {status}")

    bytes_transferred = task["bytes_transferred"]
    effective_bps = task.get("effective_bytes_per_second", 0)
    effective_gbps = effective_bps / 1e9

    request_time = datetime.fromisoformat(task["request_time"])
    completion_time = datetime.fromisoformat(task["completion_time"])
    elapsed = (completion_time - request_time).total_seconds()

    return TransferResult(
        task_id=task_id,
        bytes_transferred=bytes_transferred,
        elapsed_seconds=elapsed,
        effective_gbps=effective_gbps,
        source_collection_id=source_collection_id,
        source_path=source_path,
        destination_collection_id=destination_collection_id,
        destination_path=destination_path,
    )


def https_put_to_collection(local_path: Path, remote_path: Path) -> TransferResult:
    """
    HTTPS PUT a local file into the inference staging area.
    """
    transfer_auth = get_transfer_authorizer(f"{STAGING_COLLECTION_ID}:https")
    tc = globus_sdk.TransferClient(authorizer=transfer_auth)

    https_auth = get_https_authorizer(f"{STAGING_COLLECTION_ID}:https")

    endpoint = tc.get_endpoint(STAGING_COLLECTION_ID)
    https_server = endpoint["https_server"]
    headers = {"Authorization": str(https_auth.get_authorization_header())}

    local_path = Path(local_path).expanduser().resolve()
    assert local_path.is_file()

    start = perf_counter()
    with open(local_path, "rb") as f:
        r = httpx.put(
            f"{https_server}/{Path(remote_path).as_posix().lstrip('/')}",
            content=f,
            headers=headers,
            timeout=None,
        )
    r.raise_for_status()
    elapsed = perf_counter() - start
    return TransferResult(
        elapsed_seconds=elapsed,
        source_collection_id="local",
        source_path=local_path.as_posix(),
        destination_collection_id=STAGING_COLLECTION_ID,
        destination_path=remote_path.as_posix(),
    )
