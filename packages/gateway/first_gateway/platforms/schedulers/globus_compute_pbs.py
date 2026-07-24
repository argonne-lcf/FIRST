import asyncio
import logging
import shlex
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Self, TypedDict

from globus_compute_sdk import Client
from globus_compute_sdk.errors import TaskExecutionFailed, TaskPending

from first_common.schema.base_scheduler import (
    JobStatusInfo,
    JobSubmitPayload,
    JobSubmitResult,
    SchedulerAdapter,
    SchedulerJobState,
)
from first_gateway.settings import ClientState

from . import globus_compute_functions as fns

logger = logging.getLogger(__name__)

_STATE_MAP: dict[str, SchedulerJobState] = {
    "B": SchedulerJobState.starting,  # Job array has begun execution
    "E": SchedulerJobState.exiting,  # Exiting / cleaning up post-execution
    "F": SchedulerJobState.gone,  # Finished (completed, failed, or deleted)
    "H": SchedulerJobState.queued,  # Held
    "M": SchedulerJobState.gone,  # Moved to another server
    "Q": SchedulerJobState.queued,  # Queued
    "R": SchedulerJobState.running,  # Running
    "S": SchedulerJobState.gone,  # Suspended
    "T": SchedulerJobState.starting,  # Transiting (being routed/moved)
    "U": SchedulerJobState.gone,  # User suspended
    "W": SchedulerJobState.queued,  # Waiting (future Execution_Time)
    "X": SchedulerJobState.gone,  # Expired (finished subjob)
}


class FuncRegistry(TypedDict):
    qsub: str
    qstat: str
    qdel: str
    list_files: str
    put_file: str
    read_file: str


_func_registry: FuncRegistry | None = None


def _parse_utc_timestamp(raw: str) -> datetime:
    """
    Parse PBS datetime format: Mon Jun 22 18:24:00 2026
    Add explicit UTC timezone
    """
    dt = datetime.strptime(raw, "%a %b %d %H:%M:%S %Y")
    return dt.replace(tzinfo=timezone.utc)


def _parse_walltime_minutes(walltime_str: str) -> int:
    """
    Parse an HH:MM:SS walltime string into total minutes (integer).
    """
    parts = walltime_str.split(":")
    hours = int(parts[0])
    minutes = int(parts[1])
    seconds = int(parts[2])
    return hours * 60 + minutes + (1 if seconds > 0 else 0)


def _parse_qstat(jobs: dict[str, Any]) -> list[JobStatusInfo]:
    results: list[JobStatusInfo] = []

    for job_id, attrs in jobs.items():
        state_code = attrs["job_state"]
        state = _STATE_MAP.get(state_code)

        if state is None:
            logger.warning(f"Unknown job_state code {state_code!r} for job {job_id!r}")
            state = SchedulerJobState.gone

        results.append(
            JobStatusInfo(
                id=job_id.strip(),
                name=attrs["Job_Name"],
                state=state,
                created_at=_parse_utc_timestamp(attrs["ctime"]),
                started_at=_parse_utc_timestamp(attrs.get("stime"))
                if attrs.get("stime")
                else None,
                walltime_minutes=_parse_walltime_minutes(
                    attrs["Resource_List"]["walltime"]
                ),
            )
        )

    return results


class GlobusComputePBSAdapter(SchedulerAdapter):
    def __init__(
        self, client: Client, endpoint_id: str, func_ids: FuncRegistry
    ) -> None:
        self.client = client
        self.endpoint_id = endpoint_id
        self.func_ids = func_ids

    @classmethod
    async def build(cls, deps: ClientState, config: dict[str, Any]) -> Self:
        """
        Constructs wrapper with just-in-time function registration.

        Required config keys:
            endpoint_id: str — Globus Compute endpoint UUID for the target HPC system.
        """
        global _func_registry
        client = deps.compute_client

        if _func_registry is None:
            uuids = await asyncio.gather(
                asyncio.to_thread(client.register_function, fns.qsub),
                asyncio.to_thread(client.register_function, fns.qstat),
                asyncio.to_thread(client.register_function, fns.qdel),
                asyncio.to_thread(client.register_function, fns.list_files),
                asyncio.to_thread(client.register_function, fns.put_file),
                asyncio.to_thread(client.register_function, fns.read_file),
            )
            _func_registry = FuncRegistry(
                qsub=uuids[0],
                qstat=uuids[1],
                qdel=uuids[2],
                list_files=uuids[3],
                put_file=uuids[4],
                read_file=uuids[5],
            )

        return cls(client, config["endpoint_id"], _func_registry)

    async def _poll_for_result(
        self, task_id: str, *, timeout: int = 30, interval: float = 1.0
    ) -> Any:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                result = await asyncio.to_thread(self.client.get_result, task_id)
            except TaskPending:
                await asyncio.sleep(interval)
            else:
                return result
        raise TimeoutError(f"Timeout expired while waiting for Compute task {task_id}")

    async def submit_job(self, job: JobSubmitPayload) -> JobSubmitResult:
        args = shlex.split(f"""
            -A {job.account} -q {job.queue} -N {job.name}
            -e {job.log_path} -o {job.log_path} -j oe
            -l select="{job.num_nodes}:ngpus={job.gpus_per_node}"
            -l "walltime=00:{job.walltime_min}:00"
            {job.scheduler_flags}
            {job.script_path}
        """)
        task_id = await asyncio.to_thread(
            self.client.run,
            endpoint_id=self.endpoint_id,
            function_id=self.func_ids["qsub"],
            args=args,
        )
        try:
            scheduler_id: str = await self._poll_for_result(task_id)
        except TaskExecutionFailed as e:
            raise RuntimeError(f"GlobusCompute qsub failed:\n{e.remote_data}") from None
        scheduler_id = scheduler_id.strip()
        if not scheduler_id:
            raise RuntimeError("GlobusCompute qsub got an empty scheduler_id")
        return JobSubmitResult(job_name=job.name, scheduler_id=scheduler_id)

    async def get_job_statuses(self) -> list[JobStatusInfo]:
        task_id = await asyncio.to_thread(
            self.client.run,
            endpoint_id=self.endpoint_id,
            function_id=self.func_ids["qstat"],
        )
        try:
            raw: dict[str, Any] = await self._poll_for_result(task_id)
        except TaskExecutionFailed as e:
            raise RuntimeError(
                f"GlobusCompute qstat failed:\n{e.remote_data}"
            ) from None
        return _parse_qstat(raw)

    async def terminate_job(self, job_id: str) -> None:
        task_id = await asyncio.to_thread(
            self.client.run,
            endpoint_id=self.endpoint_id,
            function_id=self.func_ids["qdel"],
            job_id=job_id,
        )
        try:
            await self._poll_for_result(task_id)
        except TaskExecutionFailed as e:
            raise RuntimeError(f"GlobusCompute qdel failed:\n{e.remote_data}") from None

    async def put_file(self, content: str, path: Path, mode: int) -> None:
        task_id = await asyncio.to_thread(
            self.client.run,
            endpoint_id=self.endpoint_id,
            function_id=self.func_ids["put_file"],
            content=content,
            path=path.as_posix(),
            mode=mode,
        )
        try:
            await self._poll_for_result(task_id)
        except TaskExecutionFailed as e:
            raise RuntimeError(
                f"GlobusCompute put_file failed:\n{e.remote_data}"
            ) from None

    async def list_files(self, directory: Path) -> list[str]:
        task_id = await asyncio.to_thread(
            self.client.run,
            endpoint_id=self.endpoint_id,
            function_id=self.func_ids["list_files"],
            directory=directory.as_posix(),
        )
        try:
            filenames: list[str] = await self._poll_for_result(task_id)
        except TaskExecutionFailed as e:
            raise RuntimeError(
                f"GlobusCompute list_files failed:\n{e.remote_data}"
            ) from None
        return filenames

    async def read_file(self, path: Path) -> str:
        task_id = await asyncio.to_thread(
            self.client.run,
            endpoint_id=self.endpoint_id,
            function_id=self.func_ids["read_file"],
            path=path.as_posix(),
        )
        try:
            content: str = await self._poll_for_result(task_id)
        except TaskExecutionFailed as e:
            raise RuntimeError(
                f"GlobusCompute read_file failed:\n{e.remote_data}"
            ) from None
        return content
