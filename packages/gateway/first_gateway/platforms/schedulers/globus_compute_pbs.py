import asyncio
import json
import logging
import shlex
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Self, TypedDict

from globus_compute_sdk import Client
from globus_compute_sdk.errors import TaskPending

from first_common.schema.base_scheduler import (
    JobPhase,
    JobStatusInfo,
    JobSubmitPayload,
    JobSubmitResult,
    SchedulerAdapter,
)
from first_gateway.settings import ClientState

logger = logging.getLogger(__name__)

_STATE_MAP: dict[str, JobPhase] = {
    "B": JobPhase.starting,  # Job array has begun execution
    "E": JobPhase.exiting,  # Exiting / cleaning up post-execution
    "F": JobPhase.gone,  # Finished (completed, failed, or deleted)
    "H": JobPhase.queued,  # Held
    "M": JobPhase.gone,  # Moved to another server
    "Q": JobPhase.queued,  # Queued
    "R": JobPhase.running,  # Running
    "S": JobPhase.gone,  # Suspended
    "T": JobPhase.starting,  # Transiting (being routed/moved)
    "U": JobPhase.gone,  # User suspended
    "W": JobPhase.queued,  # Waiting (future Execution_Time)
    "X": JobPhase.gone,  # Expired (finished subjob)
}


class FuncRegistry(TypedDict):
    qsub: str
    qstat: str
    qdel: str
    list_files: str
    put_file: str
    read_file: str


def _qsub(args: list[str]) -> str:
    """
    Globus Compute Function to execute qsub and return Job ID from stdout
    """
    import subprocess

    p = subprocess.run(
        ["qsub", *args], text=True, check=True, capture_output=True, timeout=15
    )
    return p.stdout


def _qstat() -> dict[str, Any]:
    """
    Globus Compute Function to execute qstat and capture JSON Jobs output.
    """
    import subprocess

    p = subprocess.run(
        ["qstat", "-fF", "JSON"], text=True, check=True, capture_output=True, timeout=15
    )
    jobs: dict[str, Any] = json.loads(p.stdout)["Jobs"]
    assert isinstance(jobs, dict)
    return jobs


def _qdel(job_id: str) -> None:
    """Globus Compute function to qdel a job"""
    import subprocess

    subprocess.run(
        ["qdel", str(job_id)], text=True, check=True, capture_output=True, timeout=15
    )


def _list_files(directory: str) -> list[str]:
    """Globus Compute function to list files in a directory"""
    from pathlib import Path

    return [f.name for f in Path(directory).iterdir() if f.is_file()]


def _put_file(content: str, path: str, mode: int) -> None:
    """Globus Compute function to write file"""
    from pathlib import Path

    dest = Path(path)
    dest.parent.mkdir(exist_ok=True, parents=True)
    dest.touch(mode=mode)
    dest.chmod(mode)
    dest.write_text(content)


def _read_file(path: str) -> str:
    """Globus Compute function to read file content"""
    from pathlib import Path

    return Path(path).read_text()


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
        phase = _STATE_MAP.get(state_code)

        if phase is None:
            logger.warning(f"Unknown job_state code {state_code!r} for job {job_id!r}")
            phase = JobPhase.gone

        results.append(
            JobStatusInfo(
                id=job_id,
                name=attrs["Job_Name"],
                state=phase,
                created_at=_parse_utc_timestamp(attrs["ctime"]),
                started_at=_parse_utc_timestamp(attrs["stime"]),
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
        endpoint_id = config["endpoint_id"]
        client = deps.compute_client
        func_ids = FuncRegistry(
            qsub=await asyncio.to_thread(client.register_function, _qsub),
            qstat=await asyncio.to_thread(client.register_function, _qstat),
            qdel=await asyncio.to_thread(client.register_function, _qdel),
            list_files=await asyncio.to_thread(client.register_function, _list_files),
            put_file=await asyncio.to_thread(client.register_function, _put_file),
            read_file=await asyncio.to_thread(client.register_function, _read_file),
        )
        return cls(client, endpoint_id, func_ids)

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
        scheduler_id: str = await self._poll_for_result(task_id)
        return JobSubmitResult(job_name=job.name, scheduler_id=scheduler_id)

    async def get_job_statuses(self) -> list[JobStatusInfo]:
        task_id = await asyncio.to_thread(
            self.client.run,
            endpoint_id=self.endpoint_id,
            function_id=self.func_ids["qstat"],
        )
        raw: dict[str, Any] = await self._poll_for_result(task_id)
        return _parse_qstat(raw)

    async def terminate_job(self, job_id: str) -> None:
        task_id = await asyncio.to_thread(
            self.client.run,
            endpoint_id=self.endpoint_id,
            function_id=self.func_ids["qdel"],
            job_id=job_id,
        )
        await self._poll_for_result(task_id)

    async def put_file(self, content: str, path: Path, mode: int) -> None:
        task_id = await asyncio.to_thread(
            self.client.run,
            endpoint_id=self.endpoint_id,
            function_id=self.func_ids["put_file"],
            content=content,
            path=path.as_posix(),
            mode=mode,
        )
        await self._poll_for_result(task_id)

    async def list_files(self, directory: Path) -> list[str]:
        task_id = await asyncio.to_thread(
            self.client.run,
            endpoint_id=self.endpoint_id,
            function_id=self.func_ids["list_files"],
            directory=directory.as_posix(),
        )
        filenames: list[str] = await self._poll_for_result(task_id)
        return filenames

    async def read_file(self, path: Path) -> str:
        task_id = await asyncio.to_thread(
            self.client.run,
            endpoint_id=self.endpoint_id,
            function_id=self.func_ids["read_file"],
            path=path.as_posix(),
        )
        content: str = await self._poll_for_result(task_id)
        return content
