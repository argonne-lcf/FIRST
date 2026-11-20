import asyncio
import hashlib
import json
import logging
import shlex
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Self, TypedDict

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
_RPC_TIMEOUT = 10
# How long one adapter call waits for its Compute task to finish.
_POLL_TIMEOUT = 10.0
_POLL_INTERVAL = 1.0
# Explanations for TaskPending.reason values worth calling out in errors.
_PENDING_HINTS: dict[str, str] = {
    "waiting-for-ep": (
        "no user endpoint has picked it up; check the endpoint's "
        "endpoint.log for the user endpoint this task was routed to"
    ),
}

# Read-only functions: an in-flight task may be reused by a later identical
# call, but only if it was still pending recently enough that its result is a
# current snapshot. Everything else mutates state on the cluster.
_READ_FUNCS: frozenset[str] = frozenset({"qstat", "list_files", "read_file"})
# A reused read's result is accepted only if we saw it still pending within
# this many seconds (so it executed no earlier than that).
_READ_FRESHNESS_SEC = 30.0
# Stop waiting on a read that has been pending this long and submit a new one
# (e.g. if Globus lost the task). Writes are never abandoned: Globus Compute
# cannot cancel tasks, so resubmitting a pending qsub risks a duplicate job.
_READ_ABANDON_SEC = 600.0


@dataclass
class _InFlight:
    task_id: str
    submitted_at: float  # time.monotonic()
    # Last time the web service reported this task pending (or when it was
    # submitted). Any result arriving later was computed after this instant.
    last_pending_at: float
    last_status: str


# Unfinished Compute tasks keyed by (endpoint, function, arguments), shared by
# every adapter instance in the process (adapters are rebuilt per reconcile).
# Best effort: this is lost on restart, and concurrent identical calls may
# still each submit a task.
_in_flight: dict[tuple[str, str, str], _InFlight] = {}


_warned_no_status_table = False


def _forget_task(client: Client, task_id: str) -> None:
    """
    Drop a task from the SDK's per-Client cache of task statuses and results.

    This is to work around a memory leak in globus_compute_sdk.Client.
    It keeps every completed task (result included) in the private
    `_task_status_table` dict forever, so a long-lived controller polling qstat
    leaks a snapshot per poll.
    """
    global _warned_no_status_table
    table = getattr(client, "_task_status_table", None)
    if isinstance(table, dict):
        table.pop(task_id, None)
    elif not _warned_no_status_table:
        _warned_no_status_table = True
        logger.warning(
            "globus_compute_sdk.Client has no dict _task_status_table "
            "(found %s); not pruning its task cache",
            type(table).__name__,
        )


def _call_key(
    endpoint_id: str, func: str, kwargs: dict[str, Any]
) -> tuple[str, str, str]:
    blob = json.dumps(kwargs, sort_keys=True, default=str).encode()
    return (endpoint_id, func, hashlib.sha256(blob).hexdigest())


class FuncRegistry(TypedDict):
    qsub: str
    qstat: str
    qdel: str
    list_files: str
    put_file: str
    read_file: str


FuncName = Literal["qsub", "qstat", "qdel", "list_files", "put_file", "read_file"]


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
        if client is None:
            raise RuntimeError(
                "GlobusComputePBSAdapter needs the Globus Compute client "
                "(FIRST_GLOBUS__COMPUTE_CLIENT_ID/SECRET), which only "
                "controller-manager is configured with"
            )

        if _func_registry is None:
            async with asyncio.timeout(_RPC_TIMEOUT):
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

    async def _submit(self, func: FuncName, kwargs: dict[str, Any]) -> str:
        async with asyncio.timeout(_RPC_TIMEOUT):
            task_id: str = await asyncio.to_thread(
                self.client.run,
                endpoint_id=self.endpoint_id,
                function_id=self.func_ids[func],
                **kwargs,
            )
        return task_id

    async def _call(
        self,
        func: FuncName,
        *,
        timeout: float | None = None,
        interval: float | None = None,
        **kwargs: Any,
    ) -> Any:
        """
        Run `func(**kwargs)` on the endpoint and wait up to `timeout` seconds.

        If an identical call from earlier is still unfinished, wait on that
        task instead of submitting another, so an unresponsive endpoint doesn't
        accumulate a backlog of tasks (or duplicate qsubs) that would all run
        once it recovers. A reused read is only accepted if it is fresh.
        """
        if timeout is None:
            timeout = _POLL_TIMEOUT
        if interval is None:
            interval = _POLL_INTERVAL
        is_read = func in _READ_FUNCS
        key = _call_key(self.endpoint_id, func, kwargs)
        deadline = time.monotonic() + timeout

        entry = _in_flight.get(key)
        if (
            entry is not None
            and is_read
            and time.monotonic() - entry.submitted_at > _READ_ABANDON_SEC
        ):
            logger.warning(
                "Abandoning Compute %s task %s after %.0fs (%s)",
                func,
                entry.task_id,
                time.monotonic() - entry.submitted_at,
                entry.last_status,
            )
            _in_flight.pop(key, None)
            _forget_task(self.client, entry.task_id)
            entry = None
        if entry is None:
            entry = await self._new_task(key, func, kwargs)
        else:
            logger.info(
                "Reusing unfinished Compute %s task %s (submitted %.0fs ago)",
                func,
                entry.task_id,
                time.monotonic() - entry.submitted_at,
            )

        while time.monotonic() < deadline:
            try:
                async with asyncio.timeout(_RPC_TIMEOUT):
                    result = await asyncio.to_thread(
                        self.client.get_result, entry.task_id
                    )
            except TaskPending as e:
                entry.last_pending_at = time.monotonic()
                entry.last_status = f"task status {e.reason!r}"
                if e.reason in _PENDING_HINTS:
                    entry.last_status += f" ({_PENDING_HINTS[e.reason]})"
                await asyncio.sleep(interval)
                continue
            except TimeoutError:
                entry.last_status = (
                    f"get_result call did not return within {_RPC_TIMEOUT}s"
                )
                continue
            except TaskExecutionFailed:
                _in_flight.pop(key, None)
                _forget_task(self.client, entry.task_id)
                raise

            _in_flight.pop(key, None)
            _forget_task(self.client, entry.task_id)
            stale_for = time.monotonic() - entry.last_pending_at
            if is_read and stale_for > _READ_FRESHNESS_SEC:
                # Could have executed any time in the last `stale_for` seconds.
                logger.info(
                    "Discarding possibly stale result of Compute %s task %s "
                    "(last seen pending %.0fs ago)",
                    func,
                    entry.task_id,
                    stale_for,
                )
                entry = await self._new_task(key, func, kwargs)
                continue
            return result

        age = time.monotonic() - entry.submitted_at
        retry_note = "" if is_read else "; will not resubmit until this task completes"
        raise TimeoutError(
            f"Compute {func} task {entry.task_id} on endpoint {self.endpoint_id} "
            f"unfinished after {age:.0f}s (waited {timeout:g}s this call); "
            f"last seen: {entry.last_status}{retry_note}"
        )

    async def _new_task(
        self, key: tuple[str, str, str], func: FuncName, kwargs: dict[str, Any]
    ) -> _InFlight:
        task_id = await self._submit(func, kwargs)
        now = time.monotonic()
        entry = _InFlight(
            task_id=task_id,
            submitted_at=now,
            last_pending_at=now,
            last_status="no response from Globus Compute",
        )
        _in_flight[key] = entry
        return entry

    async def submit_job(self, job: JobSubmitPayload) -> JobSubmitResult:
        if job.script_path is None:
            raise ValueError(
                "GlobusComputePBSAdapter.submit_job requires a script_path"
            )
        args = shlex.split(f"""
            -A {job.account} -q {job.queue} -N {job.name}
            -e {job.log_path} -o {job.log_path} -j oe
            -l select="{job.num_nodes}:ngpus={job.gpus_per_node}"
            -l "walltime=00:{job.walltime_min}:00"
            {job.scheduler_flags}
            {job.script_path}
        """)
        try:
            scheduler_id: str = await self._call("qsub", args=args)
        except TaskExecutionFailed as e:
            raise RuntimeError(f"GlobusCompute qsub failed:\n{e.remote_data}") from None
        scheduler_id = scheduler_id.strip()
        if not scheduler_id:
            raise RuntimeError("GlobusCompute qsub got an empty scheduler_id")
        return JobSubmitResult(job_name=job.name, scheduler_id=scheduler_id)

    async def get_job_statuses(self) -> list[JobStatusInfo]:
        try:
            raw: dict[str, Any] = await self._call("qstat")
        except TaskExecutionFailed as e:
            raise RuntimeError(
                f"GlobusCompute qstat failed:\n{e.remote_data}"
            ) from None
        return _parse_qstat(raw)

    async def get_exact_job_status(self, job_id: str) -> JobStatusInfo | None:
        try:
            raw: dict[str, Any] = await self._call("qstat", job_id=job_id)
        except TaskExecutionFailed as e:
            raise RuntimeError(
                f"GlobusCompute qstat failed:\n{e.remote_data}"
            ) from None
        jobs = _parse_qstat(raw)
        if jobs:
            job = jobs[0]
            if job.id.partition(".")[0] != job_id.strip().partition(".")[0]:
                raise AssertionError(
                    f"qstat queried {job_id=!r} but returned {job.id=!r}"
                )
            return job
        return None

    async def terminate_job(self, job_id: str) -> None:
        try:
            await self._call("qdel", job_id=job_id)
        except TaskExecutionFailed as e:
            raise RuntimeError(f"GlobusCompute qdel failed:\n{e.remote_data}") from None

    async def put_file(self, content: str, path: Path, mode: int) -> None:
        try:
            await self._call(
                "put_file", content=content, path=path.as_posix(), mode=mode
            )
        except TaskExecutionFailed as e:
            raise RuntimeError(
                f"GlobusCompute put_file failed:\n{e.remote_data}"
            ) from None

    async def list_files(self, directory: Path) -> list[str]:
        try:
            filenames: list[str] = await self._call(
                "list_files", directory=directory.as_posix()
            )
        except TaskExecutionFailed as e:
            raise RuntimeError(
                f"GlobusCompute list_files failed:\n{e.remote_data}"
            ) from None
        return filenames

    async def read_file(self, path: Path) -> str:
        try:
            content: str = await self._call("read_file", path=path.as_posix())
        except TaskExecutionFailed as e:
            raise RuntimeError(
                f"GlobusCompute read_file failed:\n{e.remote_data}"
            ) from None
        return content
