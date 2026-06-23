from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path


@dataclass
class JobSubmitPayload:
    name: str
    queue: str
    account: str
    scheduler_flags: str
    num_nodes: int
    gpus_per_node: int
    walltime_min: int
    script_path: Path
    stdout_path: Path
    stderr_path: Path


@dataclass
class JobSubmitResult:
    job_name: str
    scheduler_id: str


class JobPhase(str, Enum):
    """
    Job State, from the HPC scheduler's point of view
    """

    pending_submit = "pending_submit"
    queued = "queued"
    starting = "starting"
    running = "running"
    exiting = "exiting"
    gone = "gone"


@dataclass
class JobStatusInfo:
    id: str
    name: str
    state: JobPhase
    created_at: datetime
    started_at: datetime
    walltime_minutes: int

    @property
    def deadline(self) -> datetime:
        """The time at which the job's walltime allocation expires."""
        return self.started_at + timedelta(minutes=self.walltime_minutes)


class SchedulerInterface(ABC):
    @abstractmethod
    async def submit_job(self, job_spec: JobSubmitPayload) -> JobSubmitResult: ...

    @abstractmethod
    async def get_job_statuses(self) -> list[JobStatusInfo]: ...

    @abstractmethod
    async def terminate_job(self, job_id: str) -> None: ...

    @abstractmethod
    async def put_file(self, content: str, path: Path, mode: int) -> None: ...

    @abstractmethod
    async def list_files(self, directory: Path) -> list[str]: ...

    @abstractmethod
    async def read_file(self, path: Path) -> str: ...
