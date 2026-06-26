from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from first_gateway.settings import ClientState


@dataclass
class JobSubmitPayload:
    """
    Input to SchedulerAdapter.submit_job() [qsub].

    This is what goes into `qsub` at a generic level; there are no
    pilot-specific abstractions here.
    """

    name: str
    queue: str
    account: str
    scheduler_flags: str
    num_nodes: int
    gpus_per_node: int
    walltime_min: int
    script_path: Path
    log_path: Path


@dataclass
class JobSubmitResult:
    """
    Result of SchedulerAdapter.submit_job() identifiying the submitted job.
    """

    job_name: str
    scheduler_id: str


class JobPhase(str, Enum):
    """
    Normalized Job State, from the HPC scheduler's point of view
    """

    pending_submit = "pending_submit"
    queued = "queued"
    starting = "starting"
    running = "running"
    exiting = "exiting"
    gone = "gone"


@dataclass
class JobStatusInfo:
    """
    Result from SchedulerAdapter.get_job_statuses() [qstat].
    """

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


class SchedulerAdapter(ABC):
    """
    Abstract base class, enabling FIRST to interact with an HPC scheduler
    at a low level.

    Pilot-specific abstractions do not belong here; this is a thin adapter to
    qstat/qsub/qdel/filesystem that other code can rely on.
    """

    @classmethod
    @abstractmethod
    async def build(cls, client_state: "ClientState", config: dict[str, Any]) -> Self:
        """
        Receives ClusterSpec.pilot_system.scheduler_config and app-wide
        ClientState (for access to shared clients like GlobusCompute).

        Constructs and returns an instance of the SchedulerAdapter.
        """

    @abstractmethod
    async def submit_job(self, job_spec: JobSubmitPayload) -> JobSubmitResult:
        """
        Submit a job to the scheduler [qsub]
        """

    @abstractmethod
    async def get_job_statuses(self) -> list[JobStatusInfo]:
        """
        List job statuses from the scheduler [qstat]
        """

    @abstractmethod
    async def terminate_job(self, job_id: str) -> None:
        """
        Terminate a job in the scheduler [qdel]
        """

    @abstractmethod
    async def put_file(self, content: str, path: Path, mode: int) -> None:
        """
        Write a file at the designated path
        """

    @abstractmethod
    async def list_files(self, directory: Path) -> list[str]:
        """
        List files in a directory
        """

    @abstractmethod
    async def read_file(self, path: Path) -> str:
        """
        Read file at the designated path
        """
