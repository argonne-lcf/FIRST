import os
import socket
import subprocess
import threading
from datetime import datetime

from pydantic import BaseModel

from first_common.errors import BadPilotRequest
from first_common.schema.pilot import ReplicaStartRequest
from first_common.schema.types import GpuClaim, HealthEndpointStatus, ReplicaPhase

from .config import Config


def discover_gpu_ids() -> list[str]:
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=index", "--format=csv,noheader,nounits"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            return []
        return [
            line.strip() for line in result.stdout.strip().splitlines() if line.strip()
        ]
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return []


def discover_hosts(node_file_env: str) -> list[str]:
    node_file = os.environ.get(node_file_env)
    localhost = socket.getfqdn()
    if not node_file:
        return [localhost]

    try:
        lines = open(node_file).readlines()
    except FileNotFoundError:
        return [localhost]

    hosts = [l.strip() for l in lines if l.strip()]
    return hosts if hosts else [localhost]


class Replica:
    """
    Handle to model replica subprocess and replica health monitor daemonic thread
    """

    ...


class ReplicaLogTail(BaseModel):
    stdout: str
    stderr: str
    logfile: str


class ReplicaStatus(BaseModel):
    phase: ReplicaPhase
    started_at: datetime
    health: HealthEndpointStatus


class ReplicaManager:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.replicas = {}
        self.lock = threading.Lock()

        gpu_ids = discover_gpu_ids()
        hosts = discover_hosts(self.config.node_file_env)
        self.resources = [
            GpuClaim(hostname=host, gpu_ids=gpu_ids.copy()) for host in hosts
        ]
        self.free_resources = set(
            (claim.hostname, gpu_id)
            for claim in self.resources
            for gpu_id in claim.gpu_ids
        )

    def start_replica(self, replica: ReplicaStartRequest) -> None:
        with self.lock:
            if replica.name in self.replicas:
                raise BadPilotRequest(f"Replica {replica.name} is already registered")

            # Validate that the requested resources are actually free

            # Assign the replica to the resources

            # Render the replica startup script

            # Prepare the replica workdir, log, stdout/stderr files
            # Launch the subprocess

            # Register the replica in "starting" state

            # Starts daemon thread that monitors health of replica:
            #  -> Poll subprocess return code: if exited, replica died
            #  -> Scrape /health endpoint of local replica
            #  -> Log error and terminate / clean up replica that fails to start within replica.launch_spec.max_startup_time

    def stop_replica(self, replica_name: str) -> None:
        with self.lock:
            if replica_name not in self.replicas:
                raise BadPilotRequest(f"Replica {replica_name!r} is not registered")

        # Kill replica subprocess, free resources, and de-register replica

    def get_status(self) -> list[ReplicaStatus]:
        ...
        # Get current replica status information

    def get_replica_logs(self) -> ReplicaLogTail:
        ...
        # Tail the log, stdout, and stderr of the replica
