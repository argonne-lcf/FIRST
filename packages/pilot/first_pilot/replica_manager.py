import logging
import os
import socket
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, wait
from enum import Enum
from typing import Literal

from cachetools.func import ttl_cache

from first_common.errors import BadPilotRequest, FirstError, NotFound
from first_common.schema.pilot import (
    GpuInfo,
    HostGpus,
    PilotResources,
    PilotRuntimeConfig,
    ReplicaStartRequest,
)
from first_common.schema.types import (
    GpuClaim,
)

from .replica import Replica

logger = logging.getLogger(__name__)

REPLICA_PORT_OFFSET = 2


class _ReservedSentinel(Enum):
    RESERVED = object()


ReservedSentinel = Literal[_ReservedSentinel.RESERVED]
_RESERVED = _ReservedSentinel.RESERVED


def _parse_gpu_row(line: str) -> GpuInfo | None:
    fields = [f.strip() for f in line.split(",")]
    if len(fields) != 4:
        logger.warning("unexpected nvidia-smi row %r; skipping", line)
        return None

    index, name, mem_total, mem_used = fields
    try:
        mem_total_mib = int(mem_total)
        mem_used_mib = int(mem_used)
    except ValueError:
        logger.warning("unparseable memory fields in nvidia-smi row %r", line)
        mem_total_mib = None
        mem_used_mib = None

    return GpuInfo(
        index=index,
        name=name,
        memory_total_mib=mem_total_mib,
        memory_used_mib=mem_used_mib,
    )


def query_gpus(hostname: str) -> HostGpus:
    try:
        result = subprocess.run(
            [
                "ssh",
                hostname,
                "nvidia-smi",
                "--query-gpu=index,name,memory.total,memory.used",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except subprocess.TimeoutExpired:
        logger.warning("nvidia-smi timed out after 5s; no GPUs discovered")
        return HostGpus(hostname=hostname, gpus=[])

    if result.returncode != 0:
        logger.warning(
            "nvidia-smi exited %d; no GPUs discovered (stderr: %s)",
            result.returncode,
            result.stderr.strip(),
        )
        return HostGpus(hostname=hostname, gpus=[])

    lines = result.stdout.strip().splitlines()

    gpus = [info for line in lines if line.strip() and (info := _parse_gpu_row(line))]
    return HostGpus(hostname=hostname, gpus=gpus)


def discover_hosts(node_file_env: str) -> list[str]:
    node_file = os.environ.get(node_file_env)
    localhost = socket.getfqdn()

    if not node_file:
        logger.info(
            "%s not set; assuming single-host deployment (%s)",
            node_file_env,
            localhost,
        )
        return [localhost]

    try:
        with open(node_file) as f:
            lines = f.readlines()
    except (FileNotFoundError, OSError) as exc:
        logger.warning(
            "node file %s=%s not read (%s); falling back to single host %s",
            node_file_env,
            node_file,
            exc,
            localhost,
        )
        return [localhost]

    hosts = [l.strip() for l in lines if l.strip()]
    if not hosts:
        logger.warning(
            "node file %s was empty; falling back to single host %s",
            node_file,
            localhost,
        )
        return [localhost]

    return hosts


class ReplicaManager:
    _STOP_JOIN_TIMEOUT = 45.0

    def __init__(self, config: PilotRuntimeConfig) -> None:
        self.config = config

        self.node_hostnames = discover_hosts(self.config.node_file_env)
        resources = self.query_resources()

        self._inventory = frozenset(
            (host.hostname, gpu.index) for host in resources.hosts for gpu in host.gpus
        )
        if not self._inventory:
            raise RuntimeError("no GPUs discovered; cannot start ReplicaManager")

        logger.info(
            f"discovered {len(self._inventory)} GPU(s) across {len(resources.hosts)} hosts"
        )

        # The lock serializes the small critical section in start_replica /
        # stop_replica that mutates these three structures together:
        #   self._replicas, self._claimed, self._used_ports
        self._lock = threading.Lock()
        self._replicas: dict[str, Replica | ReservedSentinel] = {}
        self._claimed: set[tuple[str, str]] = set()
        self._used_ports: set[int] = set()

    @ttl_cache(ttl=60)
    def query_resources(self) -> PilotResources:
        """Query nvidia-smi across all hosts in this pilot job"""
        with ThreadPoolExecutor(max_workers=8) as pool:
            host_gpus = list(pool.map(query_gpus, self.node_hostnames))

        return PilotResources(hosts=host_gpus)

    @staticmethod
    def _flatten(resources: list[GpuClaim]) -> list[tuple[str, str]]:
        return [
            (claim.hostname, gpu_id) for claim in resources for gpu_id in claim.gpu_ids
        ]

    def _validate_request(
        self, name: str, resources: list[GpuClaim]
    ) -> list[tuple[str, str]]:
        """
        Validate the parts of a start request that depend ONLY on immutable
        state (inventory + the request itself). Lock-free.
        """
        requested = self._flatten(resources)
        if not requested:
            raise BadPilotRequest("replica must request at least one GPU")

        if len(set(requested)) != len(requested):
            raise BadPilotRequest(
                f"duplicate GPU specified in replica {name!r} resources"
            )

        unknown = [r for r in requested if r not in self._inventory]
        if unknown:
            raise BadPilotRequest(f"requested GPUs not in pilot inventory: {unknown}")
        return requested

    def _allocate_port_locked(self) -> int:
        # Caller must hold self._lock.
        port = self.config.external_port + REPLICA_PORT_OFFSET
        while port in self._used_ports:
            port += 1
        self._used_ports.add(port)
        return port

    def _release_locked(self, name: str, resources: list[GpuClaim], port: int) -> None:
        # caller must hold self._lock
        self._replicas.pop(name, None)
        self._claimed.difference_update(self._flatten(resources))
        self._used_ports.discard(port)

    def start_replica(self, replica: ReplicaStartRequest) -> None:
        requested = self._validate_request(replica.name, replica.resources)

        # Short critical section: reserve name + GPUs + port atomically.
        with self._lock:
            if replica.name in self._replicas:
                raise BadPilotRequest(f"Replica {replica.name} is already registered")

            conflicting = [r for r in requested if r in self._claimed]
            if conflicting:
                raise BadPilotRequest(
                    f"requested GPUs are already claimed by another replica: "
                    f"{conflicting}"
                )

            self._claimed.update(requested)
            port = self._allocate_port_locked()
            # Insert a placeholder under the name so a racing start_replica
            # for the same name fails fast. We swap in the real Replica below.
            self._replicas[replica.name] = _RESERVED

        try:
            workdir = (
                self.config.replica_base_dir / replica.deployment_name / replica.name
            )
            workdir.mkdir(parents=True, exist_ok=True)

            r = Replica(
                name=replica.name,
                port=port,
                resources=replica.resources,
                launch_spec=replica.launch_spec,
                workdir=workdir,
            )
        except Exception as e:
            logger.exception(
                "failed to start replica %s; releasing reservation", replica.name
            )
            with self._lock:
                self._release_locked(replica.name, replica.resources, port)
            raise FirstError(f"Failed to start replica: {e}") from e

        with self._lock:
            self._replicas[replica.name] = r

    def stop_replica(self, replica_name: str) -> None:
        with self._lock:
            replica = self._replicas.get(replica_name)
            if replica is None or replica is _RESERVED:
                raise NotFound(f"Replica {replica_name!r} is not registered")
            # Claim ownership of teardown by removing the entry now: a concurrent
            # stop_replica/stop_all then sees it gone and won't call stop()
            # twice.
            del self._replicas[replica_name]

        logger.info("stopping replica %s", replica_name)
        replica.stop()

        with self._lock:
            self._release_locked(replica_name, replica.resources, replica.port)

    def stop_all(self) -> None:

        replicas = self.get_replicas()
        logger.info("stopping all %d replicas", len(replicas))

        with ThreadPoolExecutor() as pool:
            futs = [pool.submit(r.stop) for r in replicas]
            wait(futs, timeout=self._STOP_JOIN_TIMEOUT)
            pool.shutdown(wait=False, cancel_futures=True)

        with self._lock:
            for r in replicas:
                self._release_locked(r.name, r.resources, r.port)

    def get_replicas(self) -> list[Replica]:
        with self._lock:
            return [r for r in self._replicas.values() if r is not _RESERVED]

    def get_replica(self, name: str) -> Replica:
        with self._lock:
            replica = self._replicas.get(name)
            if replica is None or replica is _RESERVED:
                raise NotFound(f"Replica {name!r} is not registered")
        return replica
