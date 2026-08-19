import logging
import os
import re
import socket
import subprocess
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, wait
from enum import Enum
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal

from cachetools.func import ttl_cache

from first_common.errors import (
    BadPilotRequest,
    NotFound,
    ReplicaAlreadyPlaced,
    ReplicaStartError,
)
from first_common.schema.pilot import (
    GpuInfo,
    HostGpus,
    PilotResources,
    PilotRuntimeConfig,
    ReplicaStartRequest,
)
from first_common.schema.types import (
    GpuClaim,
    PalsDiscovery,
    SSHDiscovery,
)

from .replica import Replica

logger = logging.getLogger(__name__)


class _ReservedSentinel(Enum):
    RESERVED = object()


ReservedSentinel = Literal[_ReservedSentinel.RESERVED]
_RESERVED = _ReservedSentinel.RESERVED


_NVIDIA_SMI_ARGS = [
    "--query-gpu=index,name,memory.total,memory.used",
    "--format=csv,noheader,nounits",
]
_PALS_LABEL = re.compile(r"^(?P<host>\S+)\s+(?P<rank>\d+):\s?(?P<row>.*)$")


def _require_executable(path: Path, *, label: str) -> str:
    """Return a configured executable path or fail with a useful error."""
    if not path.is_file() or not os.access(path, os.X_OK):
        raise RuntimeError(f"required {label} is unavailable: {path}")
    return str(path)


def _parse_gpu_row(line: str) -> GpuInfo:
    fields = [field.strip() for field in line.split(",")]
    if len(fields) != 4:
        raise ValueError(f"unexpected nvidia-smi row: {line!r}")

    index, name, mem_total, mem_used = fields
    if not index.isdecimal() or not name:
        raise ValueError(f"invalid GPU index or name in nvidia-smi row: {line!r}")
    try:
        mem_total_mib = int(mem_total)
        mem_used_mib = int(mem_used)
    except ValueError as exc:
        raise ValueError(f"invalid GPU memory in nvidia-smi row: {line!r}") from exc
    if mem_total_mib <= 0 or not 0 <= mem_used_mib <= mem_total_mib:
        raise ValueError(f"out-of-range GPU memory in nvidia-smi row: {line!r}")

    return GpuInfo(
        index=index,
        name=name,
        memory_total_mib=mem_total_mib,
        memory_used_mib=mem_used_mib,
    )


def _validated_host_gpus(
    hostname: str, gpus: list[GpuInfo], expected_gpus: int
) -> HostGpus:
    indices = [gpu.index for gpu in gpus]
    if len(indices) != len(set(indices)):
        raise RuntimeError(f"GPU inventory duplicated an index on host {hostname!r}")

    expected_indices = {str(index) for index in range(expected_gpus)}
    if set(indices) != expected_indices:
        raise RuntimeError(
            f"GPU inventory for host {hostname!r} reported indices "
            f"{sorted(indices)!r}; expected {sorted(expected_indices)!r}"
        )
    return HostGpus(
        hostname=hostname,
        gpus=sorted(gpus, key=lambda gpu: int(gpu.index)),
    )


def _query_gpus_command(
    hostname: str,
    command: list[str],
    expected_gpus: int,
    timeout_sec: float,
    *,
    label: str,
) -> HostGpus:
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"{label} GPU inventory timed out after {timeout_sec:g}s"
        ) from exc

    if result.returncode != 0:
        diagnostics = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(
            f"{label} GPU inventory exited {result.returncode}: {diagnostics[-2000:]}"
        )

    try:
        gpus = [_parse_gpu_row(line) for line in result.stdout.splitlines() if line]
    except ValueError as exc:
        raise RuntimeError(f"{label} GPU inventory was malformed: {exc}") from exc
    return _validated_host_gpus(hostname, gpus, expected_gpus)


def query_gpus_local(
    hostname: str, expected_gpus: int, timeout_sec: float = 5.0
) -> HostGpus:
    """Inventory a single-node pilot locally; never use a remote shell."""
    return _query_gpus_command(
        hostname,
        ["nvidia-smi", *_NVIDIA_SMI_ARGS],
        expected_gpus,
        timeout_sec,
        label="local",
    )


def query_gpus_ssh(
    hostnames: list[str], expected_gpus: int, timeout_sec: float = 5.0
) -> list[HostGpus]:
    """Query every scheduler host concurrently and retain scheduler order."""

    def query_host(hostname: str) -> HostGpus:
        return _query_gpus_command(
            hostname,
            ["ssh", hostname, "nvidia-smi", *_NVIDIA_SMI_ARGS],
            expected_gpus,
            timeout_sec,
            label=f"SSH host {hostname!r}",
        )

    with ThreadPoolExecutor(max_workers=len(hostnames)) as pool:
        return list(pool.map(query_host, hostnames))


def _normalized_hostname(hostname: str) -> str:
    return hostname.strip().rstrip(".").lower()


def _host_identity(hostname: str) -> str:
    return _normalized_hostname(hostname).split(".", maxsplit=1)[0]


def _host_matches(label: str, expected: str) -> bool:
    label_identity = _host_identity(label)
    expected_identity = _host_identity(expected)
    return bool(label_identity) and label_identity == expected_identity


def _deduplicate_hosts(hostnames: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for hostname in hostnames:
        identity = _host_identity(hostname)
        if not identity:
            raise RuntimeError("scheduler node inventory contains an empty hostname")
        if identity not in seen:
            seen.add(identity)
            result.append(hostname)
    return result


def query_gpus_pals(
    hostnames: list[str],
    launcher_path: Path,
    expected_gpus: int,
    timeout_sec: float = 35.0,
) -> list[HostGpus]:
    """Run one labeled inventory rank per host through the site PALS launcher."""
    nvidia_smi = "nvidia-smi"
    pals = _require_executable(launcher_path, label="PALS launcher")

    command = [
        pals,
        "--pmi=pmix",
        "--genvnone",
        "--no-transfer",
        "--line-buffer",
        "--label",
        "--abort-on-failure",
        "--timeout",
        # Leave time for the launcher to report and exit before our outer
        # subprocess deadline expires.
        str(max(1, int(timeout_sec) - 5)),
        "-n",
        str(len(hostnames)),
        "--ppn",
        "1",
        "--cpu-bind=none",
        nvidia_smi,
        *_NVIDIA_SMI_ARGS,
    ]
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"PALS GPU inventory timed out after {timeout_sec:g}s"
        ) from exc

    if result.returncode != 0:
        diagnostics = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(
            f"PALS GPU inventory exited {result.returncode}: {diagnostics[-2000:]}"
        )

    gpus_by_rank: dict[int, list[GpuInfo]] = {
        rank: [] for rank in range(len(hostnames))
    }
    seen_ranks: set[int] = set()
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        match = _PALS_LABEL.fullmatch(line)
        if match is None:
            raise RuntimeError(
                f"PALS GPU inventory returned an unlabeled row: {line!r}"
            )

        rank = int(match.group("rank"))
        if rank not in gpus_by_rank:
            raise RuntimeError(f"PALS GPU inventory returned unexpected rank {rank}")
        expected_host = hostnames[rank]
        reported_host = match.group("host")
        if not _host_matches(reported_host, expected_host):
            raise RuntimeError(
                f"PALS GPU inventory rank {rank} reported host {reported_host!r}; "
                f"expected {expected_host!r}"
            )
        try:
            gpu = _parse_gpu_row(match.group("row"))
        except ValueError as exc:
            raise RuntimeError(f"PALS GPU inventory was malformed: {exc}") from exc
        gpus_by_rank[rank].append(gpu)
        seen_ranks.add(rank)

    expected_ranks = set(range(len(hostnames)))
    if seen_ranks != expected_ranks:
        missing = sorted(expected_ranks - seen_ranks)
        raise RuntimeError(f"PALS GPU inventory is incomplete; missing ranks {missing}")

    return [
        _validated_host_gpus(hostname, gpus_by_rank[rank], expected_gpus)
        for rank, hostname in enumerate(hostnames)
    ]


def discover_hosts(node_file_env: str) -> list[str]:
    node_file = os.environ.get(node_file_env)

    if not node_file:
        localhost = socket.gethostname()
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
        localhost = socket.gethostname()
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
        localhost = socket.gethostname()
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

        # PBS nodefiles may repeat a hostname once per assigned resource.  Keep
        # the scheduler's first-occurrence order because it is also the PALS
        # rank order used by gpus_by_host in the replica launch context.
        self.node_hostnames = _deduplicate_hosts(
            discover_hosts(self.config.node_file_env)
        )
        if len(self.node_hostnames) != self.config.num_nodes:
            raise RuntimeError(
                "pilot node inventory differs from its scheduler request: "
                f"discovered {len(self.node_hostnames)}, "
                f"expected {self.config.num_nodes}"
            )
        resources = self.query_resources()

        self._inventory = resources.hosts
        if not any(host.gpus for host in self._inventory):
            raise RuntimeError("no GPUs discovered; cannot start ReplicaManager")

        gpu_count = sum(len(host.gpus) for host in self._inventory)
        logger.info(
            "discovered %d GPU(s) across %d hosts", gpu_count, len(resources.hosts)
        )

        # Private directory (0700 by default) that holds every replica's Unix
        # domain socket.
        self._socket_dir = TemporaryDirectory(prefix="first-pilot-uds-")
        logger.info("replica sockets will live under %s", self._socket_dir.name)

        # The lock serializes the small critical section in start_replica /
        # stop_replica that mutates these structures together:
        #   self._replicas, self._claimed, self._next_socket_id
        self._lock = threading.Lock()
        self._replicas: dict[str, Replica | ReservedSentinel] = {}
        self._claimed: set[tuple[str, str]] = set()
        # Monotonic counter used to mint short, unique socket filenames
        self._next_socket_id = 0

    @ttl_cache(ttl=60)
    def query_resources(self) -> PilotResources:
        """Discover and validate the exact scheduler-requested inventory."""
        discovery = self.config.gpu_discovery
        if len(self.node_hostnames) == 1:
            host_gpus = [
                query_gpus_local(
                    self.node_hostnames[0],
                    self.config.gpus_per_node,
                )
            ]
        elif isinstance(discovery, SSHDiscovery):
            host_gpus = query_gpus_ssh(
                self.node_hostnames,
                self.config.gpus_per_node,
                discovery.timeout_sec,
            )
        elif isinstance(discovery, PalsDiscovery):
            host_gpus = query_gpus_pals(
                self.node_hostnames,
                discovery.launcher_path,
                self.config.gpus_per_node,
                discovery.timeout_sec,
            )
        else:
            raise AssertionError(f"unsupported GPU discovery method: {discovery!r}")

        return PilotResources(hosts=host_gpus)

    @staticmethod
    def _flatten(resources: list[GpuClaim]) -> list[tuple[str, str]]:
        return [
            (claim.hostname, gpu_id) for claim in resources for gpu_id in claim.gpu_ids
        ]

    def _validate_request(
        self, name: str, gpu_indices: list[tuple[int, int]]
    ) -> list[tuple[str, str]]:
        """
        Validate the parts of a start request that depend ONLY on immutable
        state (inventory + the request itself). Lock-free.
        """
        if not gpu_indices:
            raise BadPilotRequest("replica must request at least one GPU")

        if len(set(gpu_indices)) != len(gpu_indices):
            raise BadPilotRequest(
                f"duplicate GPU specified in replica {name!r} resources"
            )

        requested: list[tuple[str, str]] = []

        for host_idx, gpu_idx in gpu_indices:
            if (
                host_idx < 0
                or gpu_idx < 0
                or host_idx >= len(self._inventory)
                or gpu_idx >= len(self._inventory[host_idx].gpus)
            ):
                raise BadPilotRequest(
                    f"requested {host_idx=} {gpu_idx=} outside GPUs pilot inventory"
                )

            hostname = self._inventory[host_idx].hostname
            gpu_id = self._inventory[host_idx].gpus[gpu_idx].index
            requested.append((hostname, gpu_id))

        return requested

    def _allocate_uds_locked(self) -> str:
        # Caller must hold self._lock.
        socket_id = self._next_socket_id
        self._next_socket_id += 1
        return str(Path(self._socket_dir.name) / f"replica-{socket_id}.sock")

    def _release_locked(self, name: str, resources: list[GpuClaim]) -> None:
        # caller must hold self._lock
        self._replicas.pop(name, None)
        self._claimed.difference_update(self._flatten(resources))

    def start_replica(self, replica: ReplicaStartRequest) -> None:
        requested = self._validate_request(replica.name, replica.gpu_indices)

        host_gpus: dict[str, list[str]] = defaultdict(list)
        for hostname, gpu_id in requested:
            host_gpus[hostname].append(gpu_id)

        resources = [
            GpuClaim(hostname=hostname, gpu_ids=gpu_list)
            for hostname, gpu_list in host_gpus.items()
        ]

        # Short critical section: reserve name + GPUs + port atomically.
        with self._lock:
            if replica.name in self._replicas:
                raise ReplicaAlreadyPlaced(
                    f"Replica {replica.name} is already registered"
                )

            conflicting = [r for r in requested if r in self._claimed]
            if conflicting:
                raise BadPilotRequest(
                    f"requested GPUs are already claimed by another replica: "
                    f"{conflicting}"
                )

            self._claimed.update(requested)
            uds = self._allocate_uds_locked()
            # Insert a placeholder under the name so a racing start_replica
            # for the same name fails fast. We swap in the real Replica below.
            self._replicas[replica.name] = _RESERVED

        try:
            workdir = self.config.replica_base_dir / replica.name
            workdir.mkdir(parents=True, exist_ok=True)

            r = Replica(
                name=replica.name,
                uds=uds,
                resources=resources,
                launch_spec=replica.launch_spec,
                workdir=workdir,
            )
        except Exception as e:
            logger.exception(
                "failed to start replica %s; releasing reservation", replica.name
            )
            with self._lock:
                self._release_locked(replica.name, resources)
            raise ReplicaStartError(f"Failed to start replica: {e}") from e

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
            self._release_locked(replica_name, replica.resources)

    def stop_all(self) -> None:

        replicas = self.get_replicas()
        logger.info("stopping all %d replicas", len(replicas))

        with ThreadPoolExecutor() as pool:
            futs = [pool.submit(r.stop) for r in replicas]
            wait(futs, timeout=self._STOP_JOIN_TIMEOUT)
            pool.shutdown(wait=False, cancel_futures=True)

        with self._lock:
            for r in replicas:
                self._release_locked(r.name, r.resources)

        self._socket_dir.cleanup()

    def get_replicas(self) -> list[Replica]:
        with self._lock:
            return [r for r in self._replicas.values() if r is not _RESERVED]

    def get_replica(self, name: str) -> Replica:
        with self._lock:
            replica = self._replicas.get(name)
            if replica is None or replica is _RESERVED:
                raise NotFound(f"Replica {name!r} is not registered")
        return replica
