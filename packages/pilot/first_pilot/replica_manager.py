import hashlib
import logging
import os
import re
import socket
import stat
import subprocess
import threading
import time
from collections import defaultdict
from enum import Enum
from pathlib import Path
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
)

from .replica import Replica

logger = logging.getLogger(__name__)

REPLICA_PORT_OFFSET = 2


def safe_getfqdn(name: str = "", *, timeout: float = 2.0) -> str:
    """Return a DNS-free address label; retained for the control API contract."""
    del timeout
    return name or socket.gethostname()


class _ReservedSentinel(Enum):
    RESERVED = object()


ReservedSentinel = Literal[_ReservedSentinel.RESERVED]
_RESERVED = _ReservedSentinel.RESERVED


_NVIDIA_SMI_ARGS = [
    "--query-gpu=index,name,memory.total,memory.used",
    "--format=csv,noheader,nounits",
]
_NVIDIA_SMI_PATH = Path("/usr/bin/nvidia-smi")
_NVIDIA_SMI_SHA256 = "5a2c0103899cdbf5451a4d39722026fb64222ca19a54d249f2fffbf97c618bd0"
_PALS_PATH = Path("/opt/cray/pals/1.8/bin/mpiexec")
_PALS_SHA256 = "3507f81afcc0ba819a67cb210298e730bc14e16e812075e39c2bdb3d9b322925"
_PALS_LABEL = re.compile(r"^(?P<host>\S+)\s+(?P<rank>\d+):\s?(?P<row>.*)$")


def _require_frozen_executable(
    path: Path, *, expected_path: Path, expected_sha256: str, label: str
) -> str:
    if path != expected_path:
        raise RuntimeError(
            f"required {label} path differs: expected {expected_path}, got {path}"
        )
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise RuntimeError(f"required {label} is unavailable: {path}") from exc
    if (
        path.is_symlink()
        or not stat.S_ISREG(info.st_mode)
        or info.st_nlink != 1
        or info.st_uid != 0
        or info.st_gid != 0
        or stat.S_IMODE(info.st_mode) != 0o755
        or not os.access(path, os.X_OK)
    ):
        raise RuntimeError(f"required {label} identity differs: {path}")
    try:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as exc:
        raise RuntimeError(f"required {label} is unreadable: {path}") from exc
    if digest != expected_sha256:
        raise RuntimeError(f"required {label} digest differs: {path}")
    return str(path)


def _require_nvidia_smi() -> str:
    return _require_frozen_executable(
        _NVIDIA_SMI_PATH,
        expected_path=Path("/usr/bin/nvidia-smi"),
        expected_sha256=_NVIDIA_SMI_SHA256,
        label="GPU inventory binary",
    )


def _require_pals(pals_path: Path) -> str:
    return _require_frozen_executable(
        pals_path,
        expected_path=_PALS_PATH,
        expected_sha256=_PALS_SHA256,
        label="PALS launcher",
    )


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


def query_gpus(hostname: str, expected_gpus: int) -> HostGpus:
    """Inventory a single-node pilot locally; never use a remote shell."""
    nvidia_smi = _require_nvidia_smi()
    try:
        result = subprocess.run(
            [nvidia_smi, *_NVIDIA_SMI_ARGS],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("local GPU inventory timed out after 5s") from exc

    if result.returncode != 0:
        raise RuntimeError(
            f"local GPU inventory exited {result.returncode}: "
            f"{result.stderr.strip()[-2000:]}"
        )

    try:
        gpus = [_parse_gpu_row(line) for line in result.stdout.splitlines() if line]
    except ValueError as exc:
        raise RuntimeError(f"local GPU inventory was malformed: {exc}") from exc
    return _validated_host_gpus(hostname, gpus, expected_gpus)


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
    hostnames: list[str], pals_path: Path, expected_gpus: int
) -> list[HostGpus]:
    """Run one labeled inventory rank per host through the site PALS launcher."""
    nvidia_smi = _require_nvidia_smi()
    pals = _require_pals(pals_path)

    command = [
        pals,
        "--pmi=pmix",
        "--genvnone",
        "--no-transfer",
        "--line-buffer",
        "--label",
        "--abort-on-failure",
        "--timeout",
        "30",
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
            timeout=35,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("PALS GPU inventory timed out after 35s") from exc

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
    # Mirrors PilotControlClient.STOP_TIMEOUT: pre-stop, model TERM/KILL,
    # post-stop verification, auxiliary-group cleanup, and monitor join are all
    # bounded below this ceiling.
    _STOP_JOIN_TIMEOUT = 120.0

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

        # The lock serializes the small critical section in start_replica /
        # stop_replica that mutates these three structures together:
        #   self._replicas, self._claimed, self._used_ports
        self._lock = threading.Lock()
        self._replicas: dict[str, Replica | ReservedSentinel] = {}
        self._claimed: set[tuple[str, str]] = set()
        self._used_ports: set[int] = set()

    @ttl_cache(ttl=60)
    def query_resources(self) -> PilotResources:
        """Discover the exact scheduler-requested inventory without SSH."""
        if len(self.node_hostnames) == 1:
            host_gpus = [query_gpus(self.node_hostnames[0], self.config.gpus_per_node)]
        else:
            if self.config.pals_path is None:
                raise RuntimeError("multi-node GPU inventory requires pals_path")
            host_gpus = query_gpus_pals(
                self.node_hostnames,
                self.config.pals_path,
                self.config.gpus_per_node,
            )

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

    def _allocate_port_locked(self) -> int:
        # Caller must hold self._lock.
        port = self.config.external_port + REPLICA_PORT_OFFSET
        while port in self._used_ports:
            port += 1
        self._used_ports.add(port)
        return port

    def _release_locked(
        self,
        name: str,
        resources: list[GpuClaim],
        port: int,
        *,
        expected: Replica | ReservedSentinel,
    ) -> bool:
        """Release only if ``name`` still refers to the exact owned object."""
        # Caller must hold self._lock. The identity guard prevents a late
        # concurrent stop from releasing a replacement Replica's claims/port.
        if self._replicas.get(name) is not expected:
            return False
        del self._replicas[name]
        self._claimed.difference_update(self._flatten(resources))
        self._used_ports.discard(port)
        return True

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
            port = self._allocate_port_locked()
            # Insert a placeholder under the name so a racing start_replica
            # for the same name fails fast. We swap in the real Replica below.
            self._replicas[replica.name] = _RESERVED

        try:
            workdir = self.config.replica_base_dir / replica.name
            workdir.mkdir(parents=True, exist_ok=True)

            r = Replica(
                name=replica.name,
                port=port,
                resources=resources,
                launch_spec=replica.launch_spec,
                workdir=workdir,
            )
        except Exception as e:
            logger.exception(
                "failed to start replica %s; releasing reservation", replica.name
            )
            with self._lock:
                self._release_locked(
                    replica.name,
                    resources,
                    port,
                    expected=_RESERVED,
                )
            raise ReplicaStartError(f"Failed to start replica: {e}") from e

        with self._lock:
            self._replicas[replica.name] = r

    def stop_replica(self, replica_name: str) -> None:
        with self._lock:
            replica = self._replicas.get(replica_name)
            if replica is None or replica is _RESERVED:
                raise NotFound(f"Replica {replica_name!r} is not registered")

        logger.info("stopping replica %s", replica_name)
        # Replica.stop serializes concurrent callers. Keep the object and its
        # claims addressable until it reports authoritative process-group
        # absence; an exception is intentionally retryable.
        replica.stop()

        with self._lock:
            released = self._release_locked(
                replica_name,
                replica.resources,
                replica.port,
                expected=replica,
            )
        if not released:
            logger.info(
                "Replica %s stopped, but its manager entry now has a different "
                "identity; leaving the current entry untouched",
                replica_name,
            )

    def stop_all(self) -> None:
        replicas = self.get_replicas()
        logger.info("stopping all %d replicas", len(replicas))
        if not replicas:
            return

        # ThreadPoolExecutor workers are registered for an unconditional join
        # at interpreter exit, even after shutdown(wait=False).  A wedged
        # Replica.stop would therefore keep the pilot process alive past this
        # method's bound.  Explicit daemon workers preserve the outer deadline
        # and cannot extend interpreter shutdown.
        done_events = {id(replica): threading.Event() for replica in replicas}
        errors: dict[int, BaseException] = {}

        def stop_and_release(replica: Replica) -> None:
            try:
                replica.stop()
                # This also handles a completion after stop_all's deadline. If
                # the manager remains live, resources are released promptly;
                # if a replacement now owns the name, the identity guard makes
                # the late completion harmless.
                with self._lock:
                    self._release_locked(
                        replica.name,
                        replica.resources,
                        replica.port,
                        expected=replica,
                    )
            except BaseException as exc:
                errors[id(replica)] = exc
                logger.exception(
                    "stop_all teardown failed for replica %s", replica.name
                )
            finally:
                done_events[id(replica)].set()

        for replica in replicas:
            worker = threading.Thread(
                target=stop_and_release,
                args=(replica,),
                name=f"replica-stop-{replica.name}",
                daemon=True,
            )
            try:
                worker.start()
            except BaseException as exc:
                errors[id(replica)] = exc
                done_events[id(replica)].set()
                logger.exception(
                    "could not start stop_all worker for replica %s", replica.name
                )

        deadline = time.monotonic() + self._STOP_JOIN_TIMEOUT
        for replica in replicas:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            done_events[id(replica)].wait(timeout=remaining)

        failures: list[str] = []
        for replica in replicas:
            done = done_events[id(replica)].is_set()
            if done and (stop_error := errors.get(id(replica))) is not None:
                failures.append(f"{replica.name}: {stop_error}")
                continue
            if not done:
                logger.error(
                    "stop_all timed out after %.1fs waiting for replica %s",
                    self._STOP_JOIN_TIMEOUT,
                    replica.name,
                )
                failures.append(f"{replica.name}: teardown timed out")

        if failures:
            raise RuntimeError(
                "one or more replicas did not stop authoritatively: "
                + "; ".join(sorted(failures))
            )

    def get_replicas(self) -> list[Replica]:
        with self._lock:
            return [r for r in self._replicas.values() if r is not _RESERVED]

    def get_replica(self, name: str) -> Replica:
        with self._lock:
            replica = self._replicas.get(name)
            if replica is None or replica is _RESERVED:
                raise NotFound(f"Replica {name!r} is not registered")
        return replica
