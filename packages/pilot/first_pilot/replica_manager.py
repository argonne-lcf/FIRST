import logging
import os
import re
import socket
import stat
import threading
import time
from collections import defaultdict
from enum import Enum
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal

from first_common.errors import (
    BadPilotRequest,
    NotFound,
    ReplicaAlreadyPlaced,
    ReplicaStartError,
    ReplicaTeardownError,
)
from first_common.schema.pilot import (
    GpuInfo,
    HostGpus,
    PilotResources,
    PilotRuntimeConfig,
    ReplicaStartRequest,
)
from first_common.schema.types import GpuClaim, GpuDiscovery

from .host_commands import deduplicate_hosts, run_on_hosts
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
_REPLICA_UDS_NAME = re.compile(r"^replica-(?:0|[1-9][0-9]*)\.sock$")


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


def _host_gpus(hostname: str, output: str, expected_gpus: int) -> HostGpus:
    """Parse and validate one host's nvidia-smi inventory."""
    try:
        gpus = [_parse_gpu_row(line) for line in output.splitlines() if line]
    except ValueError as exc:
        raise RuntimeError(
            f"GPU inventory for host {hostname!r} was malformed: {exc}"
        ) from exc

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


def query_gpus(
    hostnames: list[str], discovery: GpuDiscovery, expected_gpus: int
) -> list[HostGpus]:
    """Inventory every scheduler host's GPUs, in scheduler order."""
    outputs = run_on_hosts(["nvidia-smi", *_NVIDIA_SMI_ARGS], hostnames, discovery)
    return [_host_gpus(host, outputs[host], expected_gpus) for host in hostnames]


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
    # bounded to 162s below this ceiling, including an opt-in 80s pre-stop hook.
    _STOP_JOIN_TIMEOUT = 180.0
    _INVENTORY_REFRESH_SEC = 60.0

    def __init__(self, config: PilotRuntimeConfig) -> None:
        self.config = config

        # PBS nodefiles may repeat a hostname once per assigned resource.  Keep
        # the scheduler's first-occurrence order because it is also the PALS
        # rank order used by gpus_by_host in the replica launch context.
        self.node_hostnames = deduplicate_hosts(
            discover_hosts(self.config.node_file_env)
        )
        if len(self.node_hostnames) != self.config.num_nodes:
            raise RuntimeError(
                "pilot node inventory differs from its scheduler request: "
                f"discovered {len(self.node_hostnames)}, "
                f"expected {self.config.num_nodes}"
            )
        resources = self.query_resources()

        # Hosts and GPU indices are fixed for the life of the job; only the
        # memory usage in self._resources is refreshed after startup.
        self._inventory = resources.hosts
        self._resources = resources
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

    def get_resources(self) -> PilotResources:
        """Latest inventory snapshot; never blocks on discovery."""
        return self._resources

    def start_inventory_refresh(self) -> None:
        """Keep the snapshot's GPU memory usage current for the process lifetime."""
        threading.Thread(
            target=self._inventory_refresh_loop,
            name="inventory-refresh",
            daemon=True,
        ).start()

    def _inventory_refresh_loop(self) -> None:
        while True:
            time.sleep(self._INVENTORY_REFRESH_SEC)
            self._refresh_inventory()

    def _refresh_inventory(self) -> None:
        try:
            self._resources = self.query_resources()
        except Exception:
            logger.exception("GPU inventory refresh failed; keeping last snapshot")

    def query_resources(self) -> PilotResources:
        """Discover and validate the exact scheduler-requested inventory."""
        return PilotResources(
            hosts=query_gpus(
                self.node_hostnames,
                self.config.gpu_discovery,
                self.config.gpus_per_node,
            )
        )

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
        base_dir = self.config.replica_base_dir.resolve()
        if not (base_dir / name).resolve().is_relative_to(base_dir):
            raise BadPilotRequest(f"replica name {name!r} escapes the replica workdir")

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

    def _release_locked(
        self,
        name: str,
        resources: list[GpuClaim],
        expected: Replica | ReservedSentinel,
    ) -> bool:
        """Release only when the name still maps to the expected owner."""
        # Caller must hold self._lock. A duplicate release is a no-op, which is
        # important if a replacement replica has since claimed the same name or
        # GPUs.
        if self._replicas.get(name) is not expected:
            return False
        del self._replicas[name]
        self._claimed.difference_update(self._flatten(resources))
        return True

    def _unlink_replica_uds(self, replica: Replica) -> None:
        """Remove one inert socket pathname after authoritative teardown.

        Refuse anything except the exact socket shape minted in the manager's
        private directory. Callers retain the replica and claims on failure so
        an idempotent stop can retry cleanup.
        """
        socket_dir = Path(self._socket_dir.name)
        uds = Path(replica.uds)
        error_prefix = f"Replica {replica.name} teardown incomplete; UDS cleanup"
        if (
            not socket_dir.is_absolute()
            or not uds.is_absolute()
            or uds.parent != socket_dir
            or _REPLICA_UDS_NAME.fullmatch(uds.name) is None
        ):
            raise ReplicaTeardownError(f"{error_prefix} refused unsafe path {uds}")

        dir_fd: int | None = None
        try:
            dir_fd = os.open(
                socket_dir,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
            )
            before = os.stat(uds.name, dir_fd=dir_fd, follow_symlinks=False)
            if not stat.S_ISSOCK(before.st_mode):
                raise ReplicaTeardownError(
                    f"{error_prefix} refused non-socket {uds.name}"
                )

            try:
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
                    probe.settimeout(0.2)
                    probe.connect(replica.uds)
            except ConnectionRefusedError:
                pass
            else:
                raise ReplicaTeardownError(
                    f"{error_prefix} refused active listener {uds.name}"
                )

            # Do not unlink a replacement entry created during the probe.
            after = os.stat(uds.name, dir_fd=dir_fd, follow_symlinks=False)
            if not stat.S_ISSOCK(after.st_mode) or (after.st_dev, after.st_ino) != (
                before.st_dev,
                before.st_ino,
            ):
                raise ReplicaTeardownError(
                    f"{error_prefix} refused changed entry {uds.name}"
                )
            os.unlink(uds.name, dir_fd=dir_fd)
            logger.info("removed inert replica socket %s", uds)
        except FileNotFoundError:
            return
        except ReplicaTeardownError:
            raise
        except OSError as exc:
            raise ReplicaTeardownError(f"{error_prefix} failed: {exc}") from exc
        finally:
            if dir_fd is not None:
                os.close(dir_fd)

    def start_replica(self, replica: ReplicaStartRequest) -> None:
        # Reused pilots must not bypass the pending/held quarantine guard. This
        # applies only to this new attempt; existing healthy replicas keep running.
        # Local import avoids admission/discover_hosts' module dependency cycle.
        from .admission import guard_quarantined_nodes

        guard_quarantined_nodes(self.config, self.node_hostnames)
        requested = self._validate_request(replica.name, replica.gpu_indices)

        host_gpus: dict[str, list[str]] = defaultdict(list)
        for hostname, gpu_id in requested:
            host_gpus[hostname].append(gpu_id)

        resources = [
            GpuClaim(hostname=hostname, gpu_ids=gpu_list)
            for hostname, gpu_list in host_gpus.items()
        ]

        # Short critical section: reserve name + GPUs + socket identity atomically.
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
                proxy_paths=replica.proxy_paths,
            )
        except Exception as e:
            logger.exception(
                "failed to start replica %s; releasing reservation", replica.name
            )
            with self._lock:
                self._release_locked(
                    replica.name,
                    resources,
                    _RESERVED,
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
        self._unlink_replica_uds(replica)

        with self._lock:
            released = self._release_locked(
                replica_name,
                replica.resources,
                replica,
            )
        if not released:
            logger.info("Replica %s was already released", replica_name)

    def stop_all(self) -> None:
        """Stop every registered replica within one bounded shutdown pass.

        This is a shutdown-only operation. Its caller must quiesce concurrent
        ``start_replica`` requests before invoking it.
        """
        replicas = self.get_replicas()
        logger.info("stopping all %d replicas", len(replicas))
        if not replicas:
            self._socket_dir.cleanup()
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
                self._unlink_replica_uds(replica)
                # This also handles a completion after stop_all's deadline. If
                # the manager remains live, resources are released promptly. A
                # duplicate release after the entry is gone is a no-op.
                with self._lock:
                    self._release_locked(
                        replica.name,
                        replica.resources,
                        replica,
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
            worker.start()

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
