"""
Startup node checks.

A check is a command that the pilot runs once on every node of its allocation,
plus a function that judges one node's output:

    @node_check("kernel", command=["uname", "-r"])
    def kernel(output: str, config: PilotRuntimeConfig) -> str | None:
        if output.strip() != "6.4.0-150600.23.25-default":
            return f"unexpected kernel {output.strip()}"
        return None

Return None for a healthy node, or a short reason for a bad one. Only a
returned reason marks a node bad: if the command cannot run or the function
raises, the pilot fails to start exactly as it would on any other startup
error, and no node is blamed.

Checks that share the same command share one launch. Enable checks by name in
`pilot_system.node_admission.checks`; see `first_pilot.admission` for what
happens to a node that fails.
"""

import csv
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Callable

from first_common.schema.pilot import PilotRuntimeConfig

from .host_commands import run_on_hosts

Evaluate = Callable[[str, PilotRuntimeConfig], str | None]


@dataclass(frozen=True)
class NodeCheck:
    name: str
    command: tuple[str, ...]
    evaluate: Evaluate


CHECKS: dict[str, NodeCheck] = {}


def node_check(name: str, *, command: list[str]) -> Callable[[Evaluate], Evaluate]:
    """Register a function as the named node check."""

    def register(evaluate: Evaluate) -> Evaluate:
        CHECKS[name] = NodeCheck(name, tuple(command), evaluate)
        return evaluate

    return register


def run_node_checks(
    names: list[str],
    hostnames: list[str],
    config: PilotRuntimeConfig,
    *,
    evidence: dict[str, object] | None = None,
) -> dict[str, list[str]]:
    """Run the named checks on every host; return the failures of each bad host."""
    unknown = [name for name in names if name not in CHECKS]
    if unknown:
        raise ValueError(f"unknown node checks {unknown}; known: {sorted(CHECKS)}")

    gpu_names = [
        name
        for name in names
        if name in _PAIRED_GPU_CHECKS
        and CHECKS[name].command == tuple(GPU_QUERY)
        and CHECKS[name].evaluate in (gpu_count, gpu_memory_free, gpu_ecc_errors)
    ]
    outputs: dict[tuple[str, ...], dict[str, str]] = {}
    failures: dict[str, list[str]] = {}
    if gpu_names:
        failures, outputs[tuple(GPU_QUERY)] = _paired_gpu_checks(
            gpu_names, hostnames, config, evidence
        )
    for check in (CHECKS[name] for name in names):
        if check.name in gpu_names:
            continue
        if check.command not in outputs:
            outputs[check.command] = run_on_hosts(
                list(check.command),
                hostnames,
                config.gpu_discovery,
                require_completion=True,
            )
        for host in hostnames:
            reason = check.evaluate(outputs[check.command][host], config)
            if reason:
                failures.setdefault(host, []).append(f"{check.name}: {reason}")
    return failures


# ---------------------------------------------------------------------------
# GPU checks
# ---------------------------------------------------------------------------

GPU_QUERY = [
    "nvidia-smi",
    "--query-gpu=index,uuid,driver_version,memory.total,memory.free,"
    "ecc.errors.uncorrected.volatile.total,ecc.errors.uncorrected.aggregate.total",
    "--format=csv,noheader,nounits",
]

GPU_PROCESS_QUERY = [
    "nvidia-smi",
    "--query-compute-apps=gpu_uuid,pid,process_name,used_gpu_memory",
    "--format=csv,noheader,nounits",
]

# The established Tara admission threshold is deliberately not equivalent to
# "used memory <= 5%": reserve another 256 MiB after rounding the 95% target up.
MIN_FREE_MEMORY_FRACTION = 0.95
MEMORY_RESERVE_MIB = 256
_PAIRED_GPU_CHECKS = {"gpu_count", "gpu_memory_free", "gpu_ecc_errors"}
_GPU_UUID = re.compile(r"GPU-[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}")
_DRIVER = re.compile(r"\d+(?:\.\d+){1,3}")


@dataclass(frozen=True)
class GpuRow:
    index: int
    uuid: str
    driver: str
    memory_total_mib: int
    memory_free_mib: int
    ecc_volatile: int
    ecc_aggregate: int

    @property
    def minimum_free_mib(self) -> int:
        return (self.memory_total_mib * 95 + 99) // 100 + MEMORY_RESERVE_MIB


def gpu_rows(output: str) -> list[GpuRow]:
    """Strict, coherent native inventory; an empty query is not a GPU fault."""
    rows: list[GpuRow] = []
    try:
        for fields in csv.reader(output.splitlines()):
            if not fields:
                continue
            index, uuid, driver, total, free, volatile, aggregate = (
                field.strip() for field in fields
            )
            numbers = (index, total, free, volatile, aggregate)
            if not all(value.isascii() and value.isdecimal() for value in numbers):
                raise ValueError("GPU numeric fields must be nonnegative integers")
            if not _GPU_UUID.fullmatch(uuid) or not _DRIVER.fullmatch(driver):
                raise ValueError("GPU identity or driver is malformed")
            row = GpuRow(
                int(index),
                uuid.lower(),
                driver,
                int(total),
                int(free),
                int(volatile),
                int(aggregate),
            )
            if (
                not 0 < row.memory_total_mib
                or row.memory_free_mib > row.memory_total_mib
            ):
                raise ValueError("GPU memory observation is incoherent")
            rows.append(row)
        if not rows:
            raise ValueError("empty GPU observations do not establish a node fault")
        if len({row.index for row in rows}) != len(rows) or len(
            {row.uuid for row in rows}
        ) != len(rows):
            raise ValueError("duplicate GPU index or UUID")
        if len({row.driver for row in rows}) != 1:
            raise ValueError("GPU driver observations disagree")
    except (ValueError, csv.Error) as exc:
        raise RuntimeError(f"malformed GPU observation: {exc}") from exc
    return rows


@node_check("gpu_count", command=GPU_QUERY)
def gpu_count(output: str, config: PilotRuntimeConfig) -> str | None:
    """Every GPU the scheduler promised is visible."""
    found = len(gpu_rows(output))
    if found != config.gpus_per_node:
        return f"found {found} GPUs, expected {config.gpus_per_node}"
    return None


@node_check("gpu_memory_free", command=GPU_QUERY)
def gpu_memory_free(output: str, _config: PilotRuntimeConfig) -> str | None:
    """No GPU memory is already in use before any model has started."""
    for gpu in gpu_rows(output):
        if gpu.memory_free_mib < gpu.minimum_free_mib:
            return (
                f"GPU {gpu.index} has {gpu.memory_free_mib} MiB free; "
                f"requires {gpu.minimum_free_mib} of {gpu.memory_total_mib} MiB"
            )
    return None


@node_check("gpu_ecc_errors", command=GPU_QUERY)
def gpu_ecc_errors(output: str, _config: PilotRuntimeConfig) -> str | None:
    """Unsupported/missing counters are infrastructure errors, not zero ECC."""
    for gpu in gpu_rows(output):
        for count in (gpu.ecc_volatile, gpu.ecc_aggregate):
            if count > 0:
                return f"GPU {gpu.index} has {count} uncorrected ECC errors"
    return None


def _gpu_processes(output: str, rows: list[GpuRow]) -> list[dict[str, object]]:
    processes: list[dict[str, object]] = []
    known_uuids = {row.uuid for row in rows}
    try:
        for fields in csv.reader(output.splitlines()):
            if not fields:
                continue
            uuid, pid, name, used = (field.strip() for field in fields)
            uuid = uuid.lower()
            if (
                uuid not in known_uuids
                or not pid.isascii()
                or not pid.isdecimal()
                or int(pid) <= 0
                or not name
            ):
                raise ValueError("incoherent GPU client identity")
            # Any client prevents unexplained-memory attribution, even when the
            # device does not report its individual memory usage.
            if used not in {"[N/A]", "N/A"} and not (
                used.isascii() and used.isdecimal()
            ):
                raise ValueError("malformed GPU client memory")
            processes.append(
                {"gpu_uuid": uuid, "pid": int(pid), "name": name, "used_mib": used}
            )
    except (ValueError, csv.Error) as exc:
        raise RuntimeError(f"malformed GPU process observation: {exc}") from exc
    return processes


def _paired_gpu_checks(
    names: list[str],
    hostnames: list[str],
    config: PilotRuntimeConfig,
    evidence: dict[str, object] | None,
) -> tuple[dict[str, list[str]], dict[str, str]]:
    snapshots: list[dict[str, list[GpuRow]]] = []
    process_snapshots: list[dict[str, list[dict[str, object]]]] = []
    timestamps: list[str] = []
    started = time.monotonic()
    for sample in range(2):
        if sample:
            time.sleep(1.0)
        output = run_on_hosts(
            GPU_QUERY, hostnames, config.gpu_discovery, require_completion=True
        )
        snapshot = {host: gpu_rows(output[host]) for host in hostnames}
        all_uuids = [row.uuid for rows in snapshot.values() for row in rows]
        if len(all_uuids) != len(set(all_uuids)):
            raise RuntimeError("GPU UUID is attributed to multiple physical hosts")
        timestamps.append(datetime.now(timezone.utc).isoformat())
        snapshots.append(snapshot)
        for host, rows in snapshot.items():
            if any(row.index >= config.gpus_per_node for row in rows):
                raise RuntimeError(
                    f"GPU indices on {host!r} disagree with requested resources"
                )
        processes: dict[str, list[dict[str, object]]] = {host: [] for host in hostnames}
        if "gpu_memory_free" in names:
            process_output = run_on_hosts(
                GPU_PROCESS_QUERY,
                hostnames,
                config.gpu_discovery,
                require_completion=True,
            )
            processes = {
                host: _gpu_processes(process_output[host], snapshot[host])
                for host in hostnames
            }
        process_snapshots.append(processes)
    if time.monotonic() - started > 30.0:
        raise RuntimeError(
            "GPU screening observations exceeded their 30s coherence window"
        )

    failures: dict[str, list[str]] = {}
    for host in hostnames:
        first, second = snapshots[0][host], snapshots[1][host]

        def identity(rows: list[GpuRow]) -> set[tuple[int, str, str, int]]:
            return {
                (row.index, row.uuid, row.driver, row.memory_total_mib) for row in rows
            }

        if identity(first) != identity(second):
            raise RuntimeError(f"GPU identity changed between observations on {host!r}")
        if process_snapshots[0][host] or process_snapshots[1][host]:
            raise RuntimeError(
                f"GPU clients present on {host!r}; not an unexplained node memory fault"
            )
        reasons: list[str] = []
        if "gpu_count" in names and len(second) != config.gpus_per_node:
            reasons.append(
                f"gpu_count: found {len(second)} GPUs, expected {config.gpus_per_node} in both observations"
            )
        old = {row.index: row for row in first}
        for gpu in second:
            previous = old[gpu.index]
            if (
                "gpu_memory_free" in names
                and gpu.memory_free_mib < gpu.minimum_free_mib
            ):
                if previous.memory_free_mib >= previous.minimum_free_mib:
                    raise RuntimeError(
                        f"GPU memory changed during screening on {host!r}; no persistent fault established"
                    )
                reasons.append(
                    f"gpu_memory_free: GPU {gpu.index} has {gpu.memory_free_mib} MiB free; requires {gpu.minimum_free_mib} MiB in both observations with no GPU clients"
                )
            elif (
                "gpu_memory_free" in names
                and previous.memory_free_mib < previous.minimum_free_mib
            ):
                raise RuntimeError(
                    f"GPU memory recovered during screening on {host!r}; no persistent fault established"
                )
            if "gpu_ecc_errors" in names and (gpu.ecc_volatile or gpu.ecc_aggregate):
                if not (previous.ecc_volatile or previous.ecc_aggregate):
                    raise RuntimeError(
                        f"GPU ECC changed during screening on {host!r}; repeat screening required"
                    )
                reasons.append(
                    f"gpu_ecc_errors: GPU {gpu.index} has uncorrected ECC errors in both observations"
                )
            elif "gpu_ecc_errors" in names and (
                previous.ecc_volatile or previous.ecc_aggregate
            ):
                raise RuntimeError(
                    f"GPU ECC changed during screening on {host!r}; repeat screening required"
                )
        if evidence is not None:
            evidence[host] = {
                "protocol": 1,
                "physical_host": host,
                "minimum_free_fraction": MIN_FREE_MEMORY_FRACTION,
                "reserve_mib": MEMORY_RESERVE_MIB,
                "samples": [
                    {
                        "observed_at": timestamps[i],
                        "gpus": [asdict(row) for row in snapshots[i][host]],
                        "processes": process_snapshots[i][host],
                    }
                    for i in range(2)
                ],
            }
        if reasons:
            failures[host] = reasons
    return failures, output
