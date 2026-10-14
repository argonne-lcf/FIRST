"""
Startup node checks.

A check is a command that the pilot runs once on every node of its allocation,
plus a function that judges one node's output:

    @node_check("kernel", command=["uname", "-r"])
    def kernel(output: str, config: PilotRuntimeConfig) -> str | None:
        if output.strip() != "6.4.0-150600.23.25-default":
            return f"unexpected kernel {output.strip()}"
        return None

Return None for a passing observation, or a short reason for a suspect fault.
Commands must complete on every allocation host. Failed commands, malformed
data and changing GPU identities are infrastructure errors: the pilot exits
without diagnosing or quarantining a node.

Built-in GPU checks share a parsed inventory. A suspect fault requires a
second coherent observation of the same GPUs; GPU memory checks also require
no compute clients. This avoids turning transient activity or another model's
memory into a hardware diagnosis. Passing startup screening is not proof of
permanent hardware health.

Checks that share the same command share one launch. Enable checks by name in
`pilot_system.node_admission.checks`; see `first_pilot.admission` for what
happens to a node that fails.
"""

import csv
import math
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
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

    gpu_names = [name for name in names if name in _PAIRED_GPU_CHECKS]
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
    "--query-compute-apps=pid",
    "--format=csv,noheader,nounits",
]

_PAIRED_GPU_CHECKS = {"gpu_count", "gpu_memory_free", "gpu_ecc_errors"}


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
    def identity(self) -> tuple[int, str, str, int]:
        return self.index, self.uuid, self.driver, self.memory_total_mib


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
            if not uuid or not driver:
                raise ValueError("GPU identity or driver is missing")
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


def _gpu_faults(
    rows: list[GpuRow], names: list[str], config: PilotRuntimeConfig
) -> dict[tuple[str, int | None], str]:
    """One parsed observation; retain which GPU/check needs confirmation."""
    faults: dict[tuple[str, int | None], str] = {}
    if "gpu_count" in names and len(rows) != config.gpus_per_node:
        faults["gpu_count", None] = (
            f"found {len(rows)} GPUs, expected {config.gpus_per_node}"
        )
    for gpu in rows:
        minimum_free_mib = (
            math.ceil(
                gpu.memory_total_mib
                * Decimal(str(config.node_admission.minimum_free_memory_fraction))
            )
            + config.node_admission.memory_reserve_mib
        )
        if "gpu_memory_free" in names and gpu.memory_free_mib < minimum_free_mib:
            faults["gpu_memory_free", gpu.index] = (
                f"GPU {gpu.index} has {gpu.memory_free_mib} MiB free; "
                f"requires {minimum_free_mib} of {gpu.memory_total_mib} MiB"
            )
        if "gpu_ecc_errors" in names and (gpu.ecc_volatile or gpu.ecc_aggregate):
            faults["gpu_ecc_errors", gpu.index] = (
                f"GPU {gpu.index} has {max(gpu.ecc_volatile, gpu.ecc_aggregate)} uncorrected ECC errors"
            )
    return faults


@node_check("gpu_count", command=GPU_QUERY)
def gpu_count(output: str, config: PilotRuntimeConfig) -> str | None:
    """The complete inventory exposes every GPU promised by the allocation."""
    return next(
        iter(_gpu_faults(gpu_rows(output), ["gpu_count"], config).values()), None
    )


@node_check("gpu_memory_free", command=GPU_QUERY)
def gpu_memory_free(output: str, config: PilotRuntimeConfig) -> str | None:
    """Every GPU has the configured free-memory fraction plus MiB reserve.

    Admission confirms a suspect result in a second sample with no compute
    clients before treating unexplained retained memory as a node fault.
    """
    return next(
        iter(_gpu_faults(gpu_rows(output), ["gpu_memory_free"], config).values()), None
    )


@node_check("gpu_ecc_errors", command=GPU_QUERY)
def gpu_ecc_errors(output: str, config: PilotRuntimeConfig) -> str | None:
    """No GPU reports volatile or aggregate uncorrected ECC errors.

    Missing or unavailable ECC counters are incomplete observations, not a
    passing result. Admission confirms a reported error before quarantine.
    """
    return next(
        iter(_gpu_faults(gpu_rows(output), ["gpu_ecc_errors"], config).values()), None
    )


def _paired_gpu_checks(
    names: list[str],
    hostnames: list[str],
    config: PilotRuntimeConfig,
    evidence: dict[str, object] | None,
) -> tuple[dict[str, list[str]], dict[str, str]]:
    """Confirm suspected faults with complete, stable GPU/client observations."""

    def observe() -> tuple[dict[str, str], dict[str, list[GpuRow]], str]:
        output = run_on_hosts(
            GPU_QUERY, hostnames, config.gpu_discovery, require_completion=True
        )
        rows = {host: gpu_rows(output[host]) for host in hostnames}
        uuids = [gpu.uuid for gpus in rows.values() for gpu in gpus]
        if len(uuids) != len(set(uuids)):
            raise RuntimeError("GPU UUID is attributed to multiple physical hosts")
        if any(
            gpu.index >= config.gpus_per_node for gpus in rows.values() for gpu in gpus
        ):
            raise RuntimeError("GPU indices disagree with requested resources")
        if "gpu_memory_free" in names:
            clients = run_on_hosts(
                GPU_PROCESS_QUERY,
                hostnames,
                config.gpu_discovery,
                require_completion=True,
            )
            for host in hostnames:
                if clients[host].strip():
                    raise RuntimeError(
                        f"GPU clients present on {host!r}; not an unexplained node memory fault"
                    )
        return output, rows, datetime.now(timezone.utc).isoformat()

    started = time.monotonic()
    output, first, observed = observe()
    faults = {host: _gpu_faults(first[host], names, config) for host in hostnames}
    samples = [(first, observed)]
    # Healthy admission is a point-in-time screen, not a hardware diagnosis.
    # A suspect check must persist on the same GPU in a second complete sample.
    if any(faults.values()):
        time.sleep(1.0)
        output, second, observed = observe()
        if time.monotonic() - started > 30.0:
            raise RuntimeError(
                "GPU screening observations exceeded their 30s coherence window"
            )
        for host in hostnames:
            if {gpu.identity for gpu in first[host]} != {
                gpu.identity for gpu in second[host]
            }:
                raise RuntimeError(
                    f"GPU identity changed between observations on {host!r}"
                )
            current = _gpu_faults(second[host], names, config)
            if faults[host].keys() != current.keys():
                raise RuntimeError(
                    f"GPU observations changed on {host!r}; no persistent fault established"
                )
            faults[host] = current
        samples.append((second, observed))
    if evidence is not None:
        for host in hostnames:
            evidence[host] = {
                "protocol": 1,
                "physical_host": host,
                "minimum_free_fraction": config.node_admission.minimum_free_memory_fraction,
                "reserve_mib": config.node_admission.memory_reserve_mib,
                "samples": [
                    {
                        "observed_at": when,
                        "gpus": [asdict(gpu) for gpu in rows[host]],
                        "processes": [],
                    }
                    for rows, when in samples
                ],
            }
    failures = {}
    for host, checks in faults.items():
        if checks:
            failures[host] = [
                f"{name}: {reason} in both observations"
                + (" with no GPU clients" if name == "gpu_memory_free" else "")
                for (name, _), reason in checks.items()
            ]
    return failures, output
