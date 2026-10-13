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

from dataclasses import dataclass
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
    names: list[str], hostnames: list[str], config: PilotRuntimeConfig
) -> dict[str, list[str]]:
    """Run the named checks on every host; return the failures of each bad host."""
    unknown = [name for name in names if name not in CHECKS]
    if unknown:
        raise ValueError(f"unknown node checks {unknown}; known: {sorted(CHECKS)}")

    outputs: dict[tuple[str, ...], dict[str, str]] = {}
    failures: dict[str, list[str]] = {}
    for check in (CHECKS[name] for name in names):
        if check.command not in outputs:
            outputs[check.command] = run_on_hosts(
                list(check.command), hostnames, config
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
    "--query-gpu=index,memory.total,memory.used,"
    "ecc.errors.uncorrected.volatile.total,ecc.errors.uncorrected.aggregate.total",
    "--format=csv,noheader,nounits",
]

# No model is running yet, so nearly all GPU memory should be free.
MAX_IDLE_MEMORY_USED_FRACTION = 0.05


@dataclass(frozen=True)
class GpuRow:
    index: str
    memory_total_mib: int
    memory_used_mib: int
    ecc_volatile: str
    ecc_aggregate: str


def gpu_rows(output: str) -> list[GpuRow]:
    """Parse GPU_QUERY output: one row per GPU."""
    rows = []
    for line in output.splitlines():
        if line.strip():
            index, total, used, volatile, aggregate = (
                field.strip() for field in line.split(",")
            )
            rows.append(GpuRow(index, int(total), int(used), volatile, aggregate))
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
        limit = gpu.memory_total_mib * MAX_IDLE_MEMORY_USED_FRACTION
        if gpu.memory_used_mib > limit:
            return (
                f"GPU {gpu.index} already has {gpu.memory_used_mib} of "
                f"{gpu.memory_total_mib} MiB in use"
            )
    return None


@node_check("gpu_ecc_errors", command=GPU_QUERY)
def gpu_ecc_errors(output: str, _config: PilotRuntimeConfig) -> str | None:
    """No GPU reports uncorrected ECC errors ("[N/A]" counts as none)."""
    for gpu in gpu_rows(output):
        for count in (gpu.ecc_volatile, gpu.ecc_aggregate):
            if count.isdecimal() and int(count) > 0:
                return f"GPU {gpu.index} has {count} uncorrected ECC errors"
    return None
