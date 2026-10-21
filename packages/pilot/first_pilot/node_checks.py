"""
Startup node checks.

The pilot samples every node's GPUs with `gpu_inventory.query_gpus`, and each
enabled check judges one node's GPUs:

    def gpu_count(gpus: list[GpuInfo], config: PilotRuntimeConfig) -> Faults:
        if len(gpus) == config.gpus_per_node:
            return {}
        return {None: f"found {len(gpus)} GPUs, expected {config.gpus_per_node}"}

A check returns its faults keyed by GPU index, or None for the whole node; no
faults is a pass. Register it in CHECKS and enable it by name in
`pilot_system.node_admission.checks`; see `first_pilot.admission` for what
happens to a node that fails.

A fault counts only if a second sample, taken a second later, shows the same
checks failing on the same GPUs. Checks in IDLE_CHECKS also require that no
process is using any GPU: another model's memory is not a hardware fault.

A sample that cannot be taken or trusted (a failed command, malformed output,
GPUs that change identity between samples), or a check that lacks a value it
needs (an "[N/A]" ECC counter), raises: the pilot fails to start and no node is
blamed. Passing the screen is not proof of permanent health.
"""

import math
import time
from decimal import Decimal
from typing import Callable

from first_common.schema.pilot import GpuInfo, PilotResources, PilotRuntimeConfig

from .gpu_inventory import query_gpu_clients, query_gpus

CONFIRM_DELAY_SEC = 1.0
# Both samples must fall within this window to describe the same node state.
COHERENCE_WINDOW_SEC = 30.0


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

# Reason by GPU index, or by None for a fault of the whole node.
Faults = dict[str | None, str]
Check = Callable[[list[GpuInfo], PilotRuntimeConfig], Faults]


def gpu_count(gpus: list[GpuInfo], config: PilotRuntimeConfig) -> Faults:
    """The node exposes every GPU the allocation promised."""
    if len(gpus) == config.gpus_per_node:
        return {}
    return {None: f"found {len(gpus)} GPUs, expected {config.gpus_per_node}"}


def gpu_memory_free(gpus: list[GpuInfo], config: PilotRuntimeConfig) -> Faults:
    """Every GPU has ceil(total * fraction) + reserve MiB free."""
    policy = config.node_admission
    fraction = Decimal(str(policy.minimum_free_memory_fraction))
    faults: Faults = {}
    for gpu in gpus:
        free = _reported(gpu, "memory_free_mib", gpu.memory_free_mib)
        required = (
            math.ceil(gpu.memory_total_mib * fraction) + policy.memory_reserve_mib
        )
        if free < required:
            faults[gpu.index] = (
                f"GPU {gpu.index} has {free} MiB free; "
                f"requires {required} of {gpu.memory_total_mib} MiB"
            )
    return faults


def gpu_ecc_errors(gpus: list[GpuInfo], _config: PilotRuntimeConfig) -> Faults:
    """No GPU reports volatile or aggregate uncorrected ECC errors."""
    faults: Faults = {}
    for gpu in gpus:
        errors = max(
            _reported(gpu, "ecc_uncorrected_volatile", gpu.ecc_uncorrected_volatile),
            _reported(gpu, "ecc_uncorrected_aggregate", gpu.ecc_uncorrected_aggregate),
        )
        if errors:
            faults[gpu.index] = f"GPU {gpu.index} has {errors} uncorrected ECC errors"
    return faults


def _reported(gpu: GpuInfo, field: str, value: int | None) -> int:
    # An unavailable counter is not a passing one.
    if value is None:
        raise RuntimeError(f"GPU {gpu.index} does not report {field}")
    return value


CHECKS: dict[str, Check] = {
    "gpu_count": gpu_count,
    "gpu_memory_free": gpu_memory_free,
    "gpu_ecc_errors": gpu_ecc_errors,
}

IDLE_CHECKS = {"gpu_memory_free"}


# ---------------------------------------------------------------------------
# Screening
# ---------------------------------------------------------------------------


HostFaults = dict[tuple[str, str | None], str]  # (check, GPU index) -> reason


def run_node_checks(
    names: list[str],
    hostnames: list[str],
    config: PilotRuntimeConfig,
    *,
    evidence: dict[str, object] | None = None,
) -> dict[str, list[str]]:
    """
    Run the named checks on every host; return the failures of each bad host.
    If `evidence` is given, record the screening policy and samples in it.
    """
    unknown = [name for name in names if name not in CHECKS]
    if unknown:
        raise ValueError(f"unknown node checks {unknown}; known: {sorted(CHECKS)}")
    idle = not IDLE_CHECKS.isdisjoint(names)

    started = time.monotonic()
    samples = [_sample(hostnames, config, idle=idle)]
    faults = _evaluate(names, samples[0], config)
    if any(faults.values()):
        time.sleep(CONFIRM_DELAY_SEC)
        samples.append(_sample(hostnames, config, idle=idle))
        if time.monotonic() - started > COHERENCE_WINDOW_SEC:
            raise RuntimeError(
                f"GPU screening observations exceeded their "
                f"{COHERENCE_WINDOW_SEC:g}s coherence window"
            )
        faults = _confirm(faults, _evaluate(names, samples[1], config), samples)

    if evidence is not None:
        evidence.update(_evidence(samples, config))
    return {
        host: [
            f"{name}: {reason} in both observations"
            + (" with no GPU clients" if name in IDLE_CHECKS else "")
            for (name, _gpu), reason in found.items()
        ]
        for host, found in faults.items()
        if found
    }


def _sample(
    hostnames: list[str], config: PilotRuntimeConfig, *, idle: bool
) -> PilotResources:
    """Every host's GPUs at one moment; raises unless the sample is trustworthy."""
    sample = query_gpus(hostnames, config.gpu_discovery)
    if any(
        int(gpu.index) >= config.gpus_per_node
        for host in sample.hosts
        for gpu in host.gpus
    ):
        raise RuntimeError("GPU indices disagree with requested resources")
    if idle:
        clients = query_gpu_clients(hostnames, config.gpu_discovery)
        busy = [host for host in hostnames if clients[host]]
        if busy:
            raise RuntimeError(
                f"GPU clients present on {busy}; not an unexplained node memory fault"
            )
    return sample


def _evaluate(
    names: list[str], sample: PilotResources, config: PilotRuntimeConfig
) -> dict[str, HostFaults]:
    return {
        host.hostname: {
            (name, index): reason
            for name in names
            for index, reason in CHECKS[name](host.gpus, config).items()
        }
        for host in sample.hosts
    }


def _confirm(
    suspected: dict[str, HostFaults],
    confirmed: dict[str, HostFaults],
    samples: list[PilotResources],
) -> dict[str, HostFaults]:
    """The second sample's faults, if they repeat the first's on the same GPUs."""
    first, second = ({h.hostname: h.gpus for h in sample.hosts} for sample in samples)
    for host in suspected:
        if _identities(first[host]) != _identities(second[host]):
            raise RuntimeError(f"GPU identity changed between observations on {host!r}")
        if suspected[host].keys() != confirmed[host].keys():
            raise RuntimeError(
                f"GPU observations changed on {host!r}; no persistent fault established"
            )
    return confirmed


def _identities(gpus: list[GpuInfo]) -> set[tuple[object, ...]]:
    """What must not change between two samples of the same GPUs."""
    return {
        (gpu.index, gpu.uuid, gpu.driver_version, gpu.memory_total_mib) for gpu in gpus
    }


def _evidence(
    samples: list[PilotResources], config: PilotRuntimeConfig
) -> dict[str, object]:
    return {
        "minimum_free_fraction": config.node_admission.minimum_free_memory_fraction,
        "reserve_mib": config.node_admission.memory_reserve_mib,
        "samples": [sample.model_dump(mode="json") for sample in samples],
    }
