"""
The pilot's view of its GPUs: one nvidia-smi query on every host, parsed into
`GpuInfo`. The replica manager publishes it as the pilot's resources, and the
node checks screen it at startup.
"""

import csv
from datetime import datetime, timezone

from first_common.schema.pilot import GpuInfo, HostGpus, PilotResources
from first_common.schema.types import GpuDiscovery

from .host_commands import run_on_hosts

# nvidia-smi --query-gpu field -> GpuInfo attribute, in query order
QUERY_FIELDS = {
    "index": "index",
    "name": "name",
    "uuid": "uuid",
    "driver_version": "driver_version",
    "memory.total": "memory_total_mib",
    "memory.used": "memory_used_mib",
    "memory.free": "memory_free_mib",
    "ecc.errors.uncorrected.volatile.total": "ecc_uncorrected_volatile",
    "ecc.errors.uncorrected.aggregate.total": "ecc_uncorrected_aggregate",
    "pstate": "pstate",
    "temperature.gpu": "temperature_c",
    "utilization.gpu": "utilization_gpu_pct",
    "utilization.memory": "utilization_memory_pct",
    "power.draw": "power_draw_w",
    "clocks_throttle_reasons.active": "clocks_throttle_reasons",
}

# Every NVIDIA GPU reports these; the rest may be unavailable.
_REQUIRED = {
    "index",
    "name",
    "uuid",
    "driver_version",
    "memory_total_mib",
    "memory_used_mib",
    "memory_free_mib",
}

GPU_QUERY = [
    "nvidia-smi",
    f"--query-gpu={','.join(QUERY_FIELDS)}",
    "--format=csv,noheader,nounits",
]

GPU_CLIENT_QUERY = [
    "nvidia-smi",
    "--query-compute-apps=pid",
    "--format=csv,noheader,nounits",
]


def query_gpus(hostnames: list[str], discovery: GpuDiscovery) -> PilotResources:
    """Every host's GPUs, in scheduler order; raises unless they are coherent."""
    observed_at = datetime.now(timezone.utc)
    outputs = run_on_hosts(GPU_QUERY, hostnames, discovery)
    hosts = []
    for host in hostnames:
        try:
            hosts.append(HostGpus(hostname=host, gpus=parse_gpus(outputs[host])))
        except ValueError as exc:
            raise RuntimeError(
                f"malformed GPU inventory from host {host!r}: {exc}"
            ) from exc
    uuids = [gpu.uuid for host in hosts for gpu in host.gpus]
    if len(set(uuids)) < len(uuids):
        raise RuntimeError("GPU UUID is attributed to multiple physical hosts")
    return PilotResources(hosts=hosts, observed_at=observed_at)


def query_gpu_clients(
    hostnames: list[str], discovery: GpuDiscovery
) -> dict[str, list[str]]:
    """The PIDs of each host's GPU compute processes."""
    outputs = run_on_hosts(GPU_CLIENT_QUERY, hostnames, discovery)
    return {host: outputs[host].split() for host in hostnames}


def parse_gpus(output: str) -> list[GpuInfo]:
    """One host's GPU_QUERY output, by index; raises ValueError unless coherent."""
    try:
        rows = [row for row in csv.reader(output.splitlines()) if row]
    except csv.Error as exc:
        raise ValueError(str(exc)) from exc
    gpus = sorted((_parse_gpu(row) for row in rows), key=lambda gpu: int(gpu.index))
    if not gpus:
        # nvidia-smi ran but saw nothing: not evidence of a node fault.
        raise ValueError("no GPUs reported")
    if len({gpu.index for gpu in gpus}) < len(gpus):
        raise ValueError("duplicate GPU index")
    if len({gpu.uuid for gpu in gpus}) < len(gpus):
        raise ValueError("duplicate GPU UUID")
    if len({gpu.driver_version for gpu in gpus}) > 1:
        raise ValueError("GPU driver versions disagree")
    return gpus


def _parse_gpu(row: list[str]) -> GpuInfo:
    if len(row) != len(QUERY_FIELDS):
        raise ValueError(f"expected {len(QUERY_FIELDS)} fields: {row}")
    values = {attr: _value(field) for attr, field in zip(QUERY_FIELDS.values(), row)}
    missing = sorted(attr for attr in _REQUIRED if values[attr] is None)
    if missing:
        raise ValueError(f"GPU reports no {', '.join(missing)}: {row}")
    gpu = GpuInfo.model_validate(values)  # a ValidationError is a ValueError
    assert gpu.memory_free_mib is not None
    if max(gpu.memory_used_mib, gpu.memory_free_mib) > gpu.memory_total_mib:
        raise ValueError(f"GPU memory observation is incoherent: {row}")
    return gpu


def _value(field: str) -> str | None:
    """None for a value nvidia-smi cannot report: [N/A], [Not Supported], ..."""
    value = field.strip()
    return None if value.startswith("[") and value.endswith("]") else value
