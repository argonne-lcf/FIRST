"""`nvidia-smi --query-gpu` output, as `first_pilot.gpu_inventory` queries it."""

from first_pilot.gpu_inventory import QUERY_FIELDS


def gpu_row(index: int = 0, *, host: str = "host", **values: object) -> str:
    """One GPU's row. `values` override GpuInfo attributes; None prints [N/A]."""
    row: dict[str, object] = {
        "index": index,
        "name": "Test GPU",
        "uuid": f"GPU-{host}-{index}",
        "driver_version": "580.82.09",
        "memory_total_mib": 97871,
        "memory_used_mib": 0,
        "memory_free_mib": 97800,
        "ecc_uncorrected_volatile": 0,
        "ecc_uncorrected_aggregate": 0,
        "pstate": "P0",
        "temperature_c": 35,
        "utilization_gpu_pct": 0,
        "utilization_memory_pct": 0,
        "power_draw_w": 70.5,
        "clocks_throttle_reasons": "0x0000000000000001",
        **values,
    }
    return ", ".join(
        "[N/A]" if row[attr] is None else str(row[attr])
        for attr in QUERY_FIELDS.values()
    )


def gpu_rows(count: int, *, host: str = "host", **values: object) -> str:
    """Rows for GPUs 0 to count - 1."""
    return "\n".join(gpu_row(index, host=host, **values) for index in range(count))
