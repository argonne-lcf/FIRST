"""Instant PromQL queries for health checks that judge Prometheus metrics."""

from pathlib import Path

from httpx import AsyncClient


def prometheus_client(ca_crt_file: Path | None) -> AsyncClient:
    return (
        AsyncClient(timeout=10.0, verify=str(ca_crt_file))
        if ca_crt_file is not None
        else AsyncClient(timeout=10.0)
    )


async def query(
    http: AsyncClient, prometheus_url: str, promql: str
) -> list[tuple[dict[str, str], float]]:
    """Run an instant query; return (labels, value) per series."""
    resp = await http.get(f"{prometheus_url}/api/v1/query", params={"query": promql})
    resp.raise_for_status()
    body = resp.json()
    if body.get("status") != "success":
        raise RuntimeError(f"Prometheus query failed: {body.get('error', body)}")
    return [
        (series["metric"], float(series["value"][1]))
        for series in body["data"]["result"]
    ]
