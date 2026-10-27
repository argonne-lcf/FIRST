from urllib.parse import urlsplit

from fastapi import APIRouter
from pydantic import BaseModel

from ...apiserver.dependencies import AppState
from ...database.redis.router_config import RouterConfig

router = APIRouter(prefix="/discovery/v1")


class PrometheusTarget(BaseModel):
    """A single target group in a Prometheus HTTP SD response."""

    targets: list[str]
    labels: dict[str, str]


@router.get("/prometheus", response_model=list[PrometheusTarget])
async def prometheus_service_discovery(
    state: AppState,
    mtls_cert_file: str | None = None,
    pilot_cluster: str | None = None,
) -> list[PrometheusTarget]:
    """Prometheus HTTP Service Discovery endpoint for live model backends.

    Advertises every live backend whose deployment exposes a
    `prometheus_metrics_path` as a scrape target.  The metrics URL is split
    into the `__address__`/`__scheme__`/`__metrics_path__` magic labels so a
    single host:port can host many distinct metrics paths; `__scrape_interval__`
    carries the deployment's scrape interval.  `instance` is pinned to the
    unique, non-recycling backend id rather than the (shared) address.

    Prometheus sets one `tls_config` per scrape job, so each mTLS certificate
    set and each pilot cluster (whose pilots carry their own TLS server name)
    needs its own job. `pilot_cluster` selects only the pilot backends on that
    cluster; `mtls_cert_file` selects the static backends presenting that
    client cert. With neither, only static backends without mTLS are listed.

    Returns HTTP 200 with an empty list when there are no targets.  The whole
    target list is returned on every scrape; Prometheus keeps its cached list
    if a refresh fails.
    """
    seen: set[str] = set()
    targets: list[PrometheusTarget] = []

    config = await RouterConfig.load(state.redis)

    for model in config.models:
        for dep in model.deployments:
            if not dep.backends or not dep.prometheus_metrics_path:
                continue
            path = dep.prometheus_metrics_path.lstrip("/")
            if dep.kind == "pilot":
                if dep.cluster_name != pilot_cluster:
                    continue
            elif pilot_cluster is not None:
                continue
            for backend in dep.backends:
                cert_file = backend.mtls.cert_file if backend.mtls else None
                if cert_file != mtls_cert_file:
                    continue
                metrics_url = f"{backend.model_url}/{path}"
                if metrics_url in seen:
                    continue
                seen.add(metrics_url)
                parts = urlsplit(metrics_url)
                targets.append(
                    PrometheusTarget(
                        targets=[parts.netloc],
                        labels={
                            "__scheme__": parts.scheme,
                            "__metrics_path__": parts.path,
                            "__scrape_interval__": f"{dep.prometheus_scrape_interval_sec}s",
                            "model": model.name,
                            "deployment": dep.name,
                            "instance": backend.id,
                        },
                    )
                )

    return targets
