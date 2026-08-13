"""Pure mapping functions from a parsed RouterConfig to desired V1 Endpoint rows."""

import hashlib
from dataclasses import dataclass, field
from typing import Any

from django.utils.text import slugify

from .router_config import ModelConfig, RouterConfig
from .settings import BridgeSettings

ENDPOINT_ADAPTER = "resource_server_async.endpoints.first_v2.FirstV2Endpoint"

# Bridge-managed rows use a deterministic high PK range so they never collide
# with fixture-loaded admin rows.
PK_BASE = 2**40
_PK_SPAN = 2**40

# V2 lists only healthy replicas, and only pilot deployments are proxied by the
# bridge.
_PILOT_KIND = "pilot"


def deterministic_pk(cluster: str, model: str) -> int:
    """Stable high PK keyed on the model identity (not backend id).

    Keying on ``(cluster, model)`` means replica churn is an in-place UPDATE of
    ``model_urls`` rather than a slug-thrashing delete+insert.
    """
    digest = hashlib.sha1(f"{cluster}/{model}".encode()).hexdigest()
    return PK_BASE + int(digest, 16) % _PK_SPAN


@dataclass
class DesiredEndpoint:
    pk: int
    endpoint_slug: str
    cluster: str
    framework: str
    model: str
    endpoint_adapter: str
    allowed_globus_groups: list[str]
    allowed_domains: list[str]
    config: dict[str, Any] = field(default_factory=dict)


def _resolve_prefix(
    deployment_name: str, prefix_map: dict[str, tuple[str, str]]
) -> tuple[str, str] | None:
    """Return the (cluster, framework) for a deployment name, or None if it
    matches no configured prefix."""
    for prefix, target in prefix_map.items():
        if deployment_name.startswith(prefix):
            return target
    return None


def desired_endpoints(
    cfg: RouterConfig, bridge_settings: BridgeSettings
) -> list[DesiredEndpoint]:
    """Compute the set of V1 Endpoint rows that should exist for a config.

    One row per ``(cluster, framework, model)``, carrying every healthy pilot
    backend URL. Models with no matching healthy backend produce no row (so a
    reconcile deletes any stale row).
    """
    desired: list[DesiredEndpoint] = []

    for model in cfg.models:
        # Group healthy pilot backends by the resolved (cluster, framework).
        grouped: dict[tuple[str, str], list[str]] = {}
        backend_model_name: dict[tuple[str, str], str] = {}

        for deployment in model.deployments:
            if deployment.kind != _PILOT_KIND:
                continue
            target = _resolve_prefix(deployment.name, bridge_settings.prefix_map)
            if target is None:
                continue
            for backend in deployment.backends:
                grouped.setdefault(target, []).append(backend.model_url)
                # All backends of a deployment share a backend_model_name;
                # first one wins for the group.
                backend_model_name.setdefault(target, backend.backend_model_name)

        for (cluster, framework), model_urls in grouped.items():
            if not model_urls:
                continue
            desired.append(
                _build_desired_endpoint(
                    cluster=cluster,
                    framework=framework,
                    model=model,
                    model_urls=model_urls,
                    backend_model_name=backend_model_name[(cluster, framework)],
                    bridge_settings=bridge_settings,
                )
            )

    return desired


def _build_desired_endpoint(
    *,
    cluster: str,
    framework: str,
    model: ModelConfig,
    model_urls: list[str],
    backend_model_name: str,
    bridge_settings: BridgeSettings,
) -> DesiredEndpoint:
    return DesiredEndpoint(
        pk=deterministic_pk(cluster, model.name),
        endpoint_slug=slugify(f"{cluster} {framework} {model.name.lower()}"),
        cluster=cluster,
        framework=framework,
        model=model.name,
        endpoint_adapter=ENDPOINT_ADAPTER,
        allowed_globus_groups=list(model.allowed_groups),
        allowed_domains=list(model.allowed_domains),
        config={
            "model_urls": model_urls,
            "backend_model_name": backend_model_name,
            "ca_cert_path": bridge_settings.ca_cert_path,
            "client_cert_path": bridge_settings.client_cert_path,
            "client_key_path": bridge_settings.client_key_path,
            "proxy_url": bridge_settings.proxy_url,
            "check_hostname": bridge_settings.check_hostname,
            "trust_env": bridge_settings.trust_env,
        },
    )
