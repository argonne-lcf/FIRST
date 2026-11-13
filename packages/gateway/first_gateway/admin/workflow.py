"""Client-side catalog snapshots and scoped plans over the existing full API.

The server remains the only configuration authority. A scoped plan overlays
explicit resources on a fresh complete catalog; it never submits a partial
catalog or silently rebases an approved plan.
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict
from yaml import safe_dump_all

from first_common.errors import InvalidSpecError
from first_common.schema.resources import ResourceChangePlan, ResourceManifest
from first_common.schema.resources.spec import ResourceSpec

from .client import AdminClient


class SavedPlan(BaseModel):
    """Exact reviewed payload, bound to both operator-selected service URLs."""

    model_config = ConfigDict(extra="forbid")

    controller_url: str
    gateway_url: str
    resources: list[ResourceManifest]
    approved_plan: ResourceChangePlan
    targets: list[str]


def service_urls(client: AdminClient) -> dict[str, str]:
    return {
        "controller_url": str(client.base_url).rstrip("/"),
        "gateway_url": str(client.gateway.base_url).rstrip("/"),
    }


def _resources_by_id(
    resources: list[ResourceManifest],
) -> dict[str, ResourceManifest]:
    result = {}
    for resource in resources:
        identity = f"{resource.kind}.{resource.name}"
        if identity in result:
            raise InvalidSpecError(f"Duplicate resource: {identity}")
        result[identity] = resource
    return result


def capture_catalog(
    client: AdminClient,
) -> tuple[list[ResourceManifest], dict[str, Any]]:
    """Read a complete catalog without mixing ConfigVersions (three attempts).

    Runtime observations are separate: manual scaling and request counters do
    not advance ConfigVersion and cannot be an atomic configuration snapshot.
    A no-change server plan also checks that the selected gateway's catalog
    agrees with the selected controller before it can become an overlay base.
    """
    for _ in range(3):
        versions = client.list_config_versions()
        before = max((version.uid for version in versions), default=0)
        catalog = client.gateway.catalog
        deployments = [
            catalog.get_pilot_deployment(deployment.name)
            for deployment in catalog.pilot_deployments()
        ]
        clusters = catalog.clusters()
        groups = catalog.access_groups()
        models = catalog.models()
        templates = catalog.launch_templates()
        statics = catalog.static_deployments()
        resources = []
        for kind, rows in (
            ("AccessGroup", groups),
            ("Model", models),
            ("Cluster", clusters),
            ("LaunchTemplate", templates),
            ("StaticDeployment", statics),
            ("PilotDeployment", deployments),
        ):
            spec = ResourceSpec.registry[kind]
            for row in rows:
                values = row.model_dump(mode="json")
                resources.append(
                    ResourceManifest.model_validate(
                        {
                            "kind": kind,
                            "name": row.name,
                            "spec": {key: values[key] for key in spec.model_fields},
                        }
                    )
                )
        jobs = [
            catalog.get_cluster(cluster.name).model_dump(mode="json")
            for cluster in clusters
            if cluster.pilot_system is not None
        ]
        versions = client.list_config_versions()
        after = max((version.uid for version in versions), default=0)
        if before != after:
            continue
        plan = client.plan(resources)
        if plan.previous_version != after:
            continue
        if plan.to_add or plan.to_update or plan.to_delete:
            raise InvalidSpecError(
                "Gateway catalog differs from controller state. Check --base-url "
                "and --gateway-url; do not use this export as an overlay base."
            )
        return resources, {
            **service_urls(client),
            "config_version": after,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "desired_replicas": {d.name: d.desired_replicas for d in deployments},
            "pilot_deployments": [d.model_dump(mode="json") for d in deployments],
            "clusters": jobs,
            "runtime_snapshot_atomic": False,
            "running_source_revision": "unverified",
        }
    raise InvalidSpecError(
        "Catalog changed during all three export attempts. Retry when stable."
    )


def write_export(
    output: Path, resources: list[ResourceManifest], snapshot: dict[str, Any]
) -> None:
    """Create a new export directory; never overwrite an earlier observation."""
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    output.chmod(0o700)
    write_private(
        output / "catalog.yaml",
        safe_dump_all(
            [resource.model_dump(mode="json") for resource in resources],
            sort_keys=False,
        ),
    )
    write_private(output / "runtime-state.json", json.dumps(snapshot, indent=2) + "\n")
    write_private(
        output / "export.json",
        json.dumps(
            {
                key: snapshot[key]
                for key in (
                    "controller_url",
                    "gateway_url",
                    "config_version",
                    "observed_at",
                    "running_source_revision",
                )
            },
            indent=2,
        )
        + "\n",
    )


def write_private(path: Path, content: str) -> None:
    """Create a private observation/plan without overwriting even a symlink."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as fp:
        os.fchmod(fp.fileno(), 0o600)
        fp.write(content)


def overlay_targets(
    live: list[ResourceManifest],
    proposed: list[ResourceManifest],
    targets: list[str],
) -> list[ResourceManifest]:
    """Replace only selected identities. Dependencies must be selected too."""
    base = _resources_by_id(live)
    source = _resources_by_id(proposed)
    for target in targets:
        if target not in source:
            raise InvalidSpecError(f"Selected resource is absent from input: {target}")
        base[target] = source[target]
    return list(base.values())


def check_target_plan(plan: ResourceChangePlan, targets: list[str]) -> None:
    changed = (
        {f"{r.kind}.{r.name}" for r in plan.to_add}
        | {f"{r.kind}.{r.name}" for r in plan.to_update}
        | {f"{r.kind}.{r.name}" for r in plan.to_delete}
    )
    unexpected = changed - set(targets)
    if plan.to_delete or unexpected:
        raise InvalidSpecError(
            "Scoped plan contains deletions or non-target changes: "
            + ", ".join(sorted(changed if plan.to_delete else unexpected))
        )


def load_saved_plan(path: Path, client: AdminClient) -> SavedPlan:
    saved = SavedPlan.model_validate_json(path.read_text())
    if service_urls(client) != {
        "controller_url": saved.controller_url,
        "gateway_url": saved.gateway_url,
    }:
        raise InvalidSpecError(
            "Saved plan belongs to different service URLs. Select the original "
            "--base-url and --gateway-url or create and review a new plan."
        )
    if saved.targets:
        check_target_plan(saved.approved_plan, saved.targets)
    return saved


def deployment_status(client: AdminClient, name: str) -> dict[str, Any]:
    """Join scheduler, manager and model observations without claiming routing."""
    deployment = client.gateway.catalog.get_pilot_deployment(name)
    cluster = client.gateway.catalog.get_cluster(deployment.cluster_name)
    jobs = {job.name: job for job in cluster.pilot_jobs}
    replicas = []
    for replica in deployment.replicas:
        if replica.deleted_at is not None:
            continue
        job = jobs.get(replica.pilot_job_name or "")
        replicas.append(
            {
                "name": replica.name,
                "uid": replica.uid,
                "state": replica.state.value,
                "message": replica.state_message,
                "pilot": replica.pilot_job_name,
                "scheduler_job_id": job.scheduler_job_id if job else None,
                "scheduler_state": job.scheduler_state.value if job else None,
                "manager_url": job.manager_url if job else None,
                "manager_health": job.manager_health.value if job else None,
                "model_url": replica.model_url,
                "resources": [r.model_dump(mode="json") for r in replica.resources],
                "reconcile_error": replica.reconcile_last_error,
                "pilot_reconcile_error": job.reconcile_last_error if job else None,
                "pilot_failure": job.failure_reason if job else None,
                "logs": f"alcf-ai --base-url {client.gateway.base_url} "
                f"deployments logs {replica.name}",
            }
        )
    return {
        **service_urls(client),
        "deployment": deployment.name,
        "desired_replicas": deployment.desired_replicas,
        "state": deployment.state.value,
        "state_ready_replicas": sum(r["state"] == "ready" for r in replicas),
        "active_replicas": len(replicas),
        "consecutive_launch_failures": deployment.consecutive_launch_failures,
        "max_consecutive_launch_failures": deployment.max_consecutive_launch_failures,
        "capacity": (
            {
                "max_concurrent_jobs": cluster.pilot_system.max_concurrent_jobs,
                "max_num_nodes": cluster.pilot_system.max_num_nodes,
                "max_nodes_per_job": cluster.pilot_system.max_nodes_per_job,
                "catalog_pilot_jobs_including_history": len(cluster.pilot_jobs),
                "pilot_workdir": str(cluster.pilot_system.workdir),
            }
            if cluster.pilot_system is not None
            else None
        ),
        "reconcile_error": deployment.reconcile_last_error,
        "replicas": replicas,
        "routed_requests": "not tested",
        "cleanup": "not independently verified",
        "running_source_revision": "unverified",
        "limitations": [
            "State-ready counts do not prove routability or exclude draining replicas.",
            "Request success and independent process/GPU/socket cleanup are separate checks.",
            "Catalog job counts include history and do not establish free same-rack capacity.",
        ],
    }
