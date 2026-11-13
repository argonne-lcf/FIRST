"""CPU-only admin workflow checks: no database, authentication or scheduler writes."""

import json
import os
import stat
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
from pydantic import BaseModel
from typer.testing import CliRunner

from first_common.errors import InvalidSpecError
from first_common.schema.resources import ResourceChangePlan, ResourceManifest
from first_common.schema.resources import read as read_schema
from first_common.schema.resources.plan_apply import (
    FieldChange,
    ResourcePatch,
    ResourceRef,
)
from first_common.schema.resources.spec import (
    LaunchTemplateSpec,
    ModelSpec,
    PilotDeploymentSpec,
    StaticDeploymentSpec,
)
from first_common.schema.types import (
    MTLSClientConfig,
    ReplicaState,
    ResourceName,
    SecretRef,
)
from first_gateway.admin import cli as cli_module
from first_gateway.admin.cli import load_resources_from_yaml
from first_gateway.admin.workflow import (
    SavedPlan,
    capture_catalog,
    check_target_plan,
    deployment_status,
    load_saved_plan,
    overlay_targets,
    service_urls,
    write_export,
    write_private,
)
from first_gateway.services.plan_apply import validate_resources

runner = CliRunner()


def no_change(
    resources: list[ResourceManifest], version: int = 7
) -> ResourceChangePlan:
    return ResourceChangePlan(
        previous_version=version,
        no_change=[ResourceRef(kind=r.kind, name=r.name) for r in resources],
        to_add=[],
        to_update=[],
        to_delete=[],
    )


@pytest.fixture
def resources() -> list[ResourceManifest]:
    return load_resources_from_yaml(Path(__file__).parent / "resource_specs/baseline")


@pytest.fixture
def client(resources: list[ResourceManifest]) -> MagicMock:
    client = MagicMock()
    client.base_url = httpx.URL("http://localhost:19100")
    client.gateway.base_url = httpx.URL("http://localhost:18000")
    client.gateway.timeout = httpx.Timeout(10)
    client.list_config_versions.return_value = [SimpleNamespace(uid=7)]
    rows: dict[str, list[Any]] = {}
    for i, resource in enumerate(resources):
        cls: type[BaseModel]
        data = {
            **resource.spec.model_dump(mode="json"),
            "kind": resource.kind,
            "name": resource.name,
            "uid": i + 1,
            "created_at": "2026-11-13T00:00:00Z",
        }
        if resource.kind == "Model":
            cls = read_schema.ModelSummary
            data.update(pilot_deployments=[], static_deployments=[])
        elif resource.kind == "Cluster":
            cls = read_schema.ClusterSummary
            data.update(health="healthy")
        elif resource.kind == "StaticDeployment":
            cls = read_schema.StaticDeploymentDetail
            data.update(health="healthy")
        elif resource.kind == "PilotDeployment":
            cls = read_schema.PilotDeploymentDetail
            data.update(
                desired_replicas=8,
                state="healthy",
                consecutive_launch_failures=0,
                replicas=[],
            )
        else:
            cls = getattr(read_schema, resource.kind)
        rows.setdefault(resource.kind, []).append(cls.model_validate(data))
    catalog = client.gateway.catalog
    for kind, method in (
        ("Model", "models"),
        ("Cluster", "clusters"),
        ("AccessGroup", "access_groups"),
        ("LaunchTemplate", "launch_templates"),
        ("StaticDeployment", "static_deployments"),
        ("PilotDeployment", "pilot_deployments"),
    ):
        getattr(catalog, method).return_value = rows[kind]
    catalog.get_pilot_deployment.return_value = rows["PilotDeployment"][0]
    catalog.get_cluster.return_value = read_schema.ClusterDetail.model_validate(
        {**rows["Cluster"][0].model_dump(), "pilot_jobs": []}
    )
    client.plan.side_effect = lambda rs: no_change(rs)
    return client


def test_export_projects_exact_specs_and_separate_manual_counts(
    client: MagicMock, resources: list[ResourceManifest], tmp_path: Path
) -> None:
    exported, snapshot = capture_catalog(client)
    assert {(r.kind, r.name): r.spec.model_dump() for r in exported} == {
        (r.kind, r.name): r.spec.model_dump() for r in resources
    }
    assert snapshot["desired_replicas"] == {"sophia/pilot/llama-3-8b": 8}
    assert all("desired_replicas" not in r.spec.model_dump() for r in exported)
    assert snapshot["runtime_snapshot_atomic"] is False
    assert snapshot["running_source_revision"] == "unverified"
    output = tmp_path / "export"
    write_export(output, exported, snapshot)
    reloaded = load_resources_from_yaml(output)
    assert {r.name for r in reloaded} == {r.name for r in exported}
    assert json.loads((output / "export.json").read_text())["config_version"] == 7
    with pytest.raises(FileExistsError):
        write_export(output, exported, snapshot)


def test_export_retries_changed_version_then_succeeds(client: MagicMock) -> None:
    client.list_config_versions.side_effect = [
        [SimpleNamespace(uid=6)],
        [SimpleNamespace(uid=7)],
        [SimpleNamespace(uid=7)],
        [SimpleNamespace(uid=7)],
    ]
    _, snapshot = capture_catalog(client)
    assert snapshot["config_version"] == 7
    assert client.gateway.catalog.models.call_count == 2


@pytest.mark.parametrize("umask", [0o022, 0o077, 0o777])
def test_export_and_saved_plan_are_private_without_overwriting(
    client: MagicMock,
    tmp_path: Path,
    umask: int,
) -> None:
    resources, snapshot = capture_catalog(client)
    output = tmp_path / "private-export"
    plan = tmp_path / "private-plan.json"
    previous = os.umask(umask)
    try:
        write_export(output, resources, snapshot)
        write_private(plan, "original")
    finally:
        os.umask(previous)
    assert stat.S_IMODE(output.stat().st_mode) == 0o700
    assert all(stat.S_IMODE(p.stat().st_mode) == 0o600 for p in output.iterdir())
    assert stat.S_IMODE(plan.stat().st_mode) == 0o600
    with pytest.raises(FileExistsError):
        write_private(plan, "replacement")
    link = tmp_path / "symlink.json"
    link.symlink_to(plan)
    with pytest.raises(FileExistsError):
        write_private(link, "replacement")
    assert plan.read_text() == "original"


def test_export_does_not_resolve_secret_references(
    client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    upstream = client.gateway.catalog.static_deployments.return_value[0]
    reference = upstream.api_key
    assert isinstance(reference, SecretRef)
    assert reference.startswith("env_var://")
    monkeypatch.setenv(reference.removeprefix("env_var://"), "MUST_NOT_ENTER_EXPORT")
    monkeypatch.setattr(
        SecretRef, "resolve", lambda _: pytest.fail("resolved a secret")
    )
    resources, snapshot = capture_catalog(client)
    serialized = json.dumps([r.model_dump(mode="json") for r in resources])
    assert reference in serialized
    assert "MUST_NOT_ENTER_EXPORT" not in serialized + json.dumps(snapshot)


def test_json_catalog_and_saved_plan_keep_native_values_until_resolution(
    client: MagicMock,
) -> None:
    configuration = {"arguments": ["--flag", "Unicode 👍🏽"], "enabled": True}
    catalog = client.gateway.catalog
    old_template = catalog.launch_templates.return_value[0]
    template = LaunchTemplateSpec.model_validate(
        {
            **old_template.model_dump(),
            "parameters": {"configuration": {"type": "json", "required": True}},
            "serve_script_template": "serve {{ parameters.configuration | quote }}",
        }
    )
    catalog.launch_templates.return_value = [
        read_schema.LaunchTemplate.model_validate(
            {**old_template.model_dump(), **template.model_dump()}
        )
    ]
    deployment = catalog.get_pilot_deployment.return_value
    deployment.launch_spec.parameters = {"configuration": configuration}
    resources, _ = capture_catalog(client)
    saved = SavedPlan(
        **service_urls(client),
        resources=resources,
        approved_plan=no_change(resources),
        targets=[],
    )
    restored = SavedPlan.model_validate_json(saved.model_dump_json())
    resource = next(r for r in restored.resources if r.kind == "PilotDeployment")
    assert isinstance(resource.spec, PilotDeploymentSpec)
    assert resource.spec.launch_spec.parameters["configuration"] == configuration
    resolved = template.resolve(resource.spec.launch_spec, 1048576)
    assert isinstance(resolved.parameters["configuration"], str)
    assert json.loads(resolved.parameters["configuration"]) == configuration


def test_export_is_bounded_and_rejects_wrong_service_pair(client: MagicMock) -> None:
    client.list_config_versions.side_effect = [
        [SimpleNamespace(uid=n)] for n in range(6)
    ]
    with pytest.raises(InvalidSpecError, match="three export attempts"):
        capture_catalog(client)
    assert client.list_config_versions.call_count == 6
    client.list_config_versions.side_effect = None
    client.plan.side_effect = None
    client.plan.return_value = ResourceChangePlan(
        previous_version=7,
        no_change=[],
        to_add=[],
        to_update=[],
        to_delete=[ResourceRef(kind="Model", name="unrelated")],
    )
    with pytest.raises(InvalidSpecError, match="Gateway catalog differs"):
        capture_catalog(client)


def test_target_overlay_preserves_unrelated_and_never_imports_dependencies(
    resources: list[ResourceManifest],
) -> None:
    proposed = [r.model_copy(deep=True) for r in resources]
    model = next(r for r in proposed if r.kind == "Model")
    assert isinstance(model.spec, ModelSpec)
    model.spec.display_name = "New display name"
    target = f"{model.kind}.{model.name}"
    deployment = next(r for r in proposed if r.kind == "PilotDeployment")
    assert isinstance(deployment.spec, PilotDeploymentSpec)
    deployment.spec.router_params.max_backend_concurrency = 99
    result = overlay_targets(resources, proposed, [target])
    assert next(r for r in result if r.kind == "Model").spec == model.spec
    assert (
        next(r for r in result if r.kind == "PilotDeployment").spec != deployment.spec
    )
    model.spec.access_group_name = ResourceName("missing")
    with pytest.raises(InvalidSpecError, match="nonexistant AccessGroup"):
        validate_resources(overlay_targets(resources, proposed, [target]))
    with pytest.raises(InvalidSpecError, match="absent"):
        overlay_targets(resources, proposed, ["Model.not-in-input"])
    with pytest.raises(InvalidSpecError, match="Duplicate"):
        overlay_targets(resources, [model, model], [target])


def test_offline_validation_skips_only_vm_certificate_files(
    resources: list[ResourceManifest],
    tmp_path: Path,
) -> None:
    static = next(r.spec for r in resources if r.kind == "StaticDeployment")
    assert isinstance(static, StaticDeploymentSpec)
    static.mtls = MTLSClientConfig(
        ca_file=str(tmp_path / "vm-only/ca.crt"),
        cert_file=str(tmp_path / "vm-only/client.crt"),
        key_file=str(tmp_path / "vm-only/client.key"),
    )
    with pytest.raises(InvalidSpecError, match="Could not load mTLS"):
        validate_resources(resources)
    assert validate_resources(resources, check_mtls_files=False)["StaticDeployment"]
    # The explicit offline call does not change the server/default behavior.
    with pytest.raises(InvalidSpecError, match="Could not load mTLS"):
        validate_resources(resources)
    with pytest.raises(InvalidSpecError, match="LaunchTemplate"):
        validate_resources(
            [r for r in resources if r.kind != "LaunchTemplate"], check_mtls_files=False
        )
    deployment = next(r.spec for r in resources if r.kind == "PilotDeployment")
    assert isinstance(deployment, PilotDeploymentSpec)
    deployment.launch_spec.parameters["unexpected"] = 1
    with pytest.raises(InvalidSpecError, match="unknown parameters"):
        validate_resources(resources, check_mtls_files=False)


def test_target_plan_rejects_deletion_and_other_updates() -> None:
    plan = no_change([])
    plan.to_update = [
        ResourcePatch(
            kind="Model", name="other", patch={"display_name": FieldChange("x", "y")}
        )
    ]
    with pytest.raises(InvalidSpecError, match="non-target changes"):
        check_target_plan(plan, ["Model.target"])
    plan.to_update = []
    plan.to_delete = [ResourceRef(kind="Model", name="target")]
    with pytest.raises(InvalidSpecError, match="deletions"):
        check_target_plan(plan, ["Model.target"])


def test_scoped_cli_saves_complete_payload_and_ignores_unselected_edits(
    client: MagicMock,
    resources: list[ResourceManifest],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    proposed = [r.model_copy(deep=True) for r in resources]
    model = next(r for r in proposed if r.kind == "Model")
    assert isinstance(model.spec, ModelSpec)
    model.spec.display_name = "Reviewed model"
    deployment = next(r for r in proposed if r.kind == "PilotDeployment")
    assert isinstance(deployment.spec, PilotDeploymentSpec)
    deployment.spec.router_params.max_backend_concurrency = 99
    source = tmp_path / "source"
    write_export(
        source,
        proposed,
        {
            **service_urls(client),
            "config_version": 7,
            "observed_at": "now",
            "running_source_revision": "unverified",
        },
    )
    passed_urls = []

    def admin_factory(controller: str, gateway: str) -> MagicMock:
        passed_urls.append((controller, gateway))
        return client

    monkeypatch.setattr(cli_module, "AdminClient", admin_factory)
    output = tmp_path / "reviewed.json"
    result = runner.invoke(
        cli_module.cli,
        [
            "--base-url",
            "http://localhost:19100",
            "--gateway-url",
            "http://localhost:18000",
            "plan",
            str(source),
            "--only",
            f"Model.{model.name}",
            "--out",
            str(output),
        ],
    )
    assert result.exit_code == 0, result.output
    assert passed_urls == [("http://localhost:19100", "http://localhost:18000")]
    saved = SavedPlan.model_validate_json(output.read_text())
    assert stat.S_IMODE(output.stat().st_mode) == 0o600
    assert len(saved.resources) == len(resources)
    assert saved.targets == [f"Model.{model.name}"]
    assert next(r for r in saved.resources if r.kind == "Model").spec == model.spec
    assert (
        next(r for r in saved.resources if r.kind == "PilotDeployment").spec
        == next(r for r in resources if r.kind == "PilotDeployment").spec
    )
    assert "Dependent deployments" in result.output
    assert "Manual desired counts preserved" in result.output
    client.apply.assert_not_called()


def test_saved_plan_url_binding_and_exact_apply_without_replan(
    client: MagicMock,
    resources: list[ResourceManifest],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = no_change(resources)
    plan.to_update = [
        ResourcePatch(
            kind="Model",
            name="meta-llama/llama-3-8b",
            patch={"display_name": FieldChange(None, "Llama")},
        )
    ]
    saved = SavedPlan(
        **service_urls(client), resources=resources, approved_plan=plan, targets=[]
    )
    path = tmp_path / "plan.json"
    path.write_text(saved.model_dump_json())
    assert load_saved_plan(path, client) == saved
    client.gateway.base_url = httpx.URL("http://localhost:8000")
    with pytest.raises(InvalidSpecError, match="different service URLs"):
        load_saved_plan(path, client)
    client.gateway.base_url = httpx.URL(saved.gateway_url)
    controller = client.base_url
    client.base_url = httpx.URL("http://localhost:9100")
    with pytest.raises(InvalidSpecError, match="different service URLs"):
        load_saved_plan(path, client)
    client.base_url = controller
    monkeypatch.setattr(cli_module, "AdminClient", lambda *_: client)
    client.apply.return_value = SimpleNamespace(uid=8)
    result = runner.invoke(cli_module.cli, ["apply", "--plan", str(path)], input="y\n")
    assert result.exit_code == 0, result.output
    client.apply.assert_called_once_with(saved.resources, saved.approved_plan)
    client.plan.assert_not_called()
    client.apply.side_effect = InvalidSpecError("Stale configuration version")
    result = runner.invoke(cli_module.cli, ["apply", "--plan", str(path)], input="y\n")
    assert result.exit_code != 0
    client.plan.assert_not_called()


def test_status_links_real_replica_pilot_and_scheduler(client: MagicMock) -> None:
    catalog = client.gateway.catalog
    deployment = catalog.get_pilot_deployment.return_value
    replica = read_schema.PilotReplica.model_validate(
        {
            "name": f"{deployment.name}/replica/test",
            "uid": 20,
            "created_at": "2026-11-13T00:00:00Z",
            "pilot_deployment_name": deployment.name,
            "pilot_job_name": "pilot-test",
            "claimed_gpu_ids": [[0, 0]],
            "resources": [{"hostname": "node0", "gpu_ids": ["0"]}],
            "model_url": "https://10.0.0.1:8443/replicas/test/",
            "state": "launching",
            "state_message": "Loading weights",
        }
    )
    deployment.replicas = [replica]
    cluster = catalog.get_cluster.return_value
    job = read_schema.PilotJob.model_validate(
        {
            "name": "pilot-test",
            "uid": 21,
            "created_at": "2026-11-13T00:00:00Z",
            "scheduler_job_id": "123.pbs",
            "cluster_name": cluster.name,
            "scheduler_state": "running",
            "manager_url": "https://10.0.0.1:8443/control",
            "manager_health": "healthy",
            "resources": {},
            "claimed_gpu_ids": [[0, 0]],
            "assigned_replicas": [replica],
            "walltime_min": 60,
            "num_nodes": 1,
            "gpus_per_node": 8,
        }
    )
    cluster.pilot_jobs = [job]
    observation = deployment_status(client, deployment.name)
    assert observation["state_ready_replicas"] == 0
    assert observation["replicas"][0]["scheduler_job_id"] == "123.pbs"
    assert observation["replicas"][0]["manager_health"] == "healthy"
    assert replica.name in observation["replicas"][0]["logs"]
    replica.state = ReplicaState.ready
    observation = deployment_status(client, deployment.name)
    assert observation["state_ready_replicas"] == 1
    assert observation["routed_requests"] == "not tested"


@pytest.mark.parametrize("observed_ready", [0, 1])
def test_status_join_and_bounded_ready_wait(
    client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    observed_ready: int,
) -> None:
    observation = deployment_status(client, "sophia/pilot/llama-3-8b")
    assert observation["desired_replicas"] == 8
    assert observation["state_ready_replicas"] == 0
    assert observation["routed_requests"] == "not tested"
    monkeypatch.setattr(cli_module, "AdminClient", lambda *_: client)
    monkeypatch.setattr(
        cli_module,
        "deployment_status",
        lambda *_: {**observation, "state_ready_replicas": observed_ready},
    )
    values = iter([0.0, 0.1, 1.0])
    monkeypatch.setattr(time, "monotonic", lambda: next(values))
    result = runner.invoke(
        cli_module.cli,
        [
            "status",
            "sophia/pilot/llama-3-8b",
            "--wait-ready",
            "1",
            "--timeout",
            "0.5",
            "--json",
        ],
    )
    assert result.exit_code == 1, result.output
    assert json.loads(result.stdout)["wait_timed_out"] is True


def test_status_wait_success_and_auth_failure_do_not_create_jobs(
    client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli_module, "AdminClient", lambda *_: client)
    monkeypatch.setattr(
        cli_module, "deployment_status", lambda *_: {"state_ready_replicas": 1}
    )
    result = runner.invoke(
        cli_module.cli, ["status", "test", "--wait-ready", "1", "--json"]
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["wait_timed_out"] is False
    client.gateway.catalog.models.side_effect = httpx.HTTPStatusError(
        "Unauthorized",
        request=httpx.Request("GET", "http://test"),
        response=httpx.Response(401),
    )
    with pytest.raises(httpx.HTTPStatusError):
        capture_catalog(client)
    client.apply.assert_not_called()
    client.set_desired_pilot_deployment_replicas.assert_not_called()
