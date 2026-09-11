"""Launch templates: validation, resolution, plan/apply, launcher and pilot rendering."""

from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from first_common.errors import InvalidSpecError
from first_common.schema.pilot import ReplicaStartRequest
from first_common.schema.resources import ResourceManifest
from first_common.schema.resources.spec import (
    AccessGroupSpec,
    LaunchSpec,
    LaunchTemplateSpec,
    ModelSpec,
    PilotDeploymentSpec,
    ScriptParameter,
)
from first_common.schema.types import GpuClaim
from first_gateway.controllers.workers.router_config_observer import (
    RouterConfigObserver,
)
from first_gateway.database.models import LaunchTemplate, Model, PilotDeployment
from first_gateway.services.plan_apply import validate_resources
from first_pilot.replica import Replica

from . import test_replica_launcher as launcher_tests
from .fixtures.auth import ADMIN_TOKEN, USER_TOKEN, auth_header
from .test_resource_apply import RESOURCES_DIR, _apply, _load, _plan

MAX_MODEL_LEN = 262144


def _template(**overrides: Any) -> LaunchTemplateSpec:
    values = {
        "parameters": {
            "weights_path": {"type": "str", "required": True},
            "context_window": {"type": "int", "default": 65536, "minimum": 1},
        },
        "env": {"COMMON": "1", "OVERRIDE": "template"},
        "serve_script_template": (
            "serve {{ parameters.weights_path | quote }}"
            " --uds {{ runtime.uds | quote }}"
            " --max-model-len {{ runtime.max_model_len }}"
            " --window {{ parameters.context_window }}"
        ),
        "pre_stop_script_template": "stop {{ quote(runtime.replica_name) }}",
        "post_stop_script_template": "verify {{ runtime.num_nodes }}",
        "max_startup_sec": 300,
        "health_check": {"url": "http://localhost/health"},
    }
    values.update(overrides)
    return LaunchTemplateSpec.model_validate(values)


def _launch(**overrides: Any) -> LaunchSpec:
    values = {
        "served_model_name": "inkling",
        "gpus_per_node": 4,
        "num_nodes": 8,
        "parameters": {"weights_path": "/weights/model's name"},
    }
    values.update(overrides)
    return LaunchSpec.model_validate(values)


def _resources() -> list[ResourceManifest]:
    """Baseline manifests using the template, launch spec and Model facts above."""
    resources = _load("baseline")
    for r in resources:
        if r.kind == "AccessGroup":
            # Open access so the user-scoped per-cluster models view lists the model.
            assert isinstance(r.spec, AccessGroupSpec)
            r.spec.allowed_groups = []
            r.spec.allowed_domains = []
        elif r.kind == "LaunchTemplate":
            r.spec = _template()
        elif r.kind == "PilotDeployment":
            assert isinstance(r.spec, PilotDeploymentSpec)
            r.spec.launch_spec = _launch()
        elif r.kind == "Model":
            assert isinstance(r.spec, ModelSpec)
            r.spec.max_model_len = MAX_MODEL_LEN
            r.spec.capabilities = {"tool_calling": True}
    return resources


def test_resolve_merges_template_and_deployment() -> None:
    template = _template()
    launch = _launch(
        max_startup_sec=900,
        env={"OVERRIDE": "deployment"},
        parameters={"weights_path": "/w", "context_window": None},
    )
    resolved = template.resolve(launch, MAX_MODEL_LEN)
    assert resolved.max_startup_sec == 900
    assert resolved.pre_stop_timeout_sec == 20
    assert resolved.max_model_len == MAX_MODEL_LEN
    assert resolved.health_check == template.health_check
    assert resolved.env == {"COMMON": "1", "OVERRIDE": "deployment"}
    assert resolved.serve_script_template == template.serve_script_template
    # An explicit null inherits the template default, exactly like an omitted key.
    assert resolved.parameters == {"weights_path": "/w", "context_window": 65536}
    assert template.env["OVERRIDE"] == "template"


@pytest.mark.parametrize(
    "parameters,match",
    [
        ({}, "required"),
        ({"weights_path": None}, "required"),
        ({"weights_path": 1}, "must be str"),
        ({"weights_path": "/w", "unexpected": 1}, "unknown"),
        ({"weights_path": "/w", "context_window": "65536"}, "must be int"),
        ({"weights_path": "/w", "context_window": 1.5}, "must be int"),
        ({"weights_path": "/w", "context_window": 0}, ">= 1"),
    ],
)
def test_invalid_parameters(parameters: dict[str, Any], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        _template().resolve(_launch(parameters=parameters), None)


def test_launch_spec_is_strict() -> None:
    with pytest.raises(ValidationError):
        _launch(parameters={"weights_path": "/w", "context_window": True})
    with pytest.raises(ValidationError):
        _launch(serve_script_template="unsafe")
    with pytest.raises(ValidationError):
        _launch(pre_stop_timeout_sec=26)


@pytest.mark.parametrize(
    "definition",
    [
        {"type": "path"},
        {"type": "int", "default": "5"},
        {"type": "int", "default": True},
        {"type": "int", "default": 0, "minimum": 1},
        {"type": "str", "default": 5},
    ],
)
def test_invalid_parameter_definitions(definition: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        ScriptParameter.model_validate(definition)


def test_reserved_parameter_names_rejected() -> None:
    with pytest.raises(ValidationError, match="reserved"):
        _template(parameters={"values": {"type": "str"}}, serve_script_template="x")


@pytest.mark.parametrize(
    "template",
    [
        "{{ parameters.typo }}",
        "{{ parameters['typo'] }}",
        "{{ runtime.typo }}",
        "{{ uds }}",
        "{{ weights_path }}",
        "{% set p = parameters %}{{ p.typo }}",
        "{{ parameters.weights_path",
        "{{ runtime.uds | nofilter }}",
    ],
)
@pytest.mark.parametrize(
    "field",
    ["serve_script_template", "pre_stop_script_template", "post_stop_script_template"],
)
def test_bad_templates_rejected_at_validation(template: str, field: str) -> None:
    with pytest.raises(ValidationError, match=field):
        _template(**{field: template})


@pytest.mark.parametrize(
    "template",
    [
        "{% for k, v in parameters.items() %}{{ k }}={{ v }}{% endfor %}",
        "{{ runtime.gpus_by_host.values() | first | join(',') }}",
        "{{ quote(runtime.uds) }} {{ runtime.max_model_len }}",
    ],
)
def test_good_templates_accepted(template: str) -> None:
    _template(serve_script_template=template)


def test_deployment_spec_requires_template_name() -> None:
    values = {
        "cluster_name": "tara",
        "model_name": "inkling",
        "launch_spec": _launch().model_dump(),
    }
    with pytest.raises(ValidationError, match="launch_template_name"):
        PilotDeploymentSpec.model_validate(values)
    PilotDeploymentSpec.model_validate({**values, "launch_template_name": "vllm"})


def test_pilot_renders_runtime_and_parameters() -> None:
    replica = Replica.__new__(Replica)
    replica.name = "tara/inkling/replica/one"
    replica.uds = "/tmp/first sock"
    replica.resources = [GpuClaim(hostname="node1", gpu_ids=["0", "1"])]
    replica.launch_spec = _template().resolve(_launch(), MAX_MODEL_LEN)
    assert replica._render_script(replica.launch_spec.serve_script_template) == (
        "serve '/weights/model'\"'\"'s name' --uds '/tmp/first sock'"
        " --max-model-len 262144 --window 65536"
    )
    assert (
        replica._render_script("{{ runtime.gpus_by_host.node1 | join(',') }}") == "0,1"
    )
    assert replica.launch_spec.pre_stop_script_template is not None
    assert (
        replica._render_script(replica.launch_spec.pre_stop_script_template)
        == "stop tara/inkling/replica/one"
    )
    assert replica.launch_spec.post_stop_script_template is not None
    assert (
        replica._render_script(replica.launch_spec.post_stop_script_template)
        == "verify 8"
    )
    with pytest.raises(ValueError, match="failed to render"):
        replica._render_script("{{ parameters.missing }}")


def test_validate_resources_checks_references_and_resolution() -> None:
    resources = _resources()
    validate_resources(resources)

    with pytest.raises(InvalidSpecError, match="LaunchTemplate.vllm"):
        validate_resources([r for r in resources if r.kind != "LaunchTemplate"])

    deployment = next(r.spec for r in resources if r.kind == "PilotDeployment")
    assert isinstance(deployment, PilotDeploymentSpec)
    deployment.launch_spec.parameters["context_window"] = 0
    with pytest.raises(InvalidSpecError, match="context_window"):
        validate_resources(resources)

    # A template edit is checked against every deployment that references it.
    deployment.launch_spec.parameters["context_window"] = 65536
    template = next(r.spec for r in resources if r.kind == "LaunchTemplate")
    assert isinstance(template, LaunchTemplateSpec)
    template.parameters["context_window"].minimum = 100000
    template.parameters["context_window"].default = 131072
    with pytest.raises(InvalidSpecError, match="context_window"):
        validate_resources(resources)


def test_all_fixture_manifests_validate() -> None:
    for directory in RESOURCES_DIR.iterdir():
        if directory.is_dir() and directory.name not in {"duplicates", "invalid_ref"}:
            validate_resources(_load(directory.name))


async def test_plan_apply_roundtrip_and_catalog_views(
    control_client: httpx.AsyncClient,
    client: httpx.AsyncClient,
    db: async_sessionmaker[AsyncSession],
) -> None:
    resources = _resources()
    deployment = next(r for r in resources if r.kind == "PilotDeployment")
    assert isinstance(deployment.spec, PilotDeploymentSpec)
    deployment.spec.launch_spec.parameters["context_window"] = 65536
    await _apply(control_client, resources, await _plan(control_client, resources))
    unchanged = await _plan(control_client, resources)
    assert not unchanged.to_update and not unchanged.to_add
    assert len(unchanged.no_change) == 6

    response = await client.get(
        "/catalog/v1/launch-templates", headers=auth_header(ADMIN_TOKEN)
    )
    assert response.status_code == 200, response.text
    [template_row] = response.json()
    assert template_row["name"] == "vllm"
    assert template_row["parameters"]["context_window"]["default"] == 65536
    response = await client.get(
        "/catalog/v1/launch-templates", headers=auth_header(USER_TOKEN)
    )
    assert response.status_code == 403

    # Model facts reach the catalog, the per-cluster view and the router config.
    for endpoint in ("/catalog/v1/models", "/resource_server/sophia/models"):
        response = await client.get(endpoint, headers=auth_header(ADMIN_TOKEN))
        assert response.status_code == 200, response.text
        [model] = response.json()
        assert model["max_model_len"] == MAX_MODEL_LEN
        assert model["capabilities"] == {"tool_calling": True}
    client_state = MagicMock()
    client_state.db_sessionmaker = db
    observer = RouterConfigObserver("router-cfg", client_state, MagicMock())
    [config] = await observer.rebuild()
    assert config.max_model_len == MAX_MODEL_LEN
    assert config.capabilities == {"tool_calling": True}

    response = await client.get(
        f"/catalog/v1/deployments/pilot/{deployment.name}",
        headers=auth_header(ADMIN_TOKEN),
    )
    assert response.status_code == 200, response.text
    assert response.json()["launch_template_name"] == "vllm"
    assert response.json()["launch_spec"]["parameters"] == {
        "weights_path": "/weights/model's name",
        "context_window": 65536,
    }

    # A template edit that breaks a dependent deployment is rejected at plan time.
    template = next(r.spec for r in resources if r.kind == "LaunchTemplate")
    assert isinstance(template, LaunchTemplateSpec)
    template.parameters["context_window"].minimum = 100000
    template.parameters["context_window"].default = 131072
    response = await control_client.post(
        "/control/v1/plan",
        headers=auth_header(ADMIN_TOKEN),
        json={"resources": [r.model_dump(mode="json") for r in resources]},
    )
    assert response.status_code == 400, response.text

    # So is removing a template that a deployment still references.
    remaining = [r for r in resources if r.kind != "LaunchTemplate"]
    response = await control_client.post(
        "/control/v1/plan",
        headers=auth_header(ADMIN_TOKEN),
        json={"resources": [r.model_dump(mode="json") for r in remaining]},
    )
    assert response.status_code == 400, response.text


async def test_launcher_sends_resolved_spec_to_pilot(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as session:
        await launcher_tests._seed_parents(session)
        await launcher_tests._insert_deployment(session)
        await launcher_tests._insert_job(session)
        uid = await launcher_tests._insert_replica(session)
        session.add(LaunchTemplate(name="vllm", **_template().model_dump(mode="json")))
        model = await Model.get_by_name(session, "llama")
        model.max_model_len = MAX_MODEL_LEN
        deployment = await PilotDeployment.get_by_name(session, "deploy-1")
        deployment.launch_template_name = "vllm"
        deployment.launch_spec = _launch(num_nodes=1, max_startup_sec=123).model_dump(
            mode="json"
        )

    seen: list[ReplicaStartRequest] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(ReplicaStartRequest.model_validate_json(request.read()))
        return httpx.Response(200)

    controller = launcher_tests._make_controller(db, handler)
    try:
        await controller.reconcile(uid)
    finally:
        await controller.client._client.aclose()

    [request] = seen
    resolved = request.launch_spec
    assert resolved.parameters == {
        "weights_path": "/weights/model's name",
        "context_window": 65536,
    }
    assert resolved.max_model_len == MAX_MODEL_LEN
    assert resolved.max_startup_sec == 123
    assert resolved.env == {"COMMON": "1", "OVERRIDE": "template"}
    assert resolved.serve_script_template == _template().serve_script_template
