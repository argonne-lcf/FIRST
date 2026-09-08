"""Launch profiles validate without GPUs, shell execution or service credentials."""

from pathlib import Path
from typing import Any

import httpx
import pytest
from alembic import command as alembic_command
from alembic.config import Config as AlembicConfig
from jinja2 import UndefinedError
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from first_common.errors import InvalidSpecError
from first_common.schema.launch_profile import (
    LaunchProfile,
    ProfileLaunchSpec,
    ScriptParameter,
)
from first_common.schema.pilot import ReplicaStartRequest
from first_common.schema.resources import ResourceManifest
from first_common.schema.resources.spec import (
    AccessGroupSpec,
    LaunchProfileSpec,
    PilotDeploymentSpec,
)
from first_common.schema.types import GpuClaim
from first_gateway import Settings
from first_gateway.database import models as db
from first_gateway.database.models import LaunchProfile as ProfileRow
from first_gateway.database.models import PilotDeployment as DeploymentRow
from first_gateway.services.plan_apply import validate_resources
from first_pilot.replica import Replica

from . import test_replica_launcher as launcher_tests
from .fixtures.auth import ADMIN_TOKEN, USER_TOKEN, auth_header
from .fixtures.db import ALEMBIC_INI
from .test_resource_apply import _apply, _load, _plan


def _profile(**overrides: Any) -> LaunchProfile:
    values = {
        "parameters": {
            "weights_path": {"type": "path", "required": True},
            "context_window": {
                "type": "integer",
                "default": 65536,
                "minimum": 1,
                "capability": "max_context_length",
            },
        },
        "env": {"COMMON": "1", "OVERRIDE": "profile"},
        "serve_script_template": "serve {{ parameters.weights_path | quote }} --uds {{ runtime.uds | quote }} --max-model-len {{ parameters.context_window }}",
        "pre_stop_script_template": "stop {{ quote(runtime.replica_name) }}",
        "post_stop_script_template": "verify {{ runtime.num_nodes }}",
        "max_startup_sec": 300,
        "health_check": {"url": "http://localhost/health"},
    }
    values.update(overrides)
    return LaunchProfile.model_validate(values)


def _launch(**overrides: Any) -> ProfileLaunchSpec:
    values = {
        "served_model_name": "inkling",
        "gpus_per_node": 4,
        "num_nodes": 8,
        "parameters": {"weights_path": "/weights/model's name"},
    }
    values.update(overrides)
    return ProfileLaunchSpec.model_validate(values)


def _resources() -> list[ResourceManifest]:
    resources = _load("baseline")
    profile = ResourceManifest.model_validate(
        {
            "kind": "LaunchProfile",
            "name": "vllm",
            "spec": _profile().model_dump(),
        }
    )
    deployment = next(r for r in resources if r.kind == "PilotDeployment")
    values = deployment.spec.model_dump()
    values.update(launch_profile_name="vllm", launch_spec=_launch().model_dump())
    deployment.spec = PilotDeploymentSpec.model_validate(values)
    return resources + [profile]


def test_resolve_defaults_overrides_env_and_capabilities() -> None:
    profile = _profile()
    launch = _launch(max_startup_sec=900, env={"OVERRIDE": "deployment"})
    resolved = profile.resolve(launch)
    assert resolved.max_startup_sec == 900
    assert resolved.pre_stop_timeout_sec == 20
    assert resolved.health_check == profile.health_check
    assert resolved.env == {"COMMON": "1", "OVERRIDE": "deployment"}
    assert resolved.parameters["context_window"] == 65536
    assert profile.env["OVERRIDE"] == "profile"
    assert profile.get_capabilities(launch) == {"max_context_length": 65536}


def test_required_with_default_and_optional_null() -> None:
    profile = _profile(
        parameters={
            "weights_path": {
                "type": "path",
                "required": True,
                "default": "/weights/default",
            },
            "context_window": {"type": "int", "required": False},
        }
    )
    assert profile.resolve_parameters({}) == {
        "weights_path": "/weights/default",
        "context_window": None,
    }
    with pytest.raises(ValueError, match="required"):
        profile.resolve_parameters({"weights_path": None})
    with pytest.raises(ValidationError):
        _launch(parameters={"weights_path": "/weights", "context_window": True})


@pytest.mark.parametrize(
    "parameters,match",
    [
        ({}, "weights_path"),
        ({"weights_path": None}, "required"),
        ({"weights_path": ""}, "empty"),
        ({"weights_path": "/weights", "unexpected": 1}, "unknown"),
        ({"weights_path": "/weights", "context_window": "65536"}, "finite"),
        ({"weights_path": "/weights", "context_window": 1.5}, "finite"),
        ({"weights_path": "/weights", "context_window": 0}, ">="),
    ],
)
def test_invalid_parameters(parameters: dict[str, Any], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        _profile().resolve(_launch(parameters=parameters))


@pytest.mark.parametrize(
    "definition",
    [
        {"type": "int", "default": True},
        {"type": "int", "default": "5"},
        {"type": "float", "default": float("inf")},
        {"type": "int", "minimum": 5, "maximum": 4},
        {"type": "str", "minimum": 5},
        {"type": "int", "max_length": 4},
        {"type": "str", "min_length": 3, "default": "ab"},
        {"type": "str", "default": "bad\x00path"},
    ],
)
def test_invalid_parameter_definitions(definition: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        ScriptParameter.model_validate(definition)


@pytest.mark.parametrize(
    "template",
    [
        "{{ parameters.typo }}",
        "{{ parameters['typo'] }}",
        "{{ runtime.typo }}",
        "{{ uds }}",
        "{{ parameters.weights_path",
    ],
)
@pytest.mark.parametrize(
    "field",
    ["serve_script_template", "pre_stop_script_template", "post_stop_script_template"],
)
def test_all_templates_validated(template: str, field: str) -> None:
    with pytest.raises(ValidationError):
        _profile(**{field: template})


def test_profile_deployment_cannot_override_templates() -> None:
    with pytest.raises(ValidationError):
        _launch(serve_script_template="unsafe")
    with pytest.raises(ValidationError, match="inline launch_spec"):
        PilotDeploymentSpec.model_validate(
            {
                "cluster_name": "tara",
                "model_name": "inkling",
                "launch_spec": _launch(),
            }
        )


def test_render_runtime_namespace_and_quote_without_running_shell() -> None:
    replica = Replica.__new__(Replica)
    replica.name = "tara/inkling/replica/one"
    replica.uds = "/tmp/first sock"
    replica.resources = [GpuClaim(hostname="node1", gpu_ids=["0", "1"])]
    replica.launch_spec = _profile().resolve(_launch())
    rendered = replica._render_script(replica.launch_spec.serve_script_template)
    assert replica.launch_spec.pre_stop_script_template is not None
    assert replica.launch_spec.post_stop_script_template is not None
    assert (
        rendered
        == "serve '/weights/model'\"'\"'s name' --uds '/tmp/first sock' --max-model-len 65536"
    )
    assert (
        replica._render_script("{{ runtime.gpus_by_host.node1 | join(',') }}") == "0,1"
    )
    assert (
        replica._render_script(replica.launch_spec.pre_stop_script_template)
        == "stop tara/inkling/replica/one"
    )
    assert (
        replica._render_script(replica.launch_spec.post_stop_script_template)
        == "verify 8"
    )
    with pytest.raises(UndefinedError):
        replica._render_script("{{ parameters.missing }}")


def test_complete_manifest_validation_rejects_missing_profile_and_invalid_inputs() -> (
    None
):
    resources = _resources()
    validate_resources(resources)
    with pytest.raises(InvalidSpecError, match="LaunchProfile.vllm"):
        validate_resources(resources[:-1])
    deployment = next(r for r in resources if r.kind == "PilotDeployment")
    assert isinstance(deployment.spec, PilotDeploymentSpec)
    deployment.spec.launch_spec.parameters["context_window"] = 0
    with pytest.raises(InvalidSpecError, match="context_window"):
        validate_resources(resources)


def test_model_capabilities_are_conservative() -> None:
    profile = db.LaunchProfile(name="vllm", **_profile().model_dump(mode="json"))

    def deployment(length: int) -> db.PilotDeployment:
        return db.PilotDeployment(
            launch_profile_name="vllm",
            launch_profile=profile,
            launch_spec=_launch(
                parameters={"weights_path": "/weights", "context_window": length}
            ).model_dump(mode="json"),
        )

    first, second = deployment(65536), deployment(65536)
    model = db.Model(
        pilot_deployments=[first, second],
        static_deployments=[],
        capabilities={"explicit": "value"},
    )
    assert model.get_capabilities() == {
        "max_context_length": 65536,
        "explicit": "value",
    }
    second.launch_spec["parameters"]["context_window"] = 1024
    assert model.get_capabilities() == {"explicit": "value"}
    assert first.resolve_launch_spec().num_nodes == 8
    model.static_deployments = [db.StaticDeployment()]
    assert model.get_capabilities() == {"explicit": "value"}


async def test_profile_plan_apply_roundtrip_and_catalog(
    control_client: httpx.AsyncClient,
    client: httpx.AsyncClient,
) -> None:
    resources = _resources()
    deployment = next(r for r in resources if r.kind == "PilotDeployment")
    assert isinstance(deployment.spec, PilotDeploymentSpec)
    deployment.spec.launch_spec.parameters["context_window"] = 65536
    plan = await _plan(control_client, resources)
    await _apply(control_client, resources, plan)
    unchanged = await _plan(control_client, resources)
    assert not unchanged.to_update and not unchanged.to_add
    assert len(unchanged.no_change) == 6

    response = await client.get(
        "/catalog/v1/launch-profiles", headers=auth_header(ADMIN_TOKEN)
    )
    assert response.status_code == 200, response.text
    assert response.json()[0]["name"] == "vllm"
    response = await client.get(
        "/catalog/v1/launch-profiles", headers=auth_header(USER_TOKEN)
    )
    assert response.status_code == 403
    response = await client.get(
        "/catalog/v1/deployments/pilot", headers=auth_header(ADMIN_TOKEN)
    )
    assert response.status_code == 200, response.text
    assert response.json()[0]["capabilities"] == {"max_context_length": 65536}
    for scale in (1, 0):
        response = await control_client.put(
            f"/control/v1/deployments/pilot/{deployment.name}/desired-replicas",
            headers=auth_header(ADMIN_TOKEN),
            json={"num_replicas": scale},
        )
        assert response.status_code == 200, response.text
        assert response.json()["desired_replicas"] == scale
        assert response.json()["capabilities"] == {"max_context_length": 65536}

    # A model with only agreeing profile deployments can advertise capabilities.
    resources = [r for r in resources if r.kind != "StaticDeployment"]
    group = next(r.spec for r in resources if r.kind == "AccessGroup")
    assert isinstance(group, AccessGroupSpec)
    group.allowed_groups = []
    group.allowed_domains = []
    await _apply(control_client, resources, await _plan(control_client, resources))
    for endpoint in ("/catalog/v1/models", "/resource_server/sophia/models"):
        response = await client.get(endpoint, headers=auth_header(ADMIN_TOKEN))
        assert response.status_code == 200, response.text
        assert response.json()[0]["capabilities"] == {"max_context_length": 65536}

    # Profile edits revalidate every referencing deployment, even unchanged ones.
    profile = resources[-1].spec
    assert isinstance(profile, LaunchProfileSpec)
    profile.parameters["context_window"].minimum = 100000
    profile.parameters["context_window"].default = 131072
    response = await control_client.post(
        "/control/v1/plan",
        headers=auth_header(ADMIN_TOKEN),
        json={"resources": [r.model_dump(mode="json") for r in resources]},
    )
    assert response.status_code == 400, response.text


async def test_profile_delete_conversion_and_migration(
    control_client: httpx.AsyncClient,
) -> None:
    inline = _load("baseline")
    await _apply(control_client, inline, await _plan(control_client, inline))
    resources = _resources()
    await _apply(control_client, resources, await _plan(control_client, resources))
    config = AlembicConfig(str(ALEMBIC_INI))
    config.attributes["connection_url"] = Settings().db_url.get_secret_value()
    with pytest.raises(RuntimeError, match="inline launch specs"):
        alembic_command.downgrade(config, "a71ea9d19503")

    # Removing a referenced profile is rejected at plan time, before SQL.
    response = await control_client.post(
        "/control/v1/plan",
        headers=auth_header(ADMIN_TOKEN),
        json={"resources": [r.model_dump(mode="json") for r in resources[:-1]]},
    )
    assert response.status_code == 400, response.text
    # Profile deletion and conversion of its deployment happen atomically.
    await _apply(control_client, inline, await _plan(control_client, inline))
    unchanged = await _plan(control_client, inline)
    assert not unchanged.to_update and not unchanged.to_delete
    alembic_command.downgrade(config, "a71ea9d19503")
    alembic_command.upgrade(config, "head")
    unchanged = await _plan(control_client, inline)
    assert len(unchanged.no_change) == 5

    await _apply(control_client, resources, await _plan(control_client, resources))
    remaining = [
        r for r in resources if r.kind not in {"PilotDeployment", "LaunchProfile"}
    ]
    await _apply(control_client, remaining, await _plan(control_client, remaining))
    assert len((await _plan(control_client, remaining)).no_change) == 4


async def test_profile_update_revalidates_and_resets_dependents(
    control_client: httpx.AsyncClient,
    db_session: AsyncSession,
) -> None:
    resources = _resources()
    await _apply(control_client, resources, await _plan(control_client, resources))
    deployment = (await db.PilotDeployment.list(db_session))[0]
    deployment.reconcile_failures = 7
    deployment.reconcile_last_error = "previous failure"
    deployment.consecutive_launch_failures = 5
    replica = db.PilotReplica.create(deployment.name)
    replica.reconcile_failures = 9
    db_session.add(replica)
    await db_session.commit()

    profile = resources[-1].spec
    assert isinstance(profile, LaunchProfileSpec)
    profile.parameters["context_window"].default = 1024
    plan = await _plan(control_client, resources)
    assert [r.kind for r in plan.to_update] == ["LaunchProfile"]
    await _apply(control_client, resources, plan)
    await db_session.refresh(deployment)
    assert getattr(deployment, "reconcile_failures") == 0
    assert getattr(deployment, "reconcile_last_error") is None
    assert getattr(deployment, "consecutive_launch_failures") == 0
    await db_session.refresh(replica)
    assert getattr(replica, "reconcile_failures") == 0
    await db_session.refresh(deployment, ["launch_profile"])
    assert deployment.resolve_launch_spec().parameters["context_window"] == 1024

    profile.parameters["context_window"].minimum = 2048
    response = await control_client.post(
        "/control/v1/apply",
        headers=auth_header(ADMIN_TOKEN),
        json={
            "resources": [r.model_dump(mode="json") for r in resources],
            "approved_plan": plan.model_dump(mode="json"),
        },
    )
    assert response.status_code == 422, response.text
    assert deployment.launch_profile is not None
    await db_session.refresh(deployment.launch_profile)
    assert deployment.resolve_launch_spec().parameters["context_window"] == 1024

    # Invalid cross-resource inputs fail inside apply and roll back its version row.
    profile.parameters["context_window"].minimum = 1
    profile.parameters["context_window"].default = 2048
    plan = await _plan(control_client, resources)
    requested = next(r.spec for r in resources if r.kind == "PilotDeployment")
    assert isinstance(requested, PilotDeploymentSpec)
    requested.launch_spec.parameters["context_window"] = 0
    response = await control_client.post(
        "/control/v1/apply",
        headers=auth_header(ADMIN_TOKEN),
        json={
            "resources": [r.model_dump(mode="json") for r in resources],
            "approved_plan": plan.model_dump(mode="json"),
        },
    )
    assert response.status_code == 400, response.text
    assert (
        await db.ConfigVersion.get_latest_version(db_session) == plan.previous_version
    )
    await db_session.refresh(deployment.launch_profile)
    assert deployment.resolve_launch_spec().parameters["context_window"] == 1024


async def test_launcher_sends_resolved_profile_to_pilot(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as session:
        await launcher_tests._seed_parents(session)
        await launcher_tests._insert_deployment(session)
        await launcher_tests._insert_job(session)
        uid = await launcher_tests._insert_replica(session)
        row = ProfileRow(name="vllm", **_profile().model_dump(mode="json"))
        session.add(row)
        deployment = await DeploymentRow.get_by_name(session, "deploy-1")
        deployment.launch_profile = row
        deployment.launch_profile_name = "vllm"
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
    assert len(seen) == 1
    resolved = seen[0].launch_spec
    assert resolved.parameters["context_window"] == 65536
    assert resolved.max_startup_sec == 123
    assert resolved.env["COMMON"] == "1"
    assert resolved.pre_stop_script_template == _profile().pre_stop_script_template


def test_existing_inline_manifests_still_validate() -> None:
    for directory in (Path(__file__).parent / "resource_specs").iterdir():
        if directory.is_dir() and directory.name not in {"duplicates", "invalid_ref"}:
            _load(directory.name)
