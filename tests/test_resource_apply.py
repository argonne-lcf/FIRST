"""
Tests for the /resources/plan and /resources/apply resource management endpoints.
"""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from alcf_ai.subcommands.admin import load_resources_from_yaml
from first_common.schema.resources import (
    ConfigVersion,
    ResourceChangePlan,
    ResourceManifest,
)

from .fixtures.auth import ADMIN_TOKEN, USER_TOKEN, auth_header

RESOURCES_DIR = Path(__file__).parent / "resource_specs"


def _load(spec_dir: str) -> list[ResourceManifest]:
    """Load resource specs from a YAML bundle in the given subdirectory."""
    return load_resources_from_yaml(RESOURCES_DIR / spec_dir)


async def _plan(
    client: httpx.AsyncClient, resources: list[ResourceManifest]
) -> ResourceChangePlan:
    """POST /plan and return the parsed JSON response."""
    resp = await client.post(
        "/resources/plan",
        json={"resources": [r.model_dump(mode="json") for r in resources]},
        headers=auth_header(ADMIN_TOKEN),
    )
    assert resp.status_code == 200, resp.text
    return ResourceChangePlan.model_validate(resp.json())


async def _apply(
    client: httpx.AsyncClient,
    resources: list[ResourceManifest],
    plan: ResourceChangePlan,
) -> ConfigVersion | None:
    """POST /apply with the given resources and approved plan."""
    resp = await client.post(
        "/resources/apply",
        json={
            "resources": [r.model_dump(mode="json") for r in resources],
            "approved_plan": plan.model_dump(mode="json"),
        },
        headers=auth_header(ADMIN_TOKEN),
    )
    assert resp.status_code == 200, resp.text
    dat = resp.json()
    return ConfigVersion.model_validate(dat) if dat is not None else None


@pytest.fixture
async def baseline_plan(client: httpx.AsyncClient) -> ResourceChangePlan:
    """Apply the baseline spec and return the resulting plan."""
    resources = _load("baseline")
    plan = await _plan(client, resources)
    await _apply(client, resources, plan)
    return plan


async def test_no_changes(
    client: httpx.AsyncClient, baseline_plan: ResourceChangePlan
) -> None:
    """Re-submitting the baseline produces a plan with everything in no_change."""
    resources = _load("baseline")
    plan = await _plan(client, resources)

    assert plan.previous_version == baseline_plan.previous_version + 1
    assert len(plan.no_change) == 5
    assert plan.to_add == []
    assert plan.to_delete == []
    assert plan.to_update == []

    # Applying a no-change plan is a no-op (200, no error)
    await _apply(client, resources, plan)


async def test_additions(
    client: httpx.AsyncClient, baseline_plan: ResourceChangePlan
) -> None:
    """Adding new resources shows them in to_add; existing resources stay in no_change."""
    resources = _load("additions")
    plan = await _plan(client, resources)

    # 5 baseline resources unchanged + 4 new
    assert len(plan.no_change) == 5
    assert len(plan.to_add) == 4
    assert plan.to_delete == []
    assert plan.to_update == []

    added_kinds = {r.kind for r in plan.to_add}
    assert "AccessGroup" in added_kinds
    assert "Cluster" in added_kinds
    assert "Model" in added_kinds
    assert "PilotDeployment" in added_kinds

    # Apply and verify version bumps
    result = await _apply(client, resources, plan)
    assert result is not None
    assert result.uid == baseline_plan.previous_version + 2

    # Re-plan after apply: everything should be no_change now
    plan2 = await _plan(client, resources)
    assert len(plan2.no_change) == 9
    assert plan2.to_add == []
    assert plan2.to_delete == []
    assert plan2.to_update == []


async def test_deletions(
    client: httpx.AsyncClient, baseline_plan: ResourceChangePlan
) -> None:
    """Removing resources shows them in to_delete."""
    resources = _load("deletions")
    plan = await _plan(client, resources)

    # 3 resources remain unchanged (AccessGroup, Cluster, Model)
    assert len(plan.no_change) == 3
    assert plan.to_add == []
    assert len(plan.to_delete) == 2
    assert plan.to_update == []

    deleted_names = {r.name for r in plan.to_delete}
    assert "sophia/static/llama-3-8b" in deleted_names
    assert "sophia/pilot/llama-3-8b" in deleted_names

    # Apply and verify
    result = await _apply(client, resources, plan)
    assert result is not None
    assert result.uid == baseline_plan.previous_version + 2

    # Re-plan: only 3 resources remain
    plan2 = await _plan(client, resources)
    assert len(plan2.no_change) == 3
    assert plan2.to_add == []
    assert plan2.to_delete == []
    assert plan2.to_update == []


async def test_updates(
    client: httpx.AsyncClient, baseline_plan: ResourceChangePlan
) -> None:
    """Modifying existing resources shows them in to_update."""
    resources = _load("updates")
    plan = await _plan(client, resources)

    # 3 resources unchanged (AccessGroup, Model, PilotDeployment)
    assert len(plan.no_change) == 3
    assert plan.to_add == []
    assert plan.to_delete == []
    assert len(plan.to_update) == 2

    updated_names = {r.name for r in plan.to_update}
    assert "sophia" in updated_names
    assert "sophia/static/llama-3-8b" in updated_names

    # Verify the Cluster update contains the maintenance_notice change
    sophia_patch = next(r for r in plan.to_update if r.name == "sophia")
    assert "maintenance_notice" in sophia_patch.patch
    old_val = sophia_patch.patch["maintenance_notice"][0]
    new_val = sophia_patch.patch["maintenance_notice"][1]
    assert old_val == "Sophia is operational"
    assert new_val == "Sophia is under scheduled maintenance"

    # Verify the StaticDeployment update contains the weight change
    static_patch = next(
        r for r in plan.to_update if r.name == "sophia/static/llama-3-8b"
    )
    assert "router_params" in static_patch.patch

    # Apply and verify
    result = await _apply(client, resources, plan)
    assert result is not None
    assert result.uid == baseline_plan.previous_version + 2


async def test_invalid_reference(client: httpx.AsyncClient) -> None:
    """A resource referencing a nonexistent parent returns 400."""
    resources = _load("invalid_ref")
    resp = await client.post(
        "/resources/plan",
        json={"resources": [r.model_dump(mode="json") for r in resources]},
        headers=auth_header(ADMIN_TOKEN),
    )
    assert resp.status_code == 400
    body = resp.json()
    assert body["error"]["code"] == "resource_spec_invalid"
    assert "nonexistent-group" in body["error"]["message"]


async def test_duplicate_resources(client: httpx.AsyncClient) -> None:
    """Two resources with the same kind+name returns 400."""
    resources = _load("duplicates")
    resp = await client.post(
        "/resources/plan",
        json={"resources": [r.model_dump(mode="json") for r in resources]},
        headers=auth_header(ADMIN_TOKEN),
    )
    assert resp.status_code == 400
    body = resp.json()
    assert body["error"]["code"] == "resource_spec_invalid"


async def test_concurrent_update_diverged_plan(
    client: httpx.AsyncClient, baseline_plan: ResourceChangePlan
) -> None:
    """
    Applying a stale plan (generated before another apply) raises SpecApplyError.
    """
    # Step 1: baseline is already applied via fixture (version 1).

    # Step 2: generate a plan for the updates spec.
    updates_resources = _load("updates")
    plan_updates = await _plan(client, updates_resources)
    assert len(plan_updates.to_update) == 2

    # Step 3: apply the additions spec instead (changes the DB state).
    additions_resources = _load("additions")
    plan_additions = await _plan(client, additions_resources)
    await _apply(client, additions_resources, plan_additions)

    # Step 4: try to apply the stale plan_updates.
    # The actual plan (replanned from current DB) will differ from plan_updates.
    resp = await client.post(
        "/resources/apply",
        json={
            "resources": [r.model_dump(mode="json") for r in updates_resources],
            "approved_plan": plan_updates.model_dump(mode="json"),
        },
        headers=auth_header(ADMIN_TOKEN),
    )
    assert resp.status_code == 409
    body = resp.json()
    assert body["error"]["code"] == "failed_to_apply_resource_spec"


async def test_list_access_groups_admin_and_user(
    client: httpx.AsyncClient, baseline_plan: ResourceChangePlan
) -> None:
    """Admin sees the baseline AccessGroup; ordinary user fails its group +
    domain restrictions and sees nothing."""
    admin_resp = await client.get(
        "/resources/access-groups", headers=auth_header(ADMIN_TOKEN)
    )
    user_resp = await client.get(
        "/resources/access-groups", headers=auth_header(USER_TOKEN)
    )
    assert admin_resp.status_code == 200
    assert user_resp.status_code == 200
    assert [g["name"] for g in admin_resp.json()] == ["research-team"]
    assert user_resp.json() == []


async def test_list_models(
    client: httpx.AsyncClient, baseline_plan: ResourceChangePlan
) -> None:
    resp = await client.get("/resources/models", headers=auth_header(ADMIN_TOKEN))
    assert resp.status_code == 200
    [model] = resp.json()
    assert model["name"] == "meta-llama/llama-3-8b"
    assert model["access_group_name"] == "research-team"
    assert len(model["pilot_deployments"]) == 1
    assert len(model["static_deployments"]) == 1


async def test_list_static_deployments(
    client: httpx.AsyncClient, baseline_plan: ResourceChangePlan
) -> None:
    resp = await client.get(
        "/resources/static-deployments", headers=auth_header(ADMIN_TOKEN)
    )
    assert resp.status_code == 200
    [d] = resp.json()
    assert d["name"] == "sophia/static/llama-3-8b"
    assert d["upstream_model_name"] == "s-llama-3-8b"


async def test_list_and_get_pilot_deployment(
    client: httpx.AsyncClient, baseline_plan: ResourceChangePlan
) -> None:
    list_resp = await client.get(
        "/resources/pilot-deployments", headers=auth_header(ADMIN_TOKEN)
    )
    assert list_resp.status_code == 200
    [summary] = list_resp.json()
    assert summary["name"] == "sophia/pilot/llama-3-8b"
    assert "replicas" not in summary  # summary view defers replicas

    detail_resp = await client.get(
        f"/resources/pilot-deployments/{summary['name']}",
        headers=auth_header(ADMIN_TOKEN),
    )
    assert detail_resp.status_code == 200
    detail = detail_resp.json()
    assert detail["name"] == summary["name"]
    assert detail["replicas"] == []  # no replicas yet
    assert detail["launch_spec"]["served_model_name"] == "meta-llama/llama-3-8b"


async def test_list_and_get_cluster(
    client: httpx.AsyncClient, baseline_plan: ResourceChangePlan
) -> None:
    # List visible to ordinary users
    user_list = await client.get("/resources/clusters", headers=auth_header(USER_TOKEN))
    assert user_list.status_code == 200
    [summary] = user_list.json()
    assert summary["name"] == "sophia"
    assert "pilot_jobs" not in summary

    # Detail visible to admins
    admin_detail = await client.get(
        "/resources/clusters/sophia", headers=auth_header(ADMIN_TOKEN)
    )
    assert admin_detail.status_code == 200
    detail = admin_detail.json()
    assert detail["name"] == "sophia"
    assert detail["pilot_jobs"] == []

    # Ordinary users forbidden from cluster detail
    user_detail = await client.get(
        "/resources/clusters/sophia", headers=auth_header(USER_TOKEN)
    )
    assert user_detail.status_code == 403
    assert user_detail.json()["error"]["code"] == "access_denied"


async def test_user_filtered_out_when_no_access(
    client: httpx.AsyncClient, baseline_plan: ResourceChangePlan
) -> None:
    """Resources gated by an AccessGroup the user fails should disappear from
    user-scoped listings, while admins still see them."""

    async def names(path: str, token: str) -> set[str]:
        resp = await client.get(path, headers=auth_header(token))
        assert resp.status_code == 200, resp.text
        return {o["name"] for o in resp.json()}

    assert await names("/resources/models", USER_TOKEN) == set()
    assert await names("/resources/pilot-deployments", USER_TOKEN) == set()
    assert await names("/resources/static-deployments", USER_TOKEN) == set()
    # Admin still sees the underlying resources.
    assert await names("/resources/models", ADMIN_TOKEN) == {"meta-llama/llama-3-8b"}


async def test_get_pilot_deployment_forbidden_for_unauthorized_user(
    client: httpx.AsyncClient, baseline_plan: ResourceChangePlan
) -> None:
    resp = await client.get(
        "/resources/pilot-deployments/sophia/pilot/llama-3-8b",
        headers=auth_header(USER_TOKEN),
    )
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "access_denied"
