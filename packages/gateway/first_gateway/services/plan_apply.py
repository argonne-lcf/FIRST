from collections import Counter, defaultdict

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from first_common.errors import InvalidSpecError, SpecApplyError
from first_common.schema.auth import UserAuthEvent
from first_common.schema.resources.plan_apply import (
    FieldChange,
    ResourceChangePlan,
    ResourceManifest,
    ResourcePatch,
    ResourceRef,
)
from first_common.schema.resources.spec import (
    AccessGroupSpec,
    ClusterSpec,
    ModelSpec,
    PilotDeploymentSpec,
    StaticDeploymentSpec,
)
from first_gateway.database import models

_RECONCILE_CASCADES: dict[str, list[tuple[type[models.ResourceRow], str]]] = {
    "Cluster": [(models.PilotJob, "cluster_name")],
    "PilotDeployment": [(models.PilotReplica, "pilot_deployment_name")],
}


async def _reset_children_reconcile_state(
    sess: AsyncSession, kind: str, name: str
) -> None:
    for child_cls, fk_col in _RECONCILE_CASCADES.get(kind, []):
        await sess.execute(
            sa.update(child_cls)
            .where(getattr(child_cls, fk_col) == name)
            .values(
                reconcile_failures=0,
                reconcile_last_error=None,
                reconcile_retry_at=None,
            )
        )


async def reset_reconcile_state(sess: AsyncSession, kind: str, name: str) -> None:
    """Reset reconcile backoff on a resource and its controller-managed children."""
    cls = models.resource_registry[kind]
    obj = await cls.get_by_name(sess, name)
    obj.reset_reconcile_state()
    await _reset_children_reconcile_state(sess, kind, name)


def validate_resources(
    resources: list[ResourceManifest],
) -> dict[str, list[ResourceManifest]]:
    """
    Validate resource uniqueness and referential integrity, then return
    resources grouped by kind.
    """
    # Uniqueness
    id_counts = Counter((r.kind, r.name) for r in resources)
    duplicates = [k for k, v in id_counts.items() if v > 1]
    if duplicates:
        raise InvalidSpecError(
            "Duplicate resources (by <kind>.<name>) are forbidden:\n"
            + "\n".join(f" - DUPLICATED: {key[0]}.{key[1]}" for key in duplicates)
        )

    by_kind: dict[str, list[ResourceManifest]] = defaultdict(list)

    # Referential Integrity
    references = [
        ("Model", "AccessGroup", "access_group_name"),
        ("StaticDeployment", "Cluster", "cluster_name"),
        ("StaticDeployment", "Model", "model_name"),
        ("PilotDeployment", "Cluster", "cluster_name"),
        ("PilotDeployment", "Model", "model_name"),
    ]

    for r in resources:
        by_kind[r.kind].append(r)

    for child_kind, parent_kind, ref in references:
        existing_parent_names = [r.name for r in by_kind[parent_kind]]
        for child in by_kind[child_kind]:
            parent_name: str = getattr(child.spec, ref)
            if parent_name not in existing_parent_names:
                raise InvalidSpecError(
                    f"{child_kind}.{child.name} references nonexistant {parent_kind}.{parent_name}"
                )

    # Return Grouped by Kind
    return by_kind


async def create_plan(
    resources: list[ResourceManifest], sess: AsyncSession
) -> ResourceChangePlan:
    resource_order = (
        (models.AccessGroup, AccessGroupSpec),
        (models.Model, ModelSpec),
        (models.Cluster, ClusterSpec),
        (models.StaticDeployment, StaticDeploymentSpec),
        (models.PilotDeployment, PilotDeploymentSpec),
    )

    no_change = []
    to_delete = []
    to_add = []
    to_update = []

    by_kind = validate_resources(resources)
    current_version = await models.ConfigVersion.get_latest_version(sess)

    for db_model, spec in resource_order:
        kind = db_model.__name__
        existing = {
            row.name: spec.model_validate(row, extra="ignore")
            for row in await db_model.list(sess)
        }
        desired = {r.name: r for r in by_kind[kind]}

        desired_names = set(desired)
        existing_names = set(existing)

        for name in sorted(desired_names - existing_names):
            to_add.append(desired[name])

        for name in sorted(existing_names - desired_names):
            to_delete.append(ResourceRef(kind=kind, name=name))

        for name in sorted(desired_names & existing_names):
            patch = {}
            old = existing[name].model_dump(mode="json")
            new = desired[name].spec.model_dump(mode="json")

            for field in spec.model_fields:
                if old[field] != new[field]:
                    patch[field] = FieldChange(old[field], new[field])

            if patch:
                to_update.append(ResourcePatch(kind=kind, name=name, patch=patch))
            else:
                no_change.append(ResourceRef(kind=kind, name=name))

    return ResourceChangePlan(
        previous_version=current_version,
        to_delete=to_delete,
        no_change=no_change,
        to_add=to_add,
        to_update=to_update,
    )


async def apply_plan(
    resources: list[ResourceManifest],
    approved_plan: ResourceChangePlan,
    user: UserAuthEvent,
    sess: AsyncSession,
) -> models.ConfigVersion | None:

    if not (approved_plan.to_add or approved_plan.to_delete or approved_plan.to_update):
        return None

    changes = approved_plan.model_dump(mode="json", exclude={"previous_version"})

    config_version = await models.ConfigVersion.record_new_version(
        approved_plan.previous_version,
        changes,
        user,
        sess,
    )

    actual_plan = await create_plan(resources, sess)
    if actual_plan.model_dump(exclude={"previous_version"}) != approved_plan.model_dump(
        exclude={"previous_version"}
    ):
        raise SpecApplyError(
            "The actual plan has diverged from the approved plan. Please try again."
        )

    with sess.no_autoflush:
        for delete_resource in actual_plan.to_delete:
            cls = models.resource_registry[delete_resource.kind]
            obj = await cls.get_by_name(sess, delete_resource.name)
            await obj.delete(sess)

        for new_resource in actual_plan.to_add:
            cls = models.resource_registry[new_resource.kind]
            cls.create_from_spec(sess, new_resource.name, new_resource.spec)

        for patch_resource in actual_plan.to_update:
            cls = models.resource_registry[patch_resource.kind]
            obj = await cls.get_by_name(sess, patch_resource.name)
            obj.apply_patch(patch_resource.patch)
            obj.reset_reconcile_state()
            await _reset_children_reconcile_state(
                sess, patch_resource.kind, patch_resource.name
            )

    return config_version
