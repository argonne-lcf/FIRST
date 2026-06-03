from collections import Counter, defaultdict
from pathlib import Path

from pydantic import ValidationError
from yaml import safe_load_all

import first.schema.resource_specs as specs
from first.database import models
from first.database.connection import AsyncSession, get_async_sessionmaker
from first.errors import InvalidSpecError, SpecApplyError
from first.schema.resource_specs import (
    FieldChange,
    ResourceApply,
    ResourceChangePlan,
    ResourceIdentifier,
    ResourcePatch,
)


def format_validation_error(
    file: str | Path, kind: str, name: str, exc: ValidationError
) -> str:
    errors = []

    for err in exc.errors(include_url=False):
        location = ".".join(str(l) for l in err["loc"])
        message = err["msg"]
        errors.append(f" - {location}: {message}")

    return f"In {file} ({kind}.{name}):\n{'\n'.join(errors)}\n"


def load_resources_from_yaml(spec_dir: Path | str) -> list[ResourceApply]:
    resources = []

    files = (
        f
        for ext in ("yml", "yaml")
        for f in Path(spec_dir).rglob(f"*.{ext}")
        if f.is_file()
    )

    errors = []

    for file in files:
        with file.open("r") as fp:
            try:
                raw_docs = list(safe_load_all(fp))
            except Exception as e:
                raise InvalidSpecError(f"Failed to load YAML {file}: {e}") from None

        for raw in raw_docs:
            try:
                resource = ResourceApply.model_validate(raw, extra="forbid")
            except ValidationError as exc:
                errors.append(
                    format_validation_error(file, raw.get("kind"), raw.get("name"), exc)
                )
            else:
                resources.append(resource)

    if errors:
        raise InvalidSpecError(
            "One or more resource specs were invalid.\n\n" + "\n".join(errors)
        )

    return resources


def validate_resources(
    resources: list[ResourceApply],
) -> dict[str, list[ResourceApply]]:
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

    by_kind: dict[str, list[ResourceApply]] = defaultdict(list)

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
    resources: list[ResourceApply], sess: AsyncSession
) -> ResourceChangePlan:
    resource_order = (
        (models.AccessGroup, specs.AccessGroup),
        (models.Model, specs.Model),
        (models.Cluster, specs.Cluster),
        (models.StaticDeployment, specs.StaticDeployment),
        (models.PilotDeployment, specs.PilotDeployment),
    )

    no_change = []
    to_delete = []
    to_add = []
    to_update = []

    by_kind = validate_resources(resources)
    current_version = await models.ConfigHistory.get_latest_version(sess)

    for db_model, spec in resource_order:
        kind = spec.__name__
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
            to_delete.append(ResourceIdentifier(kind=kind, name=name))

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
                no_change.append(ResourceIdentifier(kind=kind, name=name))

    return ResourceChangePlan(
        previous_version=current_version,
        to_delete=to_delete,
        no_change=no_change,
        to_add=to_add,
        to_update=to_update,
    )


async def apply(
    resources: list[ResourceApply],
    approved_plan: ResourceChangePlan,
    sess: AsyncSession,
) -> None:

    if not (approved_plan.to_add or approved_plan.to_delete or approved_plan.to_update):
        return

    changes = approved_plan.model_dump(mode="json", exclude={"previous_version"})

    await models.ConfigHistory.record_new_version(
        approved_plan.previous_version,
        changes,
        sess,
    )

    actual_plan = await create_plan(resources, sess)
    if actual_plan.model_dump(exclude={"previous_version"}) != approved_plan.model_dump(
        exclude={"previous_version"}
    ):
        raise SpecApplyError(
            "The actual plan has diverged from the approved plan. Please try again."
        )

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


async def main() -> None:
    import sys

    from rich import print

    sessionmaker = get_async_sessionmaker()
    loaded_resources = load_resources_from_yaml(sys.argv[1])

    async with sessionmaker.begin() as sess:
        plan = await create_plan(loaded_resources, sess)
        await apply(loaded_resources, plan, sess)
        print("Completed apply.")
        print(plan)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
