from collections import Counter, defaultdict

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
    LaunchTemplateSpec,
    ModelSpec,
    PilotDeploymentSpec,
    StaticDeploymentSpec,
)
from first_common.schema.types import MTLSLoadError
from first_gateway.database import models


def validate_resources(
    resources: list[ResourceManifest],
    *,
    check_mtls_files: bool = True,
) -> dict[str, list[ResourceManifest]]:
    """
    Validate resource uniqueness and referential integrity, then return
    resources grouped by kind.

    Offline callers may omit VM-local certificate-file checks. All schema,
    references and launch-template checks still run; server plan/apply keeps
    the default file validation. This is not an HTTP request option.
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
        ("PilotDeployment", "LaunchTemplate", "launch_template_name"),
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

    # Retirement tombstones must not contradict the live deployments: a retired
    # model has none, and a retired deployment name is not also a live one.
    deployments_by_model: dict[str, list[str]] = defaultdict(list)
    for kind in ("StaticDeployment", "PilotDeployment"):
        for deployment in by_kind[kind]:
            assert isinstance(
                deployment.spec, (StaticDeploymentSpec, PilotDeploymentSpec)
            )
            deployments_by_model[deployment.spec.model_name].append(deployment.name)
    live_deployments = {n for names in deployments_by_model.values() for n in names}
    for model_resource in by_kind["Model"]:
        model_spec = model_resource.spec
        assert isinstance(model_spec, ModelSpec)
        if model_spec.retired and (live := deployments_by_model[model_resource.name]):
            raise InvalidSpecError(
                f"Model.{model_resource.name} is retired but still has "
                f"deployments: {', '.join(sorted(live))}.  Remove them, or set "
                "retired: false."
            )
        clash = {d.name for d in model_spec.retired_deployments} & live_deployments
        if clash:
            raise InvalidSpecError(
                f"Model.{model_resource.name} lists retired_deployments that are "
                f"still live: {', '.join(sorted(clash))}."
            )

    # A request can never be admitted if it costs more than the user's token
    # bucket holds, so a context window larger than burst_tokens is unusable.
    for model_resource in by_kind["Model"]:
        model_spec = model_resource.spec
        assert isinstance(model_spec, ModelSpec)
        if model_spec.max_model_len is None:
            continue
        policy = model_spec.usage_limits
        limits = [("default", policy.default.burst_tokens)] + [
            (f"override {o.kind}={o.id}", o.burst_tokens)
            for o in policy.overrides
            if o.burst_tokens is not None
        ]
        for where, burst_tokens in limits:
            if burst_tokens < model_spec.max_model_len:
                raise InvalidSpecError(
                    f"Model.{model_resource.name}: usage_limits {where} "
                    f"burst_tokens ({burst_tokens}) must be at least "
                    f"max_model_len ({model_spec.max_model_len}); otherwise "
                    "requests using the full context can never be admitted."
                )

    # A pilot job hosts exactly one PilotDeployment replica's nodes, so the
    # cluster's per-job limit bounds every deployment's num_nodes.
    clusters_by_name = {r.name: r.spec for r in by_kind["Cluster"]}
    for deployment in by_kind["PilotDeployment"]:
        spec = deployment.spec
        assert isinstance(spec, PilotDeploymentSpec)
        cluster = clusters_by_name[spec.cluster_name]
        assert isinstance(cluster, ClusterSpec)
        if cluster.pilot_system is None:
            continue
        limit = cluster.pilot_system.max_nodes_per_job
        if spec.launch_spec.num_nodes > limit:
            raise InvalidSpecError(
                f"PilotDeployment.{deployment.name}: launch_spec.num_nodes "
                f"({spec.launch_spec.num_nodes}) exceeds Cluster."
                f"{spec.cluster_name} pilot_system.max_nodes_per_job ({limit})."
            )

    # Resolve every deployment against its template so a template edit that
    # breaks a dependent (or an invalid launch_spec) fails at plan time.
    templates = {r.name: r.spec for r in by_kind["LaunchTemplate"]}
    models_by_name = {r.name: r.spec for r in by_kind["Model"]}
    for deployment in by_kind["PilotDeployment"]:
        spec = deployment.spec
        assert isinstance(spec, PilotDeploymentSpec)
        template = templates[spec.launch_template_name]
        model = models_by_name[spec.model_name]
        assert isinstance(template, LaunchTemplateSpec)
        assert isinstance(model, ModelSpec)
        try:
            template.resolve(spec.launch_spec, model.max_model_len)
        except ValueError as exc:
            raise InvalidSpecError(f"PilotDeployment.{deployment.name}: {exc}") from exc

    # Certificate paths name files inside the controller and gateway
    # containers; check them here, where the controller can see its mounts.
    for deployment in by_kind["StaticDeployment"]:
        spec = deployment.spec
        assert isinstance(spec, StaticDeploymentSpec)
        if spec.mtls is None or not check_mtls_files:
            continue
        try:
            spec.mtls.ssl_context()
        except MTLSLoadError as exc:
            raise InvalidSpecError(
                f"StaticDeployment.{deployment.name}: {exc}"
            ) from exc

    # Return Grouped by Kind
    return by_kind


async def create_plan(
    resources: list[ResourceManifest], sess: AsyncSession
) -> ResourceChangePlan:
    resource_order = (
        (models.AccessGroup, AccessGroupSpec),
        (models.Model, ModelSpec),
        (models.Cluster, ClusterSpec),
        (models.LaunchTemplate, LaunchTemplateSpec),
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

    # Add, then update, then delete (children before parents), flushing between
    # stages so a resource repointed from a deleted parent to a new one is
    # already referencing the new parent when the old row goes away.
    with sess.no_autoflush:
        for new_resource in actual_plan.to_add:
            cls = models.resource_registry[new_resource.kind]
            cls.create_from_spec(sess, new_resource.name, new_resource.spec)
    await sess.flush()

    with sess.no_autoflush:
        for patch_resource in actual_plan.to_update:
            cls = models.resource_registry[patch_resource.kind]
            obj = await cls.get_by_name(sess, patch_resource.name)
            obj.apply_patch(patch_resource.patch)
            await cls.reset_reconcile_state(sess, obj.uid, cascade=True)
            models.reset_failure_latches(obj)
    await sess.flush()

    for delete_resource in reversed(actual_plan.to_delete):
        cls = models.resource_registry[delete_resource.kind]
        obj = await cls.get_by_name(sess, delete_resource.name)
        await obj.delete(sess)
        await sess.flush()

    return config_version
