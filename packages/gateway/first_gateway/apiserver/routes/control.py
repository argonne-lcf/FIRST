from fastapi import APIRouter, Body

from first_common.errors import InvalidSpecError
from first_common.schema.resources import (
    ConfigVersion,
    ConfigVersionSummary,
    ResourceChangePlan,
    ResourceManifest,
)
from first_common.schema.resources.read import (
    PilotDeploymentSummary,
)

from ...database import models as db
from ...database.redis.pubsub import Channel
from ...services.plan_apply import apply_plan, create_plan
from ..dependencies import (
    AdminUser,
    DbSession,
    RedisPubSub,
)

admin_router = APIRouter(prefix="/control/v1")


@admin_router.post("/plan", response_model=ResourceChangePlan)
async def plan_resources(
    sess: DbSession,
    resources: list[ResourceManifest] = Body(embed=True),
) -> ResourceChangePlan:
    """
    Create a plan for applying a set of resources without actually applying them.

    Returns a ResourceChangePlan describing what would be added, updated,
    deleted, or left unchanged.  This is the "Plan" phase of a Plan/Apply
    workflow.  The caller reviews the plan and then submits it back to the Apply
    endpoint to commit changes.
    """
    return await create_plan(resources, sess)


@admin_router.post("/apply", response_model=ConfigVersion | None)
async def apply_resources(
    resources: list[ResourceManifest],
    approved_plan: ResourceChangePlan,
    sess: DbSession,
    admin: AdminUser,
) -> db.ConfigVersion | None:
    """
    Apply a previously-approved plan.

    Takes the same resources and an approved ResourceChangePlan (one that
    was returned by the /plan endpoint and reviewed by the caller).
    Performs a two-phase commit: replans the current state and only
    proceeds if it matches the approved plan, ensuring no concurrent
    modifications have occurred.
    """
    async with sess.begin():
        return await apply_plan(resources, approved_plan, admin, sess)


@admin_router.get("/config-versions", response_model=list[ConfigVersionSummary])
async def list_config_versions(sess: DbSession) -> list[db.ConfigVersion]:
    """List all recorded ConfigVersions"""
    return await db.ConfigVersion.list(sess)


@admin_router.get("/config-versions/{uid}", response_model=ConfigVersion)
async def get_config_version(sess: DbSession, uid: int) -> db.ConfigVersion:
    """Get a single ConfigVersion by uid, including the full `changes` record."""
    return await db.ConfigVersion.get_detail(sess, uid)


@admin_router.put(
    "/deployments/pilot/{name:path}/desired-replicas",
    response_model=PilotDeploymentSummary,
)
async def set_desired_pilot_replicas(
    sess: DbSession,
    pubsub: RedisPubSub,
    name: str,
    num_replicas: int = Body(embed=True, ge=0, le=4096),
) -> db.PilotDeployment:
    """Manually set desired scale of a PilotDeployment"""
    async with sess.begin():
        deployment = await db.PilotDeployment.get_by_name(sess, name)
        deployment.consecutive_launch_failures = 0
        deployment.set_desired_replicas(num_replicas)
        await db.PilotDeployment.reset_reconcile_state(sess, deployment.uid)
    await pubsub.publish(Channel.desired_replicas_changed, name)
    return deployment


@admin_router.post("/reconcile-reset")
async def reconcile_reset(
    sess: DbSession,
    resource: str = Body(embed=True),
) -> dict[str, str]:
    """Reset reconcile backoff state for a resource and its children."""
    kind, _dot, name = resource.partition(".")
    if not name:
        raise InvalidSpecError(
            f"Invalid resource identifier {resource!r}: expected 'Kind.name'"
        )

    if not (ResourceClass := db.resource_registry.get(kind)):
        raise InvalidSpecError(f"Unknown resource kind {kind!r}")

    async with sess.begin():
        row = await ResourceClass.get_by_name(sess, name)
        await ResourceClass.reset_reconcile_state(sess, row.uid, cascade=True)
        if isinstance(row, db.PilotDeployment):
            row.consecutive_launch_failures = 0

    return {"status": "ok", "resource": resource}
