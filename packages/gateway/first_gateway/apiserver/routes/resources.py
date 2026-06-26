from fastapi import APIRouter, Body

from first_common.errors import AccessDenied
from first_common.schema.resources import (
    ConfigVersion,
    ConfigVersionSummary,
    ResourceChangePlan,
    ResourceManifest,
)
from first_common.schema.resources.read import (
    AccessGroup,
    ClusterDetail,
    ClusterSummary,
    ModelSummary,
    PilotDeploymentDetail,
    PilotDeploymentSummary,
    StaticDeployment,
)

from ...database import models as db
from ...services.plan_apply import apply_plan, create_plan
from ..auth import user_can_access_group
from ..dependencies import AdminUser, AuthUser, DbSession, IsUserAdmin

admin_router = APIRouter(prefix="/resources")
user_router = APIRouter(prefix="/resources")


@user_router.get("/access-groups", response_model=list[AccessGroup])
async def list_access_groups(
    sess: DbSession, user: AuthUser, is_admin: IsUserAdmin
) -> list[db.AccessGroup]:
    """
    List AccessGroups.  Admins see all; ordinary users see only the AccessGroups
    they qualify for.
    """
    groups = await db.AccessGroup.list(sess)
    if is_admin:
        return groups
    return [g for g in groups if user_can_access_group(user, g)]


@user_router.get("/models", response_model=list[ModelSummary])
async def list_models(
    sess: DbSession, user: AuthUser, is_admin: IsUserAdmin
) -> list[db.Model]:
    """
    List Models.  Admins see all; ordinary users see only Models whose
    AccessGroup grants them access.
    """
    models = await db.Model.list(sess)
    if is_admin:
        return models
    return [m for m in models if user_can_access_group(user, m.access_group)]


@user_router.get("/pilot-deployments", response_model=list[PilotDeploymentSummary])
async def list_pilot_deployments(
    sess: DbSession, user: AuthUser, is_admin: IsUserAdmin
) -> list[db.PilotDeployment]:
    """
    List PilotDeployments.  Admins see all; ordinary users see only deployments
    whose parent Model authorizes them.
    """
    if is_admin:
        return await db.PilotDeployment.list(sess)

    return [
        dep
        for model in await db.Model.list(sess)
        for dep in model.pilot_deployments
        if user_can_access_group(user, model.access_group)
    ]


@user_router.get("/pilot-deployments/{name:path}", response_model=PilotDeploymentDetail)
async def get_pilot_deployment(
    sess: DbSession, user: AuthUser, name: str, is_admin: IsUserAdmin
) -> db.PilotDeployment:
    """Get a single PilotDeployment with its replicas."""
    deployment = await db.PilotDeployment.get_detail(sess, name)
    if not (is_admin or user_can_access_group(user, deployment.model.access_group)):
        raise AccessDenied(f"Permission denied for PilotDeployment {name!r}.")
    return deployment


@user_router.get("/static-deployments", response_model=list[StaticDeployment])
async def list_static_deployments(
    sess: DbSession, user: AuthUser, is_admin: IsUserAdmin
) -> list[db.StaticDeployment]:
    """
    List StaticDeployments.  Admins see all; ordinary users see only deployments
    whose parent Model authorizes them.
    """
    if is_admin:
        return await db.StaticDeployment.list(sess)

    return [
        dep
        for model in await db.Model.list(sess)
        for dep in model.static_deployments
        if user_can_access_group(user, model.access_group)
    ]


@user_router.get("/clusters", response_model=list[ClusterSummary])
async def list_clusters(sess: DbSession) -> list[db.Cluster]:
    """List all configured Cluster resources.  Visible to all users."""
    return await db.Cluster.list(sess)


@admin_router.get("/clusters/{name:path}", response_model=ClusterDetail)
async def get_cluster(sess: DbSession, name: str) -> db.Cluster:
    """
    Get a Cluster with its pilot jobs.  Admin-only: pilot job details are
    sensitive operational state.
    """
    return await db.Cluster.get_detail(sess, name)


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
    "/pilot-deployments/{name:path}/desired-replicas",
    response_model=PilotDeploymentSummary,
)
async def set_desired_pilot_replicas(
    sess: DbSession, name: str, num_replicas: int = Body(embed=True, ge=0, le=4096)
) -> db.PilotDeployment:
    """Manually set desired scale of a PilotDeployment"""
    async with sess.begin():
        deployment = await db.PilotDeployment.get_by_name(sess, name)
        deployment.set_desired_replicas(num_replicas)
    return deployment


@admin_router.get("/pilot-replicas/{name:path}/logs")
async def tail_replica_logs(sess: DbSession, name: str) -> str:
    """Read tail of logs generated by replica"""
    # replica = await db.PilotReplica.get_by_name(sess, name)
    raise NotImplementedError("TODO: hit the control plane's /logs/{replica_name}")
