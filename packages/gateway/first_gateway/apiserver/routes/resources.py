from fastapi import APIRouter, Body

from first_common.schema.resource import ClusterSummary
from first_common.schema.resource_specs import (
    ConfigVersion,
    ResourceApply,
    ResourceChangePlan,
)

from ...database import models as db
from ...services.apply_spec import apply, create_plan
from ..dependencies import AdminUser, DbSession

admin_router = APIRouter(prefix="/resources")
user_router = APIRouter(prefix="/resources")


@user_router.get("/clusters", response_model=list[ClusterSummary])
async def summarize_clusters(sess: DbSession) -> list[db.Cluster]:
    """List all configured Cluster resources."""
    return await db.Cluster.list(sess)


@user_router.get("/clusters/{name}", response_model=list[ClusterSummary])
async def describe_cluster(sess: DbSession, name: str) -> list[db.Cluster]:
    """List all configured Cluster resources."""
    return await db.Cluster.get_detail(sess, name)


@admin_router.post("/plan", response_model=ResourceChangePlan)
async def plan_resources(
    sess: DbSession,
    resources: list[ResourceApply] = Body(embed=True),
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
    resources: list[ResourceApply],
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
        return await apply(resources, approved_plan, admin, sess)
