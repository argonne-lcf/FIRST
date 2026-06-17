from fastapi import APIRouter, Body

from first_common.schema.resource import ClusterSummary
from first_common.schema.resource_specs import (
    ResourceApply,
    ResourceChangePlan,
)

from ...database.models import Cluster
from ...services.apply_spec import apply, create_plan
from ..dependencies import DbSession

admin_router = APIRouter()
user_router = APIRouter()


@user_router.get("/clusters", response_model=list[ClusterSummary])
async def list_clusters(sess: DbSession) -> list[Cluster]:
    """List all configured Cluster resources."""
    return await Cluster.list(sess)


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


@admin_router.post("/apply")
async def apply_resources(
    resources: list[ResourceApply],
    approved_plan: ResourceChangePlan,
    sess: DbSession,
) -> dict[str, str]:
    """
    Apply a previously-approved plan.

    Takes the same resources and an approved ResourceChangePlan (one that
    was returned by the /plan endpoint and reviewed by the caller).
    Performs a two-phase commit: replans the current state and only
    proceeds if it matches the approved plan, ensuring no concurrent
    modifications have occurred.
    """
    async with sess.begin():
        await apply(resources, approved_plan, sess)

    return {"status": "applied"}
