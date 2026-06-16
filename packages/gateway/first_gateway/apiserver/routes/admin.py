from fastapi import APIRouter, Body, HTTPException

from first_common.errors import SpecApplyError
from first_common.schema.resource_specs import (
    ResourceApply,
    ResourceChangePlan,
)

from ..dependencies import DbSession

router = APIRouter()


@router.post("/plan", response_model=ResourceChangePlan)
async def plan(
    sess: DbSession,
    resources: list[ResourceApply] = Body(embed=True),
) -> ResourceChangePlan:
    """
    Create a plan for applying a set of resources without actually applying them.

    Returns a ResourceChangePlan describing what would be added, updated,
    deleted, or left unchanged.  This is the "Plan" phase of a
    Plan/Apply workflow — the caller reviews the plan and then submits
    it back to the Apply endpoint to commit changes.
    """
    from first_gateway.services.apply_spec import create_plan

    try:
        plan = await create_plan(resources, sess)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    return plan


@router.post("/apply")
async def apply(
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
    from first_gateway.services.apply_spec import apply

    try:
        async with sess.begin():
            await apply(resources, approved_plan, sess)
    except SpecApplyError as e:
        raise HTTPException(status_code=e.status_code, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e

    return {"status": "applied"}
