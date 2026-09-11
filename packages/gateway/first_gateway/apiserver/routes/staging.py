from fastapi import APIRouter

from first_common.schema.endpoints.staging import StagingArea

from ...services import staging
from ..dependencies import AppState, AuthUser

router = APIRouter(prefix="/data/v1")


@router.put("/staging", response_model=StagingArea)
async def ensure_staging_area(user: AuthUser, state: AppState) -> StagingArea:
    """
    Idempotently create a Globus staging area for the caller: a per-user
    directory on the data-staging collection with a read/write ACL, for moving
    inputs to (and results from) the clusters serving task models.
    """
    return await staging.ensure_staging_area(user, state)
