import logging

from django.conf import settings
from ninja import Router

from ..schemas import (
    GlobusStagingAreaPrepared,
)
from ..schemas.auth import AuthedRequest
from ..services import (
    prep_globus_staging_area,
)

router = Router()
log = logging.getLogger(__name__)


@router.put("/staging", response=GlobusStagingAreaPrepared)
def ensure_staging_area(request: AuthedRequest) -> GlobusStagingAreaPrepared:
    """
    Idempotent user request to create a staging area for the inference service.

    A temporary directory named with the user's principal ID is created and
    read/write ACLs are granted to the user to initiate data transfers.
    """
    return prep_globus_staging_area(
        principal_id=request.auth.id,
        collection_id=settings.DATA_STAGING_GLOBUS_COLLECTION_ID,
    )
