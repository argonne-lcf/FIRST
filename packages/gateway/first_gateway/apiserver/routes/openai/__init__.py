from fastapi import APIRouter

from . import deployments, federated

router = APIRouter()

router.include_router(federated.router)
router.include_router(deployments.router)
