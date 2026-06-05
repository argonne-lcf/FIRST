from ninja import Router

from .anthropic import router as anthropic_router
from .batch import router as batch_router
from .core import router as core_router
from .data import router as data_router
from .openai import router as openai_router
from .sam3 import router as sam3_router
from .streaming import router as streaming_router

router = Router()
router.add_router("/", anthropic_router, tags=["anthropic"])
router.add_router("/", batch_router, tags=["batch"])
router.add_router("/", core_router, tags=["core"])
router.add_router("/data", data_router, tags=["data"])
router.add_router("/", openai_router, tags=["openai"])
router.add_router("/", sam3_router, tags=["sam3"])
router.add_router("/", streaming_router, tags=["streaming"])

__all__ = ["router"]
