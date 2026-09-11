"""
DINOv3 semantic segmentation over a folder of images, served as a task model
through the gateway's generic ``tasks`` route.
"""

from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from .client import InferenceClient

DEFAULT_MODEL = "dinov3"
ENDPOINT = "segmentation"


class DINOv3Result(BaseModel):
    model_config = ConfigDict(extra="allow")

    mask_dir: str
    """Directory of semantic masks, under the caller's staging area."""
    overlay_dir: str | None = None
    num_images: int | None = None
    elapsed_sec: float | None = None


class DINOv3API:
    def __init__(self, client: "InferenceClient", model: str = DEFAULT_MODEL) -> None:
        self._client = client
        self.model = model

    def submit(
        self,
        input_dir: str,
        *,
        project: str | None = None,
        checkpoint: str | None = None,
        save_overlay: bool | None = None,
        batch_size: int | None = None,
        num_workers: int | None = None,
        model: str | None = None,
        **params: Any,
    ) -> DINOv3Result:
        """
        Segment every image in ``input_dir`` (a path on the serving cluster,
        typically staged in).  The results directory is derived server-side as
        a sibling of ``input_dir`` and reported back as ``mask_dir``.  Extra
        ``params`` pass through to the backend.
        """
        options = {
            "project": project,
            "checkpoint": checkpoint,
            "save_overlay": save_overlay,
            "batch_size": batch_size,
            "num_workers": num_workers,
        }
        params.update({k: v for k, v in options.items() if v is not None})
        body = self._client.inference.task(
            model or self.model, ENDPOINT, input_dir=input_dir, **params
        )
        result = body.get("result")
        return DINOv3Result.model_validate(result if isinstance(result, dict) else body)
