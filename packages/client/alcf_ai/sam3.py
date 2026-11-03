"""
SAM3 promptable segmentation, served as a task model through the gateway's
generic ``tasks`` route.  Inference is synchronous: the result comes back in
the response.

Inputs must already be in your staging area (see ``StagingAPI``): the server
only reads files under your own staging directory.  Model weights are fixed
when the server launches, so a fine-tune is a separately named catalog model
selected with ``model``.
"""

import base64
import gzip
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from .client import InferenceClient

DEFAULT_MODEL = "sam3"
ENDPOINT = "promptable-segmentation"


class Sam3ImageResult(BaseModel):
    model_config = ConfigDict(extra="allow")

    input_image: str
    prompt: dict[str, Any]
    boxes: list[list[float]]
    scores: list[float]
    labelmap_npy: str
    """Base64-encoded, gzip-compressed ``.npy`` label map (one label per object)."""

    @property
    def num_objects(self) -> int:
        return len(self.scores)

    def labelmap_bytes(self) -> bytes:
        """The decoded ``.npy`` bytes; load with ``numpy.load(BytesIO(...))``."""
        raw = base64.b64decode(self.labelmap_npy)
        return gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw

    def save_labelmap(self, path: Path) -> None:
        Path(path).write_bytes(self.labelmap_bytes())


class Sam3BatchResult(BaseModel):
    model_config = ConfigDict(extra="allow")

    result_path: str
    runtime_info: dict[str, Any] | None = None


def _unwrap(body: dict[str, Any]) -> dict[str, Any]:
    """Accept both a bare result and the ``{"result": ...}`` envelope."""
    result = body.get("result")
    return result if isinstance(result, dict) else body


class Sam3API:
    def __init__(self, client: "InferenceClient", model: str = DEFAULT_MODEL) -> None:
        self._client = client
        self.model = model

    def submit_image(
        self,
        image_uri: str,
        prompt: str,
        *,
        model: str | None = None,
        **params: Any,
    ) -> Sam3ImageResult:
        """
        Segment a single image by text prompt.  ``image_uri`` is an absolute
        path (or ``file://`` URI) of an image in your staging area, as returned
        by ``StagingAPI.remote_path``; anything else is rejected by the server.
        Extra ``params`` pass through to the backend.
        """
        body = self._client.inference.task(
            model or self.model,
            ENDPOINT,
            inference_type="single-image",
            data_uri=image_uri,
            single_image_prompt=prompt,
            **params,
        )
        return Sam3ImageResult.model_validate(_unwrap(body))

    def submit_batch(
        self,
        tar_path: str,
        *,
        model: str | None = None,
        **params: Any,
    ) -> Sam3BatchResult:
        """
        Segment a WebDataset tar of (image, prompts) pairs in your staging area
        (``tar_path`` as returned by ``StagingAPI.remote_path``).  Returns
        where the results tar was written, beside the input.
        """
        body = self._client.inference.task(
            model or self.model,
            ENDPOINT,
            inference_type="batch",
            data_uri=tar_path,
            **params,
        )
        return Sam3BatchResult.model_validate(_unwrap(body))
