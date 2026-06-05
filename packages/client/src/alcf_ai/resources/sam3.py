import base64
import gzip
import logging
import time
from io import BytesIO
from pathlib import Path
from typing import Annotated, Any, Literal

import numpy as np
import numpy.typing as npt
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
)

from .resource import ClientResource

NDArray = npt.NDArray[Any]
logger = logging.getLogger(__name__)


def to_ndarray(obj: Any) -> NDArray:
    """
    Load .npy array from gzipped and base64 encoded string.
    """
    if isinstance(obj, str):
        obj = base64.b64decode(obj)

    if isinstance(obj, bytes):
        if obj[:2] == b"\x1f\x8b":
            return np.load(BytesIO(gzip.decompress(obj)), allow_pickle=False)  # type: ignore[no-any-return]
        else:
            return np.load(BytesIO(obj), allow_pickle=False)  # type: ignore[no-any-return]

    if isinstance(obj, np.ndarray):
        return obj

    raise ValueError(f"Expected str, bytes, or ndarray; got {type(obj)}")


CompressedNDArray = Annotated[
    NDArray,
    BeforeValidator(to_ndarray),
]


class Sam3Request(BaseModel):
    inference_type: Literal["single-image", "batch"]
    data_uri: str
    single_image_prompt: str | None = None
    weights_dir_override: Path | None = None


class Sam3ImageResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    input_image: str
    prompt: dict[str, Any]
    boxes: list[list[float]]
    scores: list[float]
    labelmap_npy: CompressedNDArray

    @property
    def num_objects(self) -> int:
        return len(self.scores)


class Sam3BatchResult(BaseModel):
    result_path: str
    runtime_info: dict[str, Any] | None = None


class SubmitTaskResponse(BaseModel):
    task_id: str


class Sam3Resource(ClientResource):
    class TaskPending(Exception): ...

    def submit_image(
        self, image_uri: str, prompt: str, weights_dir_override: Path | None = None
    ) -> SubmitTaskResponse:
        """
        Submit a single image+prompt for SAM3 inference. `image_uri` is loaded
        using smart_open.open() on the target cluster and therefore supports
        local file paths in addition to https and s3 URIs.
        """
        payload = Sam3Request(
            inference_type="single-image",
            data_uri=image_uri,
            single_image_prompt=prompt,
            weights_dir_override=weights_dir_override,
        )
        resp = self._client.post(f"{self.name}/process", json=payload.model_dump())
        resp.raise_for_status()
        return SubmitTaskResponse.model_validate(resp.json())

    def submit_batch(
        self, tar_path: str, weights_dir_override: Path | None = None
    ) -> SubmitTaskResponse:
        """
        Submit a batched inference request, providing the path to a WebDataset
        structured tar file containing the images and prompts.
        """
        payload = Sam3Request(
            inference_type="batch",
            data_uri=tar_path,
            single_image_prompt=None,
            weights_dir_override=weights_dir_override,
        )
        resp = self._client.post(
            f"{self.name}/process", json=payload.model_dump(mode="json")
        )
        resp.raise_for_status()
        return SubmitTaskResponse.model_validate(resp.json())

    def get_task_result(self, task_id: str) -> Sam3ImageResult | Sam3BatchResult:
        """
        Get the result of a submitted SAM3 inference task. Raises
        Sam3Resource.TaskPending if the inference has not yet finished.
        """
        resp = self._client.get(f"{self.name}/tasks/{task_id}")

        if resp.status_code == 202 and b"pending" in resp.content:
            raise Sam3Resource.TaskPending
        elif resp.status_code >= 400:
            resp.raise_for_status()

        result = resp.json().get("result")
        if result and "scores" in result:
            return Sam3ImageResult.model_validate(result)
        elif result and "result_path" in result:
            return Sam3BatchResult.model_validate(result)

        raise RuntimeError(f"Unexpected SAM3 Inference response: {resp}")

    def poll_task_result(
        self, task_id: str, timeout: int = 300
    ) -> Sam3ImageResult | Sam3BatchResult:
        """
        Poll on the SAM3 inference task for up to `timeout` seconds.
        """
        start = time.monotonic()
        logger.info(f"Polling on inference {task_id=}")
        while time.monotonic() - start < timeout:
            try:
                return self.get_task_result(task_id)
            except Sam3Resource.TaskPending:
                time.sleep(1)
        raise TimeoutError(f"{task_id=} not finished in {timeout=}")
