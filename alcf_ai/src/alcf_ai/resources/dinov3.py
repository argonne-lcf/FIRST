import logging
import time
from typing import Any

from pydantic import BaseModel

from .resource import ClientResource

logger = logging.getLogger(__name__)


class DINOv3Request(BaseModel):
    input_dir: str
    project: str | None = None
    checkpoint: str | None = None
    save_overlay: bool | None = None
    batch_size: int | None = None
    num_workers: int | None = None


class SubmitTaskResponse(BaseModel):
    task_id: str


class DINOv3Resource(ClientResource):
    class TaskPending(Exception): ...

    def submit(
        self,
        input_dir: str,
        project: str | None = None,
        checkpoint: str | None = None,
        save_overlay: bool | None = None,
        batch_size: int | None = None,
        num_workers: int | None = None,
    ) -> SubmitTaskResponse:
        """
        Submit a DINOv3 segmentation request. ``input_dir`` is a filesystem path
        on the compute node under the staging collection root; the GPU
        dataloader batches over all images found in it. The results directory is
        derived server-side (a sibling of ``input_dir`` within your staging
        area) and reported back as ``mask_dir`` in the task result.
        """
        payload = DINOv3Request(
            input_dir=input_dir,
            project=project,
            checkpoint=checkpoint,
            save_overlay=save_overlay,
            batch_size=batch_size,
            num_workers=num_workers,
        )
        resp = self._client.post(
            f"{self.name}/process",
            json=payload.model_dump(mode="json", exclude_none=True),
        )
        resp.raise_for_status()
        return SubmitTaskResponse.model_validate(resp.json())

    def get_task_result(self, task_id: str) -> dict[str, Any]:
        """
        Get the result of a submitted inference task. Raises
        DINOv3Resource.TaskPending if the inference has not yet finished.

        Returns
        ``{"status": ..., "num_images": ..., "mask_dir": ...,
        "overlay_dir": ..., "elapsed_sec": ...}``
        """
        resp = self._client.get(f"{self.name}/tasks/{task_id}")

        if resp.status_code == 202 and b"pending" in resp.content:
            raise DINOv3Resource.TaskPending
        elif resp.status_code >= 400:
            resp.raise_for_status()

        result: dict[str, Any] = resp.json().get("result")
        if result is not None:
            return result

        raise RuntimeError(f"Unexpected DINOv3 inference response: {resp}")

    def poll_task_result(self, task_id: str, timeout: int = 300) -> dict[str, Any]:
        """
        Poll on the inference task for up to ``timeout`` seconds.
        """
        start = time.monotonic()
        logger.info(f"Polling on inference {task_id=}")
        while time.monotonic() - start < timeout:
            try:
                return self.get_task_result(task_id)
            except DINOv3Resource.TaskPending:
                time.sleep(1)
        raise TimeoutError(f"{task_id=} not finished in {timeout=}")
