import logging
import time
from typing import Any

from pydantic import BaseModel

from .resource import ClientResource

logger = logging.getLogger(__name__)


class D3TritonRequest(BaseModel):
    model_name: str
    input_path: str
    output_path: str
    outputs: list[str] | None = None


class SubmitTaskResponse(BaseModel):
    task_id: str


class D3TritonResource(ClientResource):
    class TaskPending(Exception): ...

    def submit(
        self,
        model_name: str,
        input_path: str,
        output_path: str,
        outputs: list[str] | None = None,
    ) -> SubmitTaskResponse:
        """
        Submit a Triton HEP inference request. The input_path and output_path
        are filesystem paths on the compute node (typically under the staging
        collection root).
        """
        payload = D3TritonRequest(
            model_name=model_name,
            input_path=input_path,
            output_path=output_path,
            outputs=outputs,
        )
        resp = self._client.post(
            f"{self.name}/process", json=payload.model_dump(mode="json")
        )
        resp.raise_for_status()
        return SubmitTaskResponse.model_validate(resp.json())

    def get_task_result(self, task_id: str) -> dict[str, Any]:
        """
        Get the result of a submitted inference task. Raises
        D3TritonResource.TaskPending if the inference has not yet finished.

        Returns the dict produced by the Globus Compute function:
        ``{"model_name": ..., "output_path": ...}``
        """
        resp = self._client.get(f"{self.name}/tasks/{task_id}")

        if resp.status_code == 202 and b"pending" in resp.content:
            raise D3TritonResource.TaskPending
        elif resp.status_code >= 400:
            resp.raise_for_status()

        result: dict[str, Any] = resp.json().get("result")
        if result is not None:
            return result

        raise RuntimeError(f"Unexpected D3 Triton inference response: {resp}")

    def poll_task_result(
        self, task_id: str, timeout: int = 300
    ) -> dict[str, Any]:
        """
        Poll on the inference task for up to ``timeout`` seconds.
        """
        start = time.monotonic()
        logger.info(f"Polling on inference {task_id=}")
        while time.monotonic() - start < timeout:
            try:
                return self.get_task_result(task_id)
            except D3TritonResource.TaskPending:
                time.sleep(1)
        raise TimeoutError(f"{task_id=} not finished in {timeout=}")
