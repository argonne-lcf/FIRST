import uuid
from dataclasses import dataclass
from typing import Any

from django.http import StreamingHttpResponse
from pydantic import BaseModel, Field

from resource_server_async.schemas.batch import BatchStatus


class FrameworkSummary(BaseModel):
    models: list[str]
    endpoints: list[str]


class ClusterSummary(BaseModel):
    base_url: str
    frameworks: dict[str, FrameworkSummary]


class ListEndpointsResponse(BaseModel):
    clusters: dict[str, ClusterSummary]


class SubmitTaskAsyncResponse(BaseModel):
    task_id: str


class SubmitTaskResult(BaseModel):
    result: Any
    task_id: str | None


@dataclass
class SubmitStreamingTaskResponse:
    response: StreamingHttpResponse
    task_id: str | None


class SubmitBatchResult(BaseModel):
    batch_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    input_file: str
    output_folder_path: str | None = None
    task_ids: str | None = None
    status: BatchStatus = Field(default=BatchStatus.failed)


class BatchStatusResult(BaseModel):
    status: BatchStatus
    result: str | None


class BatchResultMetrics(BaseModel):
    response_time: float
    throughput_tokens_per_second: float
    total_tokens: int
    num_responses: int
    lines_processed: int
