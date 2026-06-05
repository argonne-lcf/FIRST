from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from ninja import FilterSchema
from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator


# Batch status
class BatchStatus(str, Enum):
    pending = "pending"
    running = "running"
    failed = "failed"
    completed = "completed"


class BatchLogSummary(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: str
    cluster: str
    framework: str
    input_file: str
    in_progress_at: datetime | None
    completed_at: datetime | None
    failed_at: datetime | None
    status: BatchStatus

    @computed_field  # type: ignore[prop-decorator]
    @property
    def batch_id(self) -> str:
        return self.id

    @field_validator("id", mode="before")
    @classmethod
    def coerce_uuid(cls, v: Any) -> Any:
        if isinstance(v, UUID):
            return str(v)
        return v


class BatchListFilter(FilterSchema):
    status: BatchStatus | None = None


class BatchSubmit(BaseModel):
    model_config = ConfigDict(extra="forbid")
    input_file: str = Field(..., min_length=1)
    model: str = Field(..., min_length=1)
    output_folder_path: str | None = None
