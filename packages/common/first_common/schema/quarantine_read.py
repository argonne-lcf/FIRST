"""Administrative views/actions; no scheduling or model configuration fields."""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class NodeQuarantineSummary(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    uid: int
    cluster_name: str
    host: str
    environment: str
    operation: str
    state: Literal["pending", "isolated", "failed", "lost", "released"]
    state_message: str
    origin: dict[str, Any]
    fault: dict[str, Any]
    evidence: dict[str, Any]
    job_identity: dict[str, Any] | None
    expires_at: datetime | None
    repair_reference: str | None
    screen_nonce: str | None
    screen_requested_at: datetime | None
    release_requested_at: datetime | None
    submission_attempts: int
    submission_uncertain: bool


class QuarantineOperatorAction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reference: str = Field(
        min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$"
    )
