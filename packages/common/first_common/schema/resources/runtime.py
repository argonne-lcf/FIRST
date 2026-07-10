"""
High-churn runtime state (sourced from Redis).

We separate the runtime state and include it as a nested submodel of read
models, so that ORM/Redis data can be fetched indepdently and composed as
needed.
"""

from datetime import datetime
from typing import NamedTuple

from pydantic import BaseModel


class ReplicaRuntime(BaseModel):
    inflight: int = 0
    cooldown_errors: int = 0


class ModelRuntime(BaseModel):
    total_inflight: int = 0
    capacity_rejects_total: int = 0
    last_capacity_reject: datetime | None = None


class ScaledownCandidate(NamedTuple):
    num_replicas: int
    starting_from: datetime


class AutoscalerRuntime(BaseModel):
    demand_ewma: dict[str, float]
    scale_down_candidates: dict[str, list[ScaledownCandidate]]
