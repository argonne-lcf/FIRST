"""
High-churn runtime state (sourced from Redis).

We separate the runtime state and include it as a nested submodel of read
models, so that ORM/Redis data can be fetched indepdently and composed as
needed.
"""

from datetime import datetime
from typing import Literal, NamedTuple

from pydantic import BaseModel

from ..pilot import PilotResources

Severity = Literal["info", "warn", "crit"]


class BackendRuntime(BaseModel):
    inflight: int = 0
    cooldown_errors: int = 0


class ModelRuntime(BaseModel):
    total_inflight: int = 0
    capacity_rejects_total: int = 0
    last_capacity_reject: datetime | None = None


class RejectSample(NamedTuple):
    ts: datetime
    rejects_total: int


class ScaledownCandidate(NamedTuple):
    num_replicas: int
    starting_from: datetime


class AutoscalerModelRuntime(BaseModel):
    """Per-model autoscaler state (one Redis key per model)."""

    demand_ewma: float = 0.0
    reject_window: list[RejectSample] = []
    # keyed by deployment name — deployments scale independently
    scale_down_candidates: dict[str, list[ScaledownCandidate]] = {}


class StagedTransition(BaseModel):
    status: str  # target status being confirmed ("" == recovering to ok)
    severity: Severity = "crit"
    summary: str = ""
    group: str = ""  # Slack category, carried from the Observation
    owner: str = ""  # check function that produced this, for recovery scoping
    first_seen: datetime  # debounce timer start (reset when target changes)


class CommittedAlert(BaseModel):
    status: str
    severity: Severity = "crit"
    group: str = ""
    owner: str = ""  # check function that owns this key


class HealthAlertState(BaseModel):
    """Singleton Redis blob for the health alerter (one key, not per-resource)."""

    committed: dict[str, CommittedAlert] = {}
    staging: dict[str, StagedTransition] = {}
    last_daily_report: str | None = None  # "YYYY-MM-DD" (UTC) digest dedup
    reported_failures: dict[str, str] = {}  # check_function -> error message


class PilotJobRuntime(BaseModel):
    resources: PilotResources = PilotResources(hosts=[])
