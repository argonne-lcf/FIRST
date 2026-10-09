"""Read the Tara coordinator's held PBS registry through the existing adapter.

PBS carries a bounded read-only status mirror, not the authoritative state.
Missing or invalid mirrors never imply repair. No raw PBS environment is logged.
"""

import base64
import binascii
import json
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from first_common.schema.base_scheduler import JobStatusInfo, SchedulerJobState
from first_common.schema.resources.runtime import HealthAlertState, Severity

from .types import Observation

REGISTRY_NAME = "first_tara_quarantine_registry"
_MAX_STATUS_BYTES = 32768
_MAX_AGE_S = 180


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate quarantine JSON key")
        result[key] = value
    return result


class QuarantineRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    host: str = Field(pattern=r"^x(?:4818|4819|4820)c[0-7]s[0-7]b[01]n[0-3]$")
    origin_job: str = Field(min_length=1, max_length=128)
    environment: Literal["dev", "prod"]
    code: Literal["missing_gpu", "unexplained_gpu_memory_loss", "uncorrected_ecc"]
    reason: str = Field(min_length=1, max_length=300)
    evidence: str = Field(min_length=1, max_length=2048)
    quarantine_job_id: str | None = Field(default=None, max_length=128)
    expires_unix: int | None = Field(default=None, gt=0)
    status: Literal["pending", "isolated", "failed", "lost", "released"]
    last_transition_unix: int | None = Field(default=None, ge=0)


class QuarantineSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal[1]
    generation: int = Field(ge=0)
    updated_unix: int = Field(gt=0)
    notification_environment: Literal["dev", "prod"]
    gate_closed: bool
    gate_error: str | None = Field(default=None, max_length=240)
    records: list[QuarantineRecord] = Field(max_length=128)


def verify_quarantine_history(
    state: HealthAlertState, observations: list[Observation]
) -> None:
    """Do not turn a disappearing unresolved record into a health recovery."""
    observed_keys = {observation.key for observation in observations}
    for key, previous in state.committed.items():
        if (
            previous.owner == "check_tara_quarantine"
            and key.startswith("tara/quarantine/x")
            and previous.status != "released"
            and key not in observed_keys
        ):
            raise RuntimeError(
                "Tara quarantine registry omitted an unresolved node; no recovery inferred"
            )


def validated_quarantine_snapshot(
    jobs: list[JobStatusInfo], now: int
) -> QuarantineSnapshot:
    """Validate the shared protocol mirror independently of its alert owner."""
    registries = [job for job in jobs if job.name == REGISTRY_NAME]
    if len(registries) != 1:
        raise RuntimeError(
            "Tara quarantine registry missing or duplicated; isolation unverified"
        )
    registry = registries[0]
    if (
        registry.owner != "openinference_svc"
        or registry.state != SchedulerJobState.queued
        or registry.hold_type != "u"
        or registry.coordination_env.get("FIRST_QUARANTINE_VERSION") != "1"
        or registry.coordination_env.get("FIRST_QUARANTINE_KIND") != "registry"
    ):
        raise RuntimeError(
            "Tara quarantine registry identity or user hold is unverified"
        )
    raw = registry.coordination_env.get("FIRST_QUARANTINE_STATUS", "")
    if not raw or len(raw) > _MAX_STATUS_BYTES:
        raise RuntimeError("Tara quarantine registry status is missing or oversized")
    try:
        decoded = base64.b64decode(raw, altchars=b"-_", validate=True)
        if len(decoded) > _MAX_STATUS_BYTES:
            raise ValueError("oversized decoded status")
        snapshot = QuarantineSnapshot.model_validate(
            json.loads(decoded, object_pairs_hook=_unique_json_object)
        )
    except (ValueError, binascii.Error) as exc:
        raise RuntimeError("Tara quarantine registry status is invalid") from exc
    if not 0 <= now - snapshot.updated_unix <= _MAX_AGE_S:
        raise RuntimeError(
            "Tara quarantine registry heartbeat stale; isolation unverified"
        )
    hosts = [record.host for record in snapshot.records]
    if len(hosts) != len(set(hosts)):
        raise RuntimeError("Tara quarantine registry contains duplicate physical hosts")
    for record in snapshot.records:
        if record.status == "isolated" and (
            not record.quarantine_job_id
            or record.expires_unix is None
            or record.expires_unix <= now
        ):
            raise RuntimeError(
                f"Tara quarantine expiration/identity unverified for {record.host}"
            )
    return snapshot


def quarantine_job_disposition(
    snapshot: QuarantineSnapshot | None, job_id: str
) -> Literal["ordinary", "blocked", "isolated"]:
    """Only typed, completely isolated node faults bypass model-failure charge."""
    if snapshot is None:
        return "ordinary"
    records = [record for record in snapshot.records if record.origin_job == job_id]
    if not records:
        return "ordinary"
    if any(record.status in {"pending", "failed", "lost"} for record in records):
        return "blocked"
    return (
        "isolated"
        if all(record.status == "isolated" for record in records)
        else "ordinary"
    )


def quarantine_observations(
    jobs: list[JobStatusInfo], environment: Literal["dev", "prod"], now: int
) -> list[Observation]:
    """Return stable transitions, preserving failure on missing/stale mirrors."""
    snapshot = validated_quarantine_snapshot(jobs, now)
    # Both controllers can inspect the mirror. Exactly one configured
    # environment emits its transitions through the existing Slack mechanism.
    if snapshot.notification_environment != environment:
        return []
    observations: list[Observation] = []
    for record in snapshot.records:
        severity: Severity = (
            "crit"
            if record.status in {"failed", "lost"}
            else "warn"
            if record.status == "pending"
            else "info"
        )
        state_text = {
            "pending": "fault detected; isolation pending",
            "isolated": "isolation confirmed",
            "failed": "isolation failed",
            "lost": "isolation lost; node has not been declared repaired",
            "released": "repaired, freshly screened and explicitly returned to service",
        }[record.status]
        summary = (
            f"Tara node {record.host}: {state_text}; origin={record.origin_job} "
            f"environment={record.environment}; reason={record.reason}; evidence={record.evidence}"
        )
        if record.quarantine_job_id:
            summary += f"; quarantine={record.quarantine_job_id} expires_unix={record.expires_unix}"
        observations.append(
            Observation(
                key=f"tara/quarantine/{record.host}",
                status=record.status,
                summary=summary,
                severity=severity,
                display_name=f"Tara node {record.host}",
                recovery_hint="explicit repair confirmation, passing screen and quarantine release required",
                debounce_s=1.0,
            )
        )
    if snapshot.gate_closed:
        observations.append(
            Observation(
                key="tara/quarantine/submission-gate",
                status="closed",
                summary=(
                    "Tara new model starts are held pending verified quarantine; healthy serving jobs are untouched"
                    + (
                        f"; coordinator={snapshot.gate_error}"
                        if snapshot.gate_error
                        else ""
                    )
                ),
                severity="warn",
                display_name="Tara quarantine submission gate",
                debounce_s=1.0,
            )
        )
    return observations
