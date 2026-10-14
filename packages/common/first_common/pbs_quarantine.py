"""Shared attribution and accepted-request checks for PBS quarantine jobs."""

import re
from dataclasses import dataclass
from typing import Any

from .schema.types import QUARANTINE_JOB_PREFIX

ACTIVE_STATES = frozenset("QHWTRBSE")
EXCLUSIVE_PLACEMENT = frozenset(
    {
        "exclhost",
        "scatter:exclhost",
        "pack:exclhost",
        "excl",
        "scatter:excl",
        "scatter:excl:group=tier0",
    }
)
MAX_WALLTIME_SECONDS = 7 * 24 * 60 * 60
TAG = "FIRST_QUAR_"


def physical_host(value: Any) -> str:
    """Canonical physical hostname, never a vnode/selector expression."""
    if not isinstance(value, str):
        raise RuntimeError("physical hostname is unavailable")
    short = value.rstrip(".").lower().split(".", 1)[0]
    if re.fullmatch(r"[a-z][a-z0-9-]{0,62}", short) is None:
        raise RuntimeError("invalid physical hostname")
    return short


def pbs_job_id(value: Any) -> str:
    if (
        not isinstance(value, str)
        or len(value) > 128
        or re.fullmatch(r"[1-9][0-9]*\.[A-Za-z0-9][A-Za-z0-9.-]{0,110}", value) is None
    ):
        raise RuntimeError("invalid PBS job identity")
    return value


def positive_int(value: Any) -> int:
    if type(value) is int and value > 0:
        return value
    if isinstance(value, str) and re.fullmatch(r"[1-9][0-9]*", value):
        return int(value)
    raise RuntimeError("invalid PBS resource quantity")


def job_owner(job: dict[str, Any]) -> str:
    value = job.get("Job_Owner")
    if (
        not isinstance(value, str)
        or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]*@[A-Za-z0-9.-]+", value) is None
    ):
        raise RuntimeError("PBS job ownership is unavailable")
    return value.split("@", 1)[0]


def select_resources(value: Any) -> dict[str, str]:
    if not isinstance(value, str) or len(value) > 4096:
        raise RuntimeError("PBS select request is unavailable")
    fields = value.split(":")
    if fields.pop(0) != "1":
        raise RuntimeError("quarantine must request exactly one physical host")
    result: dict[str, str] = {}
    for field in fields:
        match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)=([A-Za-z0-9_.-]+)", field)
        if match is None or match[1] in result:
            raise RuntimeError("ambiguous PBS select request")
        result[match[1]] = match[2]
    return result


def execution_vnodes(value: Any) -> list[tuple[str, dict[str, str]]]:
    if not isinstance(value, str) or not 0 < len(value) <= 65536:
        raise RuntimeError("physical allocation membership is unavailable")
    if not value.startswith("(") or not value.endswith(")"):
        raise RuntimeError("malformed physical allocation membership")
    result = []
    for chunk in value[1:-1].split(")+("):
        for vnode in chunk.split("+"):
            fields = vnode.split(":")
            host = fields.pop(0)
            if physical_host(host) != host or not fields:
                raise RuntimeError("noncanonical physical allocation vnode")
            result.append((host, select_resources("1:" + ":".join(fields))))
    return result


@dataclass(frozen=True)
class QuarantineRequest:
    job_id: str
    host: str
    environment: str
    cluster: str
    operation: str
    source: str
    reason: str
    evidence: str
    state: str
    ncpus: int
    ngpus: int
    walltime_seconds: int


def quarantine_request(
    job_id: str,
    metadata: dict[str, Any],
    state: Any,
    ncpus: Any,
    ngpus: Any,
    walltime_seconds: int,
) -> QuarantineRequest:
    """Validate minimal attribution shared by native and GraphQL PBS observations."""
    environment, host = metadata.get(TAG + "ENV"), metadata.get(TAG + "HOST")
    operation = metadata.get(TAG + "OP")
    if environment not in {"dev", "prod"} or physical_host(host) != host:
        raise RuntimeError("quarantine environment/physical host is unavailable")
    reason, evidence = metadata.get(TAG + "REASON"), metadata.get(TAG + "EVIDENCE")
    for value in (operation, reason, evidence):
        if (
            not isinstance(value, str)
            or not value
            or any(char in value for char in ",\n\r\x00")
        ):
            raise RuntimeError("quarantine attribution is unavailable or not CSV-safe")
    if (
        type(walltime_seconds) is not int
        or not 0 < walltime_seconds <= MAX_WALLTIME_SECONDS
    ):
        raise RuntimeError("quarantine walltime exceeds the qualified site limit")
    if not isinstance(state, str) or state not in ACTIVE_STATES | {"F", "X"}:
        raise RuntimeError("quarantine state is unavailable")
    return QuarantineRequest(
        pbs_job_id(job_id),
        host,
        environment,
        "tara",
        str(operation),
        pbs_job_id(metadata.get(TAG + "SOURCE")),
        str(reason),
        str(evidence),
        state,
        positive_int(ncpus),
        positive_int(ngpus),
        walltime_seconds,
    )


def validate_quarantine(
    job_id: str, job: dict[str, Any], expected_owner: str
) -> QuarantineRequest:
    """Names aid discovery; owner, attribution and accepted resources are authority."""
    if job_owner(job) != expected_owner:
        raise RuntimeError("quarantine is not service-owned")
    meta, resources = job.get("Variable_List"), job.get("Resource_List")
    if not isinstance(meta, dict) or not isinstance(resources, dict):
        raise RuntimeError("quarantine attribution/request is unavailable")
    requested = select_resources(resources.get("select"))
    if (
        requested.get("host") != meta.get(TAG + "HOST")
        or resources.get("place") not in EXCLUSIVE_PLACEMENT
    ):
        raise RuntimeError("quarantine is not an exact-host exclusive request")
    walltime = resources.get("walltime")
    if (
        not isinstance(walltime, str)
        or re.fullmatch(r"[0-9]+:[0-5][0-9]:[0-5][0-9]", walltime) is None
    ):
        raise RuntimeError("quarantine walltime is unavailable")
    hours, minutes, seconds = map(int, walltime.split(":"))
    return quarantine_request(
        job_id,
        meta,
        job.get("job_state"),
        requested.get("ncpus"),
        requested.get("ngpus"),
        hours * 3600 + minutes * 60 + seconds,
    )


def discover_quarantines(
    jobs: dict[str, dict[str, Any]], expected_owner: str
) -> list[QuarantineRequest]:
    """Ignore unrelated/foreign jobs; incomplete service-owned requests are errors."""
    result = []
    for job_id, job in jobs.items():
        name, meta = job.get("Job_Name"), job.get("Variable_List")
        tagged = isinstance(meta, dict) and any(
            str(key).startswith(TAG) for key in meta
        )
        if not tagged and (
            not isinstance(name, str) or not name.startswith(QUARANTINE_JOB_PREFIX)
        ):
            continue
        if job_owner(job) != expected_owner:
            continue
        request = validate_quarantine(job_id, job, expected_owner)
        if request.state in ACTIVE_STATES:
            result.append(request)
    return result
