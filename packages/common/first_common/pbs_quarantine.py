"""Shared attribution and accepted-request checks for PBS quarantine jobs."""

import re
from dataclasses import dataclass
from typing import Any

from .schema.types import QUARANTINE_JOB_PREFIX

ACTIVE_STATES = frozenset("QHWTRBSE")
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


def exclusive_placement(value: Any) -> bool:
    """Check PBS sharing, without prescribing site-specific resource groups.

    PBS permits arrangement, sharing and grouping in any order. Native jobs
    request ``exclhost``; some sites normalize it to ``excl``. That is sufficient
    only for the supported one-physical-vnode-per-host allocation layout.
    """
    if not isinstance(value, str) or not 0 < len(value) <= 4096:
        return False
    seen = set()
    for token in value.split(":"):
        if token in {"free", "pack", "scatter", "vscatter"}:
            category = "arrangement"
        elif token in {"excl", "exclhost"}:
            category = "exclusive"
        elif re.fullmatch(r"group=[A-Za-z_][A-Za-z0-9_]*", token):
            category = "grouping"
        else:
            return False
        if category in seen:
            return False
        seen.add(category)
    return "exclusive" in seen


def execution_vnodes(value: Any) -> list[tuple[str, dict[str, str]]]:
    """Parse allocation entries whose vnode names are physical hostnames.

    This native quarantine path does not resolve logical vnodes through PBS
    MOM mappings. Callers must check node-file membership and resource counts;
    the site's one-physical-vnode-per-host layout must be established separately.
    Mismatching names or resource counts are infrastructure errors, not faults.
    """
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
    operation: str
    environment: Any
    source: Any
    reason: Any
    evidence: Any


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
    host = physical_host(requested.get("host"))
    if (
        requested.get("host") != host
        or meta.get(TAG + "HOST") != host
        or not exclusive_placement(resources.get("place"))
    ):
        raise RuntimeError("quarantine is not an exact-host exclusive request")
    operation = meta.get(TAG + "OP")
    if not isinstance(operation, str) or not operation:
        raise RuntimeError("quarantine operation identity is unavailable")
    return QuarantineRequest(
        pbs_job_id(job_id),
        host,
        operation,
        meta.get(TAG + "ENV"),
        meta.get(TAG + "SOURCE"),
        meta.get(TAG + "REASON"),
        meta.get(TAG + "EVIDENCE"),
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
        state = job.get("job_state")
        if not isinstance(state, str) or state not in ACTIVE_STATES | {"F", "X"}:
            raise RuntimeError("quarantine state is unavailable")
        if state not in ACTIVE_STATES:
            continue
        request = validate_quarantine(job_id, job, expected_owner)
        result.append(request)
    return result
