"""Small, shared validation rules for pilot-created native PBS quarantine jobs.

Names aid discovery; ownership, metadata and exact resource requests establish
identity. These rules neither schedule model jobs nor infer hardware recovery.
"""

import re
from dataclasses import dataclass
from typing import Any

from .schema.types import QUARANTINE_JOB_PREFIX

ACTIVE_STATES = frozenset("QHWTRBSE")
PLACED_STATES = frozenset("RBSE")
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
    """Accept a canonical physical hostname, not a vnode/selector expression."""
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
            attrs = select_resources("1:" + ":".join(fields))
            result.append((host, attrs))
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


def validate_quarantine(
    job_id: str, job: dict[str, Any], expected_owner: str
) -> QuarantineRequest:
    """Validate a discovered request, including PBS's accepted identity."""
    pbs_job_id(job_id)
    if job_owner(job) != expected_owner:
        raise RuntimeError("quarantine is not service-owned")
    meta, resources = job.get("Variable_List"), job.get("Resource_List")
    if not isinstance(meta, dict) or not isinstance(resources, dict):
        raise RuntimeError("quarantine attribution/request is unavailable")
    if str(meta.get(TAG + "VERSION")) != "1":
        raise RuntimeError("unsupported quarantine metadata version")
    environment, cluster = meta.get(TAG + "ENV"), meta.get(TAG + "CLUSTER")
    host, operation = meta.get(TAG + "HOST"), meta.get(TAG + "OP")
    if environment not in {"dev", "prod"} or cluster != "tara":
        raise RuntimeError("quarantine environment/cluster is unavailable")
    if physical_host(host) != host or not isinstance(operation, str):
        raise RuntimeError("quarantine host/operation is unavailable")
    if re.fullmatch(r"[0-9a-f]{32}", operation) is None:
        raise RuntimeError("invalid quarantine operation")
    name = f"{QUARANTINE_JOB_PREFIX}{environment}_{host}_{operation[:8]}"
    if len(name) > 63 or job.get("Job_Name") != name:
        raise RuntimeError("quarantine name differs from metadata (possibly truncated)")
    source = pbs_job_id(meta.get(TAG + "SOURCE"))
    reason, evidence = meta.get(TAG + "REASON"), meta.get(TAG + "EVIDENCE")
    if (
        not isinstance(reason, str)
        or re.fullmatch(r"[a-z0-9_-]{1,128}", reason) is None
    ):
        raise RuntimeError("quarantine reason is unavailable")
    if (
        not isinstance(evidence, str)
        or not evidence.startswith("/")
        or any(char in evidence for char in ",\n\r\x00")
        or len(evidence) > 1024
    ):
        raise RuntimeError("quarantine evidence path is unavailable")
    requested = select_resources(resources.get("select"))
    if (
        requested.get("host") != host
        or resources.get("place") not in EXCLUSIVE_PLACEMENT
    ):
        raise RuntimeError("quarantine is not an exact-host exclusive request")
    ncpus, ngpus = (
        positive_int(requested.get("ncpus")),
        positive_int(requested.get("ngpus")),
    )
    walltime = resources.get("walltime")
    if (
        not isinstance(walltime, str)
        or re.fullmatch(r"[0-9]+:[0-5][0-9]:[0-5][0-9]", walltime) is None
    ):
        raise RuntimeError("quarantine walltime is unavailable")
    hours, minutes, seconds = map(int, walltime.split(":"))
    walltime_seconds = hours * 3600 + minutes * 60 + seconds
    if not 0 < walltime_seconds <= MAX_WALLTIME_SECONDS:
        raise RuntimeError("quarantine walltime exceeds the qualified site limit")
    state = job.get("job_state")
    if not isinstance(state, str) or state not in ACTIVE_STATES | {"F", "X"}:
        raise RuntimeError("quarantine state is unavailable")
    return QuarantineRequest(
        job_id,
        host,
        environment,
        cluster,
        operation,
        source,
        reason,
        evidence,
        state,
        ncpus,
        ngpus,
        walltime_seconds,
    )


def discover_quarantines(
    jobs: dict[str, dict[str, Any]], expected_owner: str
) -> list[QuarantineRequest]:
    """Ignore unrelated/foreign jobs, never trust an invalid service-owned name."""
    result = []
    for job_id, job in jobs.items():
        name = job.get("Job_Name")
        meta = job.get("Variable_List")
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


def prove_isolation(
    request: QuarantineRequest,
    job: dict[str, Any],
    jobs: dict[str, dict[str, Any]],
    nodes: dict[str, dict[str, Any]],
) -> bool:
    """Native physical-host proof, not merely an exclusivity request flag."""
    if request.state != "R":
        return False
    vnodes = execution_vnodes(job.get("exec_vnode"))
    if len(vnodes) != 1 or vnodes[0][0] != request.host:
        return False
    physical = [
        row
        for name, row in nodes.items()
        if physical_host(name) == request.host
        or (row.get("resources_available") or {}).get("host") == request.host
    ]
    if len(physical) != 1 or request.host not in nodes:
        return False
    node = physical[0]
    available, assigned = (
        node.get("resources_available"),
        node.get("resources_assigned"),
    )
    if not isinstance(available, dict) or not isinstance(assigned, dict):
        return False
    if available.get("host") != request.host or "job-exclusive" not in str(
        node.get("state", "")
    ).split(","):
        return False
    for resource, expected in (("ncpus", request.ncpus), ("ngpus", request.ngpus)):
        if any(
            positive_int(row.get(resource)) != expected
            for row in (available, assigned, vnodes[0][1])
        ):
            return False
    occupants = set()
    for job_id, other in jobs.items():
        if other.get("job_state") not in ACTIVE_STATES | {"F", "X"}:
            raise RuntimeError("native occupancy census includes an unknown job state")
        if other.get("job_state") not in PLACED_STATES:
            continue
        if request.host in {
            host for host, _ in execution_vnodes(other.get("exec_vnode"))
        }:
            occupants.add(job_id)
    return occupants == {request.job_id}
