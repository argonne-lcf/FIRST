"""Pilot admission with bounded native PBS requests; normal retries are unchanged."""

import json
import logging
import os
import pwd
import subprocess
import time
import uuid
from dataclasses import dataclass
from typing import Any

from first_common.pbs_quarantine import (
    TAG,
    QuarantineRequest,
    discover_quarantines,
    execution_vnodes,
    job_owner,
    pbs_job_id,
    physical_host,
    positive_int,
)
from first_common.schema.pilot import PilotRuntimeConfig
from first_common.schema.types import QUARANTINE_JOB_PREFIX

from .host_commands import deduplicate_hosts
from .node_checks import run_node_checks
from .replica_manager import discover_hosts

logger = logging.getLogger(__name__)
_PBS_TIMEOUT_SEC = 15.0
_ACTION_BUDGET_SEC = 90.0


class NodeChecksFailed(RuntimeError):
    """A verified node fault was observed; the pilot must not start."""


class AdmissionDeferredKnownQuarantine(RuntimeError):
    """A known request targets this allocation; no new fault is diagnosed."""


def _pbs(command: list[str], *, timeout: float = _PBS_TIMEOUT_SEC) -> str:
    return subprocess.run(
        command, capture_output=True, text=True, check=True, timeout=timeout
    ).stdout


def _call(command: list[str], deadline: float) -> str:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise RuntimeError("native PBS quarantine operation deadline expired")
    return _pbs(command, timeout=min(_PBS_TIMEOUT_SEC, remaining))


def _json_rows(output: str, field: str) -> dict[str, dict[str, Any]]:
    parsed = json.loads(output)
    rows = parsed.get(field) if isinstance(parsed, dict) else None
    if not isinstance(rows, dict) or any(
        not isinstance(key, str) or not isinstance(value, dict)
        for key, value in rows.items()
    ):
        raise RuntimeError(f"native PBS {field} inventory is incomplete")
    return rows


def _jobs(deadline: float) -> dict[str, dict[str, Any]]:
    return _json_rows(_call(["qstat", "-f", "-F", "json"], deadline), "Jobs")


def _owner() -> str:
    return pwd.getpwuid(os.getuid()).pw_name


@dataclass
class _Allocation:
    source: str
    resources: dict[str, tuple[int, int]]
    quarantines: list[QuarantineRequest]


def _allocation(
    config: PilotRuntimeConfig, hosts: list[str], deadline: float
) -> _Allocation:
    jobs = _jobs(deadline)
    source = pbs_job_id(os.environ.get("PBS_JOBID"))
    job = jobs.get(source)
    if (
        not isinstance(job, dict)
        or job_owner(job) != _owner()
        or job.get("job_state") != "R"
    ):
        raise RuntimeError(
            "originating PBS allocation ownership/execution is unverified"
        )
    vnodes = execution_vnodes(job.get("exec_vnode"))
    canonical = {physical_host(host) for host in hosts}
    resources = {
        host: (positive_int(attrs.get("ncpus")), positive_int(attrs.get("ngpus")))
        for host, attrs in vnodes
    }
    if len(resources) != len(vnodes) or len(canonical) != config.num_nodes:
        raise RuntimeError("physical allocation node count is incoherent")
    if len(resources) != config.num_nodes or set(resources) != canonical:
        raise RuntimeError("node file does not match originating PBS allocation")
    if any(ngpus != config.gpus_per_node for _, ngpus in resources.values()):
        raise RuntimeError("PBS GPU allocation differs from pilot configuration")
    return _Allocation(source, resources, discover_quarantines(jobs, _owner()))


def guard_quarantined_nodes(
    config: PilotRuntimeConfig, hosts: list[str] | None = None
) -> _Allocation | None:
    """Reject pending/held/running requests before model initialization."""
    if config.node_admission.on_failure != "pbs_quarantine":
        return None
    if hosts is None:
        hosts = deduplicate_hosts(discover_hosts(config.node_file_env))
    allocation = _allocation(config, hosts, time.monotonic() + _ACTION_BUDGET_SEC)
    matching = [
        row for row in allocation.quarantines if row.host in allocation.resources
    ]
    if matching:
        identities = [(row.host, row.job_id, row.environment) for row in matching]
        raise AdmissionDeferredKnownQuarantine(
            f"admission_deferred_known_quarantine: {identities}"
        )
    return allocation


def _evidence(
    config: PilotRuntimeConfig,
    failures: dict[str, list[str]],
    observations: dict[str, object] | None = None,
) -> str:
    config.workdir.mkdir(parents=True, exist_ok=True)
    path = config.workdir / f"node-admission-{uuid.uuid4().hex}.json"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump(
            {
                "source": os.environ.get("PBS_JOBID"),
                "faults": failures,
                "environment": config.node_admission.environment
                or os.environ.get("TARA_DEPLOYMENT_ENV"),
                "observations": observations,
                "recorded_unix": time.time(),
            },
            stream,
        )
    return str(path.resolve())


def admit_nodes(config: PilotRuntimeConfig) -> None:
    """Guard, screen, submit bounded fault actions, then end the failed attempt."""
    admission = config.node_admission
    if not admission.checks and admission.on_failure != "pbs_quarantine":
        return
    hosts = deduplicate_hosts(discover_hosts(config.node_file_env))
    allocation = guard_quarantined_nodes(config, hosts)
    if not admission.checks:
        return
    observations: dict[str, object] = {}
    failures = run_node_checks(admission.checks, hosts, config, evidence=observations)
    if not failures:
        logger.info("all %d nodes passed %s", len(hosts), admission.checks)
        return
    if not set(failures).issubset(hosts):
        raise RuntimeError("screening faults refer to nodes outside the allocation")
    evidence = _evidence(config, failures, observations)
    deadline = time.monotonic() + _ACTION_BUDGET_SEC
    if allocation is not None:
        allocation.quarantines = discover_quarantines(_jobs(deadline), _owner())
    for hostname, reasons in failures.items():
        logger.error(
            "node %s failed checks: %s; evidence=%s", hostname, reasons, evidence
        )
        if admission.on_failure == "pbs_quarantine":
            try:
                pbs_quarantine(
                    hostname,
                    config,
                    reason=reasons[0].split(":", 1)[0],
                    evidence=evidence,
                    deadline=deadline,
                    allocation=allocation,
                )
            except Exception:
                logger.exception(
                    "quarantine action failed for %s; ordinary pilot exit continues",
                    hostname,
                )
    raise NodeChecksFailed(
        f"nodes failed startup checks: {sorted(failures)}; evidence={evidence}"
    )


def pbs_quarantine(
    hostname: str,
    config: PilotRuntimeConfig,
    *,
    reason: str = "node_screen_failed",
    evidence: str | None = None,
    deadline: float | None = None,
    allocation: _Allocation | None = None,
) -> None:
    """Submit at most once, inspect accepted identity, never wait for execution."""
    if not config.queue or not config.account:
        raise ValueError("pbs_quarantine requires the pilot's queue and account")
    deadline = time.monotonic() + _ACTION_BUDGET_SEC if deadline is None else deadline
    if allocation is None:
        allocation = _allocation(
            config, deduplicate_hosts(discover_hosts(config.node_file_env)), deadline
        )
    host = physical_host(hostname)
    if host not in allocation.resources:
        raise RuntimeError("fault host was not in the originating allocation")
    existing = [row for row in allocation.quarantines if row.host == host]
    if existing:
        logger.warning(
            "existing quarantine for %s: %s", host, [row.job_id for row in existing]
        )
        return
    if (
        len({row.host for row in allocation.quarantines})
        >= config.node_admission.max_quarantined_nodes
    ):
        raise RuntimeError(
            "quarantine safety budget reached; ordinary retries remain active"
        )
    environment: str | None = config.node_admission.environment
    runtime_environment = os.environ.get("TARA_DEPLOYMENT_ENV")
    if runtime_environment and environment and runtime_environment != environment:
        raise RuntimeError(
            "pilot quarantine environment disagrees with deployment context"
        )
    environment = runtime_environment or environment
    operation = uuid.uuid4().hex
    if evidence is None:
        evidence = _evidence(config, {hostname: [reason]})
    metadata = {
        TAG + "ENV": environment,
        TAG + "HOST": host,
        TAG + "OP": operation,
        TAG + "SOURCE": allocation.source,
        TAG + "REASON": reason,
        TAG + "EVIDENCE": evidence,
    }
    ncpus, ngpus = allocation.resources[host]
    seconds = config.node_admission.quarantine_walltime_min * 60
    if any(
        "," in str(value) or "\n" in str(value) or "\r" in str(value)
        for value in metadata.values()
    ):
        raise ValueError("quarantine qsub variables contain a delimiter")
    name = f"{QUARANTINE_JOB_PREFIX}{environment}_{host}_{operation[:8]}"
    command = [
        "qsub",
        "-N",
        name,
        "-q",
        config.queue,
        "-A",
        config.account,
        "-l",
        f"select=1:host={host}:ncpus={ncpus}:ngpus={ngpus}",
        "-l",
        "place=exclhost",
        "-l",
        f"walltime={seconds // 3600}:{seconds // 60 % 60:02}:00",
        "-v",
        ",".join(f"{key}={value}" for key, value in metadata.items()),
        "-j",
        "oe",
        "-o",
        "/dev/null",
        "--",
        "/bin/sleep",
        "infinity",
    ]
    acknowledged = None
    try:
        acknowledged = pbs_job_id(_call(command, deadline).strip())
    except (subprocess.SubprocessError, OSError, RuntimeError):
        logger.warning(
            "qsub acknowledgement ambiguous for %s; lookup only",
            operation,
            exc_info=True,
        )
    for attempt in range(3):
        try:
            current = discover_quarantines(_jobs(deadline), _owner())
        except (subprocess.SubprocessError, OSError):
            logger.warning("quarantine acknowledgement lookup failed", exc_info=True)
            current = []
        matches = [row for row in current if row.operation == operation]
        if len(matches) > 1:
            raise RuntimeError(
                "duplicate quarantine operation requires operator inspection"
            )
        if matches:
            accepted = matches[0]
            if accepted.host != host or (
                acknowledged is not None and accepted.job_id != acknowledged
            ):
                raise RuntimeError(
                    "PBS accepted identity differs from intended quarantine request"
                )
            allocation.quarantines.append(accepted)
            logger.warning(
                "isolation pending: host=%s quarantine=%s operation=%s source=%s evidence=%s",
                host,
                accepted.job_id,
                operation,
                allocation.source,
                evidence,
            )
            return
        if attempt < 2 and deadline - time.monotonic() > 1:
            time.sleep(1)
    raise RuntimeError(
        "quarantine acceptance unresolved; ordinary pilot exit/retry continues"
    )
