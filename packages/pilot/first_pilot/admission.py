"""Pilot admission with bounded native PBS requests; normal retries are unchanged."""

import json
import logging
import os
import pwd
import subprocess
import time
import uuid
from typing import Any, Callable

from first_common.pbs_quarantine import (
    MAX_WALLTIME_SECONDS,
    TAG,
    discover_quarantines,
    execution_vnodes,
    job_owner,
    pbs_job_id,
    physical_host,
    positive_int,
    validate_quarantine,
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
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise RuntimeError("duplicate native PBS JSON key")
            result[key] = value
        return result

    if len(output) > 16 * 1024 * 1024:
        raise RuntimeError("native PBS observation exceeds its bounded size")
    parsed = json.loads(output, object_pairs_hook=unique)
    rows = parsed.get(field) if isinstance(parsed, dict) else None
    if (
        not isinstance(rows, dict)
        or len(rows) > 10000
        or any(
            not isinstance(key, str) or not isinstance(value, dict)
            for key, value in rows.items()
        )
    ):
        raise RuntimeError(f"native PBS {field} inventory is incomplete")
    return rows


def _jobs(deadline: float) -> dict[str, dict[str, Any]]:
    return _json_rows(_call(["qstat", "-f", "-F", "json"], deadline), "Jobs")


def _owner() -> str:
    return pwd.getpwuid(os.getuid()).pw_name


def _allocation(
    config: PilotRuntimeConfig, jobs: dict[str, dict[str, Any]], hosts: list[str]
) -> tuple[str, list[str]]:
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
    prefix = config.job_name_prefix
    if job.get("Job_Name") != prefix + config.job_name:
        raise RuntimeError(
            "originating PBS allocation name differs from pilot identity"
        )
    vnodes = execution_vnodes(job.get("exec_vnode"))
    actual = [host for host, _ in vnodes]
    canonical = [physical_host(host) for host in hosts]
    if len(set(actual)) != len(actual) or len(set(canonical)) != config.num_nodes:
        raise RuntimeError("physical allocation node count is incoherent")
    if len(actual) != config.num_nodes or set(actual) != set(canonical):
        raise RuntimeError("node file does not match originating PBS allocation")
    if any(
        positive_int(attrs.get("ngpus")) != config.gpus_per_node for _, attrs in vnodes
    ):
        raise RuntimeError("PBS GPU allocation differs from pilot configuration")
    return source, canonical


def guard_quarantined_nodes(
    config: PilotRuntimeConfig, hosts: list[str] | None = None
) -> None:
    """Reject pending/held/running requests before model initialization."""
    if config.node_admission.on_failure != "pbs_quarantine":
        return
    if hosts is None:
        hosts = deduplicate_hosts(discover_hosts(config.node_file_env))
    jobs = _jobs(time.monotonic() + _ACTION_BUDGET_SEC)
    _, physical = _allocation(config, jobs, hosts)
    matching = [
        request
        for request in discover_quarantines(jobs, _owner())
        if request.host in physical
    ]
    if matching:
        identities = [
            (row.host, row.job_id, row.environment, row.state) for row in matching
        ]
        logger.warning("admission_deferred_known_quarantine: %s", identities)
        raise AdmissionDeferredKnownQuarantine(
            f"admission_deferred_known_quarantine: {identities}"
        )


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
                "environment": os.environ.get("TARA_DEPLOYMENT_ENV")
                or config.node_admission.environment,
                "observations": observations,
                "recorded_unix": time.time(),
            },
            stream,
        )
    return str(path.resolve())


def admit_nodes(config: PilotRuntimeConfig) -> None:
    """Guard, screen, submit bounded fault actions, then end the failed attempt."""
    admission = config.node_admission
    hosts = None
    if admission.checks or admission.on_failure == "pbs_quarantine":
        hosts = deduplicate_hosts(discover_hosts(config.node_file_env))
        guard_quarantined_nodes(config, hosts)
    if not admission.checks:
        return
    assert hosts is not None
    observations: dict[str, object] = {}
    failures = run_node_checks(admission.checks, hosts, config, evidence=observations)
    if not failures:
        logger.info("all %d nodes passed %s", len(hosts), admission.checks)
        return
    if not set(failures).issubset(hosts):
        raise RuntimeError("screening faults refer to nodes outside the allocation")
    evidence = _evidence(config, failures, observations)
    deadline = time.monotonic() + _ACTION_BUDGET_SEC
    act = ACTIONS[admission.on_failure]
    for hostname, reasons in failures.items():
        logger.error(
            "node %s failed checks: %s; evidence=%s", hostname, reasons, evidence
        )
        try:
            if act is pbs_quarantine:
                reason = reasons[0].split(":", 1)[0]
                pbs_quarantine(
                    hostname,
                    config,
                    reason=reason,
                    evidence=evidence,
                    deadline=deadline,
                )
            else:
                act(hostname, config)
        except Exception:
            logger.exception(
                "%s action failed for %s; ordinary pilot exit continues",
                admission.on_failure,
                hostname,
            )
    raise NodeChecksFailed(
        f"nodes failed startup checks: {sorted(failures)}; evidence={evidence}"
    )


def exit_only(hostname: str, config: PilotRuntimeConfig) -> None:
    """Take no additional action; ordinary pilot failure/retry handles the attempt."""


def pbs_quarantine(
    hostname: str,
    config: PilotRuntimeConfig,
    *,
    reason: str = "node_screen_failed",
    evidence: str | None = None,
    deadline: float | None = None,
) -> None:
    """Submit at most once, inspect accepted identity, never wait for execution."""
    if not config.queue or not config.account:
        raise ValueError("pbs_quarantine requires the pilot's queue and account")
    deadline = time.monotonic() + _ACTION_BUDGET_SEC if deadline is None else deadline
    jobs = _jobs(deadline)
    hosts = deduplicate_hosts(discover_hosts(config.node_file_env))
    source, allocated = _allocation(config, jobs, hosts)
    host = physical_host(hostname)
    if host not in allocated:
        raise RuntimeError("fault host was not in the originating allocation")
    existing = discover_quarantines(jobs, _owner())
    matching = [request for request in existing if request.host == host]
    if matching:
        logger.warning(
            "existing quarantine request(s) for %s: %s",
            host,
            [row.job_id for row in matching],
        )
        return
    if (
        len({row.host for row in existing})
        >= config.node_admission.max_quarantined_nodes
    ):
        raise RuntimeError(
            "quarantine safety budget reached; ordinary retries remain active"
        )
    runtime_environment = os.environ.get("TARA_DEPLOYMENT_ENV")
    configured_environment = config.node_admission.environment
    if (
        runtime_environment
        and configured_environment
        and runtime_environment != configured_environment
    ):
        raise RuntimeError(
            "pilot quarantine environment disagrees with deployment context"
        )
    environment = runtime_environment or configured_environment
    if environment not in {"dev", "prod"}:
        raise RuntimeError("pilot quarantine environment must be dev or prod")
    nodes = _json_rows(_call(["pbsnodes", "-a", "-F", "json"], deadline), "nodes")
    physical = [
        row
        for key, row in nodes.items()
        if physical_host(key) == host
        or (row.get("resources_available") or {}).get("host") == host
    ]
    if len(physical) != 1 or host not in nodes:
        raise RuntimeError("whole-physical-host resources are unverified")
    available = physical[0].get("resources_available")
    if not isinstance(available, dict) or available.get("host") != host:
        raise RuntimeError("physical host resource identity is unverified")
    ncpus, ngpus = (
        positive_int(available.get("ncpus")),
        positive_int(available.get("ngpus")),
    )
    if ngpus != config.gpus_per_node:
        raise RuntimeError(
            "whole-host GPUs differ from the configured admission boundary"
        )
    operation = uuid.uuid4().hex
    if evidence is None:
        evidence = _evidence(config, {hostname: [reason]})
    metadata = {
        TAG + "VERSION": "1",
        TAG + "ENV": environment,
        TAG + "CLUSTER": "tara",
        TAG + "HOST": host,
        TAG + "OP": operation,
        TAG + "SOURCE": source,
        TAG + "REASON": reason,
        TAG + "EVIDENCE": evidence,
    }
    name = f"{QUARANTINE_JOB_PREFIX}{environment}_{host}_{operation[:8]}"
    seconds = config.node_admission.quarantine_walltime_min * 60
    if not 0 < seconds <= MAX_WALLTIME_SECONDS:
        raise RuntimeError("quarantine walltime exceeds the qualified site limit")
    resources = {
        "select": f"1:host={host}:ncpus={ncpus}:ngpus={ngpus}",
        "place": "exclhost",
        "walltime": f"{seconds // 3600}:{seconds // 60 % 60:02}:00",
    }
    validate_quarantine(
        "1.pbs",
        {
            "Job_Name": name,
            "Job_Owner": f"{_owner()}@pilot",
            "Variable_List": metadata,
            "Resource_List": resources,
            "job_state": "Q",
        },
        _owner(),
    )
    command = [
        "qsub",
        "-N",
        name,
        "-q",
        config.queue,
        "-A",
        config.account,
        "-l",
        f"select={resources['select']}",
        "-l",
        "place=exclhost",
        "-l",
        f"walltime={resources['walltime']}",
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
    except Exception:
        logger.warning(
            "qsub acknowledgement ambiguous for operation %s; lookup only",
            operation,
            exc_info=True,
        )
    for attempt in range(3):
        try:
            current = discover_quarantines(_jobs(deadline), _owner())
            matches = [row for row in current if row.operation == operation]
            if len(matches) > 1:
                raise RuntimeError(
                    "duplicate quarantine operation requires operator inspection"
                )
            if matches:
                accepted = matches[0]
                if (
                    accepted.host != host
                    or accepted.source != source
                    or accepted.environment != environment
                    or accepted.reason != reason
                    or accepted.evidence != evidence
                    or accepted.ncpus != ncpus
                    or accepted.ngpus != ngpus
                    or accepted.walltime_seconds != seconds
                    or (acknowledged is not None and accepted.job_id != acknowledged)
                ):
                    raise RuntimeError(
                        "PBS accepted identity differs from intended quarantine request"
                    )
                logger.warning(
                    "isolation pending: host=%s quarantine=%s operation=%s source=%s evidence=%s",
                    host,
                    accepted.job_id,
                    operation,
                    source,
                    evidence,
                )
                return
        except (subprocess.SubprocessError, OSError, json.JSONDecodeError):
            logger.warning("quarantine acknowledgement lookup failed", exc_info=True)
        if attempt < 2 and deadline - time.monotonic() > 1:
            time.sleep(1)
    raise RuntimeError(
        "quarantine acceptance unresolved; ordinary pilot exit/retry continues"
    )


Action = Callable[[str, PilotRuntimeConfig], None]
ACTIONS: dict[str, Action] = {"exit": exit_only, "pbs_quarantine": pbs_quarantine}
