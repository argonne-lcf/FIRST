"""
Node admission: screen the allocation's nodes before the pilot serves anything.

The pilot runs the configured node checks and passes the result to the
configured action. If any node failed, the pilot then exits, so the gateway's
ordinary retry submits a replacement job.

Command failures and ambiguous observations raise from the checks: the pilot
fails to start like on any other startup error, and no node is blamed.

To add an action, write a function taking (config, hosts, failures, evidence),
add it to ACTIONS, and add its name to `NodeAdmission.on_failure`.
"""

import json
import logging
import os
import pwd
import re
import subprocess
import time
import uuid
from typing import Callable

from first_common.schema.pilot import PilotRuntimeConfig
from first_common.schema.types import QUARANTINE_JOB_PREFIX, QUARANTINE_VAR_PREFIX

from .host_commands import deduplicate_hosts, host_identity
from .node_checks import run_node_checks
from .replica_manager import discover_hosts

logger = logging.getLogger(__name__)

_PBS_TIMEOUT_SEC = 30.0

# qsub -v separates variables with commas and gives quotes and whitespace
# special meaning; anything else outside this set is replaced in a value.
_QSUB_UNSAFE = re.compile(r"[^A-Za-z0-9_.:/@+-]")


class NodeChecksFailed(RuntimeError):
    """At least one node failed a startup check; the pilot must not start."""


def admit_nodes(config: PilotRuntimeConfig) -> None:
    """Run the configured checks and action; raise NodeChecksFailed on a fault."""
    admission = config.node_admission
    if not admission.checks:
        return

    hosts = deduplicate_hosts(discover_hosts(config.node_file_env))
    observations: dict[str, object] = {}
    failures = run_node_checks(admission.checks, hosts, config, evidence=observations)
    evidence = _write_evidence(config, failures, observations) if failures else None
    for host, reasons in failures.items():
        logger.error("node %s failed startup checks: %s", host, "; ".join(reasons))

    try:
        ACTIONS[admission.on_failure](config, hosts, failures, evidence)
    except Exception:
        # The screen result stands: a healthy pilot still starts and a failed
        # one still exits.
        logger.exception("node admission action %s failed", admission.on_failure)

    if failures:
        raise NodeChecksFailed(
            f"nodes failed startup checks: {sorted(failures)}; evidence={evidence}"
        )
    logger.info("all %d nodes passed %s", len(hosts), admission.checks)


def _write_evidence(
    config: PilotRuntimeConfig,
    failures: dict[str, list[str]],
    observations: dict[str, object],
) -> str:
    """Record a failed screen's raw observations for later diagnosis."""
    config.workdir.mkdir(parents=True, exist_ok=True)
    path = config.workdir / f"node-admission-{uuid.uuid4().hex}.json"
    path.write_text(
        json.dumps(
            {
                "source": os.environ.get("PBS_JOBID"),
                "recorded_unix": time.time(),
                "failures": failures,
                "observations": observations,
            }
        )
    )
    return str(path.resolve())


def exit_only(
    config: PilotRuntimeConfig,
    hosts: list[str],
    failures: dict[str, list[str]],
    evidence: str | None,
) -> None:
    """Take no action; a failed pilot simply exits."""


def pbs_quarantine(
    config: PilotRuntimeConfig,
    hosts: list[str],
    failures: dict[str, list[str]],
    evidence: str | None,
) -> None:
    """
    Park an exclusive sleep job on each failing node, so PBS cannot place
    another job there once it runs. Delete queued quarantine jobs on nodes that
    just passed.

    The quarantine jobs in PBS are the only record of quarantined nodes. Each
    environment sees only its own: Dev may run experimental checks, which must
    not clear or crowd out Prod's quarantines. A node returns to service when
    its job is deleted (`qdel`) or reaches its walltime; the next pilot to land
    there checks it again.
    """
    environment = config.node_admission.environment
    assert environment is not None  # NodeAdmission validates this

    failed = {host_identity(host): reasons for host, reasons in failures.items()}
    passed = {host_identity(host) for host in hosts} - failed.keys()
    quarantined = _quarantine_jobs(environment)  # job ID -> host

    # This pilot holds its hosts exclusively, so a quarantine job for one of
    # them has not started yet. The host just passed: withdraw the request.
    for job_id, host in list(quarantined.items()):
        if host in passed:
            _pbs(["qdel", job_id])
            del quarantined[job_id]
            logger.warning("node %s passed; deleted queued quarantine %s", host, job_id)

    limit = config.node_admission.max_quarantined_nodes
    for host, reasons in sorted(failed.items()):
        if host in quarantined.values():
            logger.info("node %s is already quarantined", host)
            continue
        if len(set(quarantined.values())) >= limit:
            logger.error("not quarantining %s: %d nodes already are", host, limit)
            continue
        job_id = _submit_quarantine(config, environment, host, reasons, evidence)
        quarantined[job_id] = host
        logger.warning("quarantined node %s with PBS job %s", host, job_id)


def _submit_quarantine(
    config: PilotRuntimeConfig,
    environment: str,
    host: str,
    reasons: list[str],
    evidence: str | None,
) -> str:
    if _QSUB_UNSAFE.search(host) or _QSUB_UNSAFE.search(environment):
        raise ValueError(f"cannot quarantine host {host!r} in {environment!r}")

    variables = {
        "ENV": environment,
        "HOST": host,
        "SOURCE": os.environ.get("PBS_JOBID", ""),
        # The failed check names, e.g. "gpu_count+gpu_ecc_errors"
        "REASON": "+".join(reason.split(":", 1)[0] for reason in reasons),
        "EVIDENCE": evidence or "",
    }
    walltime_min = config.node_admission.quarantine_walltime_min
    qsub = [
        "qsub",
        *("-N", f"{QUARANTINE_JOB_PREFIX}{environment}_{host}"),
        *("-q", config.queue),
        *("-A", config.account),
        *("-l", f"select=1:host={host}"),
        *("-l", "place=exclhost"),
        *("-l", f"walltime={walltime_min // 60}:{walltime_min % 60:02}:00"),
        *(
            "-v",
            ",".join(
                f"{QUARANTINE_VAR_PREFIX}{name}={_QSUB_UNSAFE.sub('_', value)}"
                for name, value in variables.items()
            ),
        ),
        *("-j", "oe"),
        *("-o", "/dev/null"),
        *("--", "/bin/sleep", "infinity"),
    ]
    return _pbs(qsub).strip()


def _quarantine_jobs(environment: str) -> dict[str, str]:
    """Job ID -> host of this environment's queued and running quarantines."""
    jobs = json.loads(_pbs(["qstat", "-f", "-F", "json"])).get("Jobs", {})
    owner = pwd.getpwuid(os.getuid()).pw_name
    result = {}
    for job_id, job in jobs.items():
        variables = job.get("Variable_List", {})
        if (
            job.get("Job_Owner", "").split("@", 1)[0] == owner
            and variables.get(QUARANTINE_VAR_PREFIX + "ENV") == environment
            and QUARANTINE_VAR_PREFIX + "HOST" in variables
        ):
            result[job_id] = variables[QUARANTINE_VAR_PREFIX + "HOST"]
    return result


def _pbs(command: list[str]) -> str:
    return subprocess.run(
        command, capture_output=True, text=True, check=True, timeout=_PBS_TIMEOUT_SEC
    ).stdout


Action = Callable[
    [PilotRuntimeConfig, list[str], dict[str, list[str]], str | None], None
]

ACTIONS: dict[str, Action] = {
    "exit": exit_only,
    "pbs_quarantine": pbs_quarantine,
}
