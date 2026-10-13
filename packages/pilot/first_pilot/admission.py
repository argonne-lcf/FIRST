"""
Node admission: screen the allocation's nodes before the pilot serves anything.

The pilot runs the configured node checks. If every node passes, startup
continues. Otherwise it applies the configured action to each bad node and
exits, so the gateway's ordinary retry submits a replacement job.

To add an action, write a function taking (hostname, config), add it
to ACTIONS, and add its name to `NodeAdmission.on_failure`.
"""

import json
import logging
import subprocess
from typing import Callable

from first_common.schema.pilot import PilotRuntimeConfig
from first_common.schema.types import QUARANTINE_JOB_PREFIX

from .node_checks import run_node_checks
from .replica_manager import _deduplicate_hosts, _host_identity, discover_hosts

logger = logging.getLogger(__name__)

_PBS_TIMEOUT_SEC = 30.0


class NodeChecksFailed(RuntimeError):
    """At least one node failed a startup check; the pilot must not start."""


def admit_nodes(config: PilotRuntimeConfig) -> None:
    """Run the configured checks; act on bad nodes and raise NodeChecksFailed."""
    admission = config.node_admission
    if not admission.checks:
        return

    hostnames = _deduplicate_hosts(discover_hosts(config.node_file_env))
    failures = run_node_checks(admission.checks, hostnames, config)
    if not failures:
        logger.info("all %d nodes passed %s", len(hostnames), admission.checks)
        return

    act = ACTIONS[admission.on_failure]
    for hostname, reasons in failures.items():
        logger.error("node %s failed startup checks: %s", hostname, "; ".join(reasons))
        try:
            act(hostname, config)
        except Exception:
            logger.exception("%s action failed for %s", admission.on_failure, hostname)
    raise NodeChecksFailed(f"nodes failed startup checks: {sorted(failures)}")


def exit_only(hostname: str, config: PilotRuntimeConfig) -> None:
    """Take no action; the pilot simply exits."""


def pbs_quarantine(hostname: str, config: PilotRuntimeConfig) -> None:
    """
    Park a sleep job on the node so PBS cannot place another job there.

    The quarantine jobs in PBS are the only record of which nodes are
    quarantined. A node returns to service when its job is deleted (`qdel`) or
    reaches its walltime; the next pilot to land there checks it again.
    """
    if not config.queue or not config.account:
        raise ValueError("pbs_quarantine requires the pilot's queue and account")

    host = _host_identity(hostname)
    job_name = QUARANTINE_JOB_PREFIX + host
    quarantined = _quarantine_job_names()
    if job_name in quarantined:
        logger.info("node %s is already quarantined", host)
        return
    limit = config.node_admission.max_quarantined_nodes
    if len(quarantined) >= limit:
        logger.error(
            "not quarantining %s: %d nodes are already quarantined (limit %d)",
            host,
            len(quarantined),
            limit,
        )
        return

    walltime_min = config.node_admission.quarantine_walltime_min
    qsub = [
        "qsub",
        *("-N", job_name),
        *("-q", config.queue),
        *("-A", config.account),
        *("-l", f"select=1:host={host}"),
        *("-l", "place=exclhost"),
        *("-l", f"walltime={walltime_min // 60}:{walltime_min % 60:02}:00"),
        *("-j", "oe"),
        *("-o", "/dev/null"),
        *("--", "/bin/sleep", "infinity"),
    ]
    job_id = _pbs(qsub).strip()
    logger.warning("quarantined node %s with PBS job %s", host, job_id)


def _quarantine_job_names() -> set[str]:
    """Names of the quarantine jobs currently queued or running in PBS."""
    jobs = json.loads(_pbs(["qstat", "-f", "-F", "json"])).get("Jobs", {})
    names = {job.get("Job_Name", "") for job in jobs.values()}
    return {name for name in names if name.startswith(QUARANTINE_JOB_PREFIX)}


def _pbs(command: list[str]) -> str:
    return subprocess.run(
        command, capture_output=True, text=True, check=True, timeout=_PBS_TIMEOUT_SEC
    ).stdout


Action = Callable[[str, PilotRuntimeConfig], None]

ACTIONS: dict[str, Action] = {
    "exit": exit_only,
    "pbs_quarantine": pbs_quarantine,
}
