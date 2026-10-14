import base64
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Self

from httpx import AsyncClient

from first_common.pbs_quarantine import (
    QuarantineRequest,
    execution_vnodes,
    quarantine_request,
)
from first_common.schema.base_scheduler import (
    JobStatusInfo,
    JobSubmitPayload,
    JobSubmitResult,
    SchedulerAdapter,
    SchedulerJobState,
)
from first_gateway.settings import ClientState

from .graphql_pbs_resources import TaskResourceConfig, requested_resources

logger = logging.getLogger(__name__)

# JobStatus.state integer codes -> normalized state (pbs_graphql_schema_doc.md)
_STATE_MAP: dict[int, SchedulerJobState] = {
    0: SchedulerJobState.queued,  # Queued
    1: SchedulerJobState.queued,  # Waiting (future execution time)
    2: SchedulerJobState.queued,  # DependHeld
    3: SchedulerJobState.queued,  # Held
    4: SchedulerJobState.gone,  # StagingFail
    5: SchedulerJobState.starting,  # StagingIn
    6: SchedulerJobState.exiting,  # StagingOut
    7: SchedulerJobState.running,  # Running
    8: SchedulerJobState.exiting,  # Suspended; allocation release is unproven
    9: SchedulerJobState.exiting,  # Exiting
    10: SchedulerJobState.gone,  # Done
    11: SchedulerJobState.gone,  # Failed
    12: SchedulerJobState.gone,  # Deleted
    13: SchedulerJobState.gone,  # Moved
    14: SchedulerJobState.queued,  # Unlicensed
}

# States in which the job is actually placed on machines, so allocatedMachines
# (and thus hsn_ips) is meaningful
_ACTIVE_STATES = frozenset({5, 6, 7, 8, 9})

_HSN_RESOURCE_NAME = "hsn_ips"
_PBS_JOB_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_STATUS_PAGE_SIZE = 500
_STATUS_MAX_PAGES = 10
_EXEC_VNODE_PART = re.compile(
    r"(?P<name>[A-Za-z0-9][A-Za-z0-9._\[\]-]{0,254})"
    r"(?::[A-Za-z_][A-Za-z0-9_.-]*=[^():+\s=]+)+"
)


@dataclass(frozen=True)
class QuarantineStatus:
    request: QuarantineRequest
    isolated: bool
    expires_at: datetime | None


def _quarantine_hint(row: dict[str, Any]) -> bool:
    """Names aid discovery; metadata also protects renamed/truncated jobs."""
    name = row.get("name")
    if not isinstance(name, str):
        raise RuntimeError("PBS job name is unavailable")
    env = row.get("env")
    if env is not None and not isinstance(env, list):
        raise RuntimeError("PBS job metadata is malformed")
    return name.startswith(("first_quar_", "first_tara_quarantine_")) or any(
        isinstance(pair, dict)
        and isinstance(pair.get("name"), str)
        and pair["name"].startswith(("FIRST_QUAR_", "FIRST_QUARANTINE_"))
        for pair in env or []
    )


def _quarantine_status(row: dict[str, Any], expected_owner: str) -> QuarantineStatus:
    """Read PBS's accepted request; do not recreate an independent scheduler."""
    owner = row.get("owner")
    if (
        not isinstance(owner, str)
        or re.fullmatch(rf"{re.escape(expected_owner)}(?:@[A-Za-z0-9.-]+)?", owner)
        is None
    ):
        raise RuntimeError("PBS quarantine ownership is unverified")
    metadata: dict[str, Any] = {}
    for pair in row.get("env") or []:
        key, value = pair.get("name"), pair.get("value")
        if isinstance(key, str) and key.startswith("FIRST_QUAR_"):
            if key in metadata:
                raise RuntimeError("duplicate quarantine attribution")
            metadata[key] = value
    resources = row.get("resourcesRequested") or {}
    tasks = resources.get("tasksResources") or []
    count = resources.get("taskCount") or {}
    if (
        len(tasks) != 1
        or tasks[0].get("index") != "0"
        or type(count.get("min")) is not int
        or count.get("min") not in (0, 1)
        or type(count.get("max")) is not int
        or count.get("max") != 1
        or type(resources.get("jobPlacementSharing")) is not int
        or resources.get("jobPlacementSharing") not in (1, 2)
        or tasks[0].get("candidateMachineName") != metadata.get("FIRST_QUAR_HOST")
    ):
        raise RuntimeError("PBS exact-host exclusive request is unverified")
    state = (row.get("status") or {}).get("state")
    if type(state) is not int or state not in _STATE_MAP:
        raise RuntimeError("PBS quarantine state is unverified")
    native_state = {
        0: "Q",
        1: "W",
        2: "H",
        3: "H",
        4: "F",
        5: "B",
        6: "E",
        7: "R",
        8: "S",
        9: "E",
        10: "F",
        11: "F",
        12: "X",
        13: "X",
        14: "Q",
    }[state]
    walltime = (resources.get("jobResources") or {}).get("wallClockTime")
    if type(walltime) is not int:
        raise RuntimeError("PBS quarantine walltime is unavailable")
    request = quarantine_request(
        row["jobId"],
        metadata,
        native_state,
        tasks[0].get("slots"),
        tasks[0].get("gpus"),
        walltime,
    )
    started = _parse_epoch_micros(row.get("startTime"))
    expires = (
        datetime.fromtimestamp(
            started.timestamp() + request.walltime_seconds, timezone.utc
        )
        if started
        else None
    )
    isolated = False
    if request.state == "R" and expires and expires > datetime.now(timezone.utc):
        machines = row.get("allocatedMachines") or []
        extension = row.get("extension") or {}
        try:
            vnodes = execution_vnodes(
                extension.get("exec_vnode") if isinstance(extension, dict) else None
            )
        except RuntimeError:
            vnodes = []
        isolated = (
            len(machines) == 1
            and machines[0].get("name") == request.host
            and len(vnodes) == 1
            and vnodes[0][0] == request.host
        )
    return QuarantineStatus(request, isolated, expires)


def _parse_epoch_micros(raw: int | None) -> datetime | None:
    """Convert an EpochTime (microseconds since epoch) to a UTC datetime."""
    if not raw:
        return None
    return datetime.fromtimestamp(raw / 1_000_000, tz=timezone.utc)


def _execution_head(
    machines: list[dict[str, Any]], extension: Any
) -> dict[str, Any] | None:
    """Resolve the first PBS exec_vnode; allocatedMachines is unordered.

    Older bridges without exec_vnode are safe only for a single-machine job.
    Present but malformed metadata never falls back, even for one machine.
    """
    if extension is None or (
        isinstance(extension, dict) and "exec_vnode" not in extension
    ):
        return machines[0] if len(machines) == 1 else None
    if not isinstance(extension, dict):
        return None
    raw = extension.get("exec_vnode")
    if (
        not isinstance(raw, str)
        or not 0 < len(raw) <= 65536
        or not raw.startswith("(")
        or not raw.endswith(")")
    ):
        return None
    names = []
    for chunk in raw[1:-1].split(")+("):
        for part in chunk.split("+"):
            match = _EXEC_VNODE_PART.fullmatch(part)
            if match is None:
                return None
            names.append(match.group("name"))
    matches = [
        machine
        for machine in machines
        if names[0] in (machine.get("name"), machine.get("hostname"))
    ]
    return matches[0] if len(matches) == 1 else None


def _head_node_ip(head: dict[str, Any]) -> str | None:
    """Pull the proven execution head's first hsn_ips address."""
    resources_avail = head.get("resourcesAvail") or {}
    for pair in resources_avail.get("customResources") or []:
        if pair.get("name") == _HSN_RESOURCE_NAME:
            ips = pair["value"].replace(",", " ").split()
            return ips[0] if ips else None
    return None


def _job_status_from_node(node: dict[str, Any]) -> JobStatusInfo:
    job_id = (node.get("jobId") or "").strip()
    if not job_id:
        raise RuntimeError("GraphQL jobs query returned an empty jobId")

    state_code: int | None = (node.get("status") or {}).get("state")
    state = _STATE_MAP.get(state_code) if state_code is not None else None
    if state is None:
        raise RuntimeError(
            f"unknown GraphQL job state {state_code!r} for job {job_id!r}"
        )

    resources = (node.get("resourcesRequested") or {}).get("jobResources") or {}
    walltime_sec = resources.get("wallClockTime") or 0
    head_ip = None
    head_hostname = None
    if state_code in _ACTIVE_STATES:
        machines = node.get("allocatedMachines") or []
        head = _execution_head(machines, node.get("extension"))
        if head is not None:
            head_ip = _head_node_ip(head)
            head_hostname = head.get("hostname") or None
        elif machines:
            logger.warning("Cannot resolve execution head for PBS job %s", job_id)

    return JobStatusInfo(
        id=job_id,
        name=node["name"],
        state=state,
        created_at=_parse_epoch_micros(node.get("submitTime"))
        or datetime.now(timezone.utc),
        started_at=_parse_epoch_micros(node.get("startTime")),
        walltime_minutes=walltime_sec // 60,
        head_node_ip_address=head_ip,
        head_node_hostname=head_hostname,
    )


class GraphQLPBSAdapter(SchedulerAdapter):
    def __init__(
        self,
        client: AsyncClient,
        owner: str,
        url: str,
        task_resources: dict[str, str] | None = None,
    ) -> None:
        self.client = client
        self.owner = owner
        self.url = url
        self.task_resources = TaskResourceConfig(
            task_resources={} if task_resources is None else task_resources
        ).task_resources

    @classmethod
    async def build(cls, deps: ClientState, config: dict[str, Any]) -> Self:
        """
        Constructs the adapter around a pre-authenticated Keycloak client.

        Required config keys:
            keycloak_client_name: str — key into ClientState.keycloak_clients.
            job_owner: str — PBS username whose jobs this adapter manages.
        """
        name = config["keycloak_client_name"]
        owner = config["job_owner"]
        graphql_url = config["graphql_url"]
        options = TaskResourceConfig.model_validate(
            {"task_resources": config.get("task_resources", {})}
        )
        return cls(
            client=deps.keycloak_clients[name],
            owner=owner,
            url=graphql_url,
            task_resources=options.task_resources,
        )

    async def _post(
        self, query: str, variables: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        request: dict[str, Any] = {"query": query}
        if variables is not None:
            request["variables"] = variables
        resp = await self.client.post(self.url, json=request)
        resp.raise_for_status()
        body: dict[str, Any] = resp.json()
        if body.get("errors"):
            raise RuntimeError(f"GraphQL query failed:\n{body['errors']}")
        data: dict[str, Any] = body["data"]
        return data

    async def submit_job(self, job: JobSubmitPayload) -> JobSubmitResult:
        if job.script is None:
            raise ValueError("GraphQLPBSAdapter.submit_job requires an inline script")

        # scriptContent expects urlsafe base64 (schema Base64 type); this preserves
        # newlines/quotes/heredocs verbatim.
        script_b64 = base64.urlsafe_b64encode(job.script.encode()).decode()

        query = """
        mutation SubmitJob($input: JobInput!) {
            createJob(input: $input) {
                node { jobId }
                error { errorCode errorMessage }
            }
        }
        """
        data = await self._post(
            query,
            {
                "input": {
                    "scriptContent": script_b64,
                    "name": job.name,
                    "resourcesRequested": requested_resources(job, self.task_resources),
                    "queue": {"name": job.queue},
                    "accountingId": job.account,
                    "errorPath": str(job.log_path),
                    "outputPath": str(job.log_path),
                    "joinFiles": True,
                }
            },
        )
        payload = data["createJob"]
        if payload.get("error"):
            raise RuntimeError(f"GraphQL createJob failed:\n{payload['error']}")
        scheduler_id = (payload["node"]["jobId"] or "").strip()
        if not scheduler_id:
            raise RuntimeError("GraphQL createJob returned an empty jobId")
        return JobSubmitResult(job_name=job.name, scheduler_id=scheduler_id)

    async def _job_rows(self, job_id: str | None = None) -> list[dict[str, Any]]:
        """The same bounded inventory serves ordinary status and quarantine reads."""
        if job_id is not None and _PBS_JOB_ID.fullmatch(job_id) is None:
            raise ValueError(f"invalid PBS scheduler job ID: {job_id!r}")
        declaration = "ActiveJobs($owner: String!, $cursor: Cursor)"
        job_filter = "owner: $owner, withHistoryJobs: false"
        variables: dict[str, Any] = {"owner": self.owner}
        if job_id is not None:
            declaration = "ExactJobState($jobId: String!, $cursor: Cursor)"
            job_filter = "jobIds: [$jobId], withHistoryJobs: true"
            variables = {"jobId": job_id}
        query = f"""
        query {declaration} {{
          jobs(filter: {{{job_filter}}}, count: {_STATUS_PAGE_SIZE if job_id is None else 1}, from: $cursor) {{
            edges {{ node {{
              jobId name owner env {{ name value }} submitTime startTime extension
              status {{ state }}
              resourcesRequested {{
                jobPlacementSharing taskCount {{ min max }}
                tasksResources {{ index candidateMachineName slots gpus }}
                jobResources {{ wallClockTime }}
              }}
              allocatedMachines {{
                name hostname resourcesAvail {{ customResources {{ name value }} }}
              }}
            }} error {{ errorCode errorMessage }} }}
            pageInfo {{ hasNextPage endCursor }}
          }}
        }}
        """
        rows: dict[str, dict[str, Any]] = {}
        cursors: set[str] = set()
        cursor = None
        for _ in range(_STATUS_MAX_PAGES):
            data = await self._post(query, {**variables, "cursor": cursor})
            for edge in data["jobs"]["edges"]:
                row = edge.get("node")
                if edge.get("error") or not isinstance(row, dict):
                    raise RuntimeError("GraphQL jobs edge failed or was incomplete")
                identity = row.get("jobId")
                if not isinstance(identity, str) or not identity:
                    raise RuntimeError("GraphQL jobs query returned an empty jobId")
                if job_id is not None and identity != job_id:
                    raise RuntimeError(
                        "GraphQL exact-job query returned a different job ID"
                    )
                rows.setdefault(identity, row)
            page = data["jobs"]["pageInfo"]
            if not page["hasNextPage"]:
                return list(rows.values())
            cursor = page.get("endCursor")
            if not isinstance(cursor, str) or not cursor or cursor in cursors:
                raise RuntimeError("GraphQL jobs pagination cursor did not advance")
            cursors.add(cursor)
        raise RuntimeError("GraphQL active-job listing exceeded bounded pagination")

    async def get_job_statuses(self) -> list[JobStatusInfo]:
        return [_job_status_from_node(row) for row in await self._job_rows()]

    async def terminate_job(self, job_id: str) -> None:
        job_id = job_id.strip()
        if _PBS_JOB_ID.fullmatch(job_id) is None:
            raise ValueError(f"invalid PBS scheduler job ID: {job_id!r}")

        # Ordinary lifecycle/orphan cleanup must never cancel either
        # environment's isolation jobs, including malformed renamed jobs.
        rows = await self._job_rows(job_id)
        if not rows:
            logger.info(
                "ordinary cleanup job %s is absent from the exact PBS lookup", job_id
            )
            return
        if _quarantine_hint(rows[0]):
            raise RuntimeError("ordinary model cleanup cannot delete a quarantine job")
        owner = rows[0].get("owner")
        if (
            not isinstance(owner, str)
            or re.fullmatch(rf"{re.escape(self.owner)}(?:@[A-Za-z0-9.-]+)?", owner)
            is None
        ):
            raise RuntimeError("ordinary model cleanup PBS ownership is unverified")

        # Ordinary lifecycle disposal must allow the scheduler's normal
        # termination/cleanup path. Never silently escalate a failed request
        # to forced deletion, which can discard scheduler-side job state.
        query = """
        mutation DeleteJob($jobId: String!) {
            deleteJob (jobId: $jobId, input: {force: false}) {
                node {
                    jobId
                }
                error {
                    errorCode
                    errorMessage
                }
            }
        }
        """
        data = await self._post(query, {"jobId": job_id})
        payload = data["deleteJob"]
        if payload.get("error"):
            state = await self._get_exact_job_state(job_id)
            if state is None or state == SchedulerJobState.gone:
                logger.warning(f"terminate {job_id=} error: job is already gone.")
                return
            raise RuntimeError(f"GraphQL deleteJob failed:\n{payload['error']}")
        returned_id = ((payload.get("node") or {}).get("jobId") or "").strip()
        if returned_id and returned_id != job_id:
            raise RuntimeError(
                "GraphQL deleteJob returned a different job ID: "
                f"expected {job_id!r}, got {returned_id!r}"
            )

        # The mutation acknowledges qdel; it does not prove that PBS has
        # released the allocation. The controller records the job as exiting,
        # and the scheduler observer completes deletion after it observes gone.

    async def get_exact_job_status(self, job_id: str) -> JobStatusInfo | None:
        rows = await self._job_rows(job_id.strip())
        return _job_status_from_node(rows[0]) if rows else None

    async def _get_exact_job_state(self, job_id: str) -> SchedulerJobState | None:
        status = await self.get_exact_job_status(job_id)
        return None if status is None else status.state

    async def get_quarantine_statuses(self) -> list[QuarantineStatus]:
        """Observe PBS-accepted requests without an all-owner isolation census."""
        result = []
        for row in await self._job_rows():
            if not _quarantine_hint(row):
                continue
            owner = row.get("owner")
            if isinstance(owner, str) and owner.split("@", 1)[0] != self.owner:
                continue
            result.append(_quarantine_status(row, self.owner))
        return result

    async def put_file(self, content: str, path: Path, mode: int) -> None:
        raise NotImplementedError

    async def list_files(self, directory: Path) -> list[str]:
        raise NotImplementedError

    async def read_file(self, path: Path) -> str:
        raise NotImplementedError
