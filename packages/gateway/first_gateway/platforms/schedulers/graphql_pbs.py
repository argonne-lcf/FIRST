import base64
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Self

from httpx import AsyncClient

from first_common.schema.base_scheduler import (
    JobStatusInfo,
    JobSubmitPayload,
    JobSubmitResult,
    SchedulerAdapter,
    SchedulerJobState,
)
from first_common.schema.quarantine import AUXILIARY_PREFIXES
from first_gateway.settings import ClientState

from .graphql_pbs_quarantine import GraphQLQuarantineMixin
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
_PBS_USERNAME = re.compile(r"[A-Za-z_][A-Za-z0-9_.-]{0,63}")
_DNS_LABEL = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?")
_PBS_HOLDS = re.compile(r"n|[uos]{1,3}")
_NATIVE_STATE_CODES = {
    "Q": {0, 14},
    "W": {1},
    "H": {2, 3},
    "B": {5},
    "R": {7},
    "S": {8},
    "E": {6, 9},
    "F": {4, 10, 11, 12},
    "M": {13},
}
_EXEC_VNODE_PART = re.compile(
    r"(?P<name>[A-Za-z0-9][A-Za-z0-9._\[\]-]{0,254})"
    r"(?::[A-Za-z_][A-Za-z0-9_.-]*=[^():+\s=]+)+"
)


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


def _pbs_owner_username(raw: Any, job_id: str) -> str | None:
    """Normalize PBS's user@submission-host form, without discarding ambiguity."""
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise RuntimeError(f"invalid PBS owner identity for job {job_id!r}")
    parts = raw.split("@")
    if _PBS_USERNAME.fullmatch(parts[0]) is None or len(parts) > 2:
        raise RuntimeError(f"invalid PBS owner identity for job {job_id!r}")
    if len(parts) == 2:
        host = parts[1]
        if not 0 < len(host) <= 253 or any(
            _DNS_LABEL.fullmatch(label) is None for label in host.split(".")
        ):
            raise RuntimeError(f"invalid PBS owner submit host for job {job_id!r}")
    return parts[0]


def _pbs_hold_type(node: dict[str, Any], job_id: str) -> str | None:
    """Read native hold evidence; a Held state alone never implies a user hold.

    Tara's bridge returns null for Job.holdType but supplies the native PBS
    Hold_Types and job_state through Job.extension. Contradictory representations
    fail closed. Nothing from the raw extension (which can include submission
    environment) is logged or retained.
    """
    typed = node.get("holdType")
    extension = node.get("extension")
    native = (
        extension.get("Hold_Types")
        if isinstance(extension, dict) and "Hold_Types" in extension
        else None
    )
    native_state = extension.get("job_state") if isinstance(extension, dict) else None
    for hold in (typed, native):
        if hold is not None and (
            not isinstance(hold, str)
            or _PBS_HOLDS.fullmatch(hold) is None
            or len(set(hold)) != len(hold)
        ):
            raise RuntimeError(f"invalid PBS hold identity for job {job_id!r}")
    if typed is not None and native is not None and typed != native:
        raise RuntimeError(f"PBS hold representations disagree for job {job_id!r}")
    if native is not None or native_state is not None:
        state_code = (node.get("status") or {}).get("state")
        if (
            not isinstance(native_state, str)
            or native_state not in _NATIVE_STATE_CODES
            or state_code not in _NATIVE_STATE_CODES[native_state]
        ):
            raise RuntimeError(
                f"native PBS hold/state proof is unverified for job {job_id!r}"
            )
    selected = typed if typed is not None else native
    state_code = (node.get("status") or {}).get("state")
    if selected == "u" and state_code in {0, 1, 14}:
        raise RuntimeError(
            f"PBS user hold is inconsistent with queued state for job {job_id!r}"
        )
    return selected


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

    submit_time = node.get("submitTime")
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
        owner=_pbs_owner_username(node.get("owner"), job_id),
        hold_type=_pbs_hold_type(node, job_id),
        queue=(node.get("queue") or {}).get("name"),
        account=node.get("accountingId"),
        output_path=node.get("outputPath"),
        submit_time_epoch_s=(
            submit_time // 1_000_000
            if type(submit_time) is int and submit_time > 0
            else None
        ),
        is_auxiliary=_auxiliary_node(node),
    )


def _auxiliary_node(node: dict[str, Any]) -> bool:
    if str(node.get("name", "")).startswith(AUXILIARY_PREFIXES):
        return True
    return any(
        isinstance(pair, dict)
        and pair.get("name") == "FIRST_QUARANTINE_KIND"
        and isinstance(pair.get("value"), str)
        and pair.get("value")
        in {
            "quarantine",
            "fault",
            "activation",
            "screen",
            "screen_request",
            "status",
            "registry",
            "cancel",
        }
        for pair in node.get("env") or []
    )


class GraphQLPBSAdapter(SchedulerAdapter, GraphQLQuarantineMixin):
    def __init__(
        self,
        client: AsyncClient,
        owner: str,
        url: str,
        task_resources: dict[str, str] | None = None,
        quarantine_environment: Literal["dev", "prod"] | None = None,
    ) -> None:
        self.client = client
        self.owner = owner
        self.url = url
        if quarantine_environment not in (None, "dev", "prod"):
            raise ValueError("quarantine_environment must be dev or prod")
        self.quarantine_environment = quarantine_environment
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
            quarantine_environment=config.get("_quarantine_environment"),
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
        job_input: dict[str, Any] = {
            "scriptContent": script_b64,
            "name": job.name,
            "resourcesRequested": requested_resources(job, self.task_resources),
            "queue": {"name": job.queue},
            "accountingId": job.account,
            "errorPath": str(job.log_path),
            "outputPath": str(job.log_path),
            "joinFiles": True,
        }
        data = await self._post(query, {"input": job_input})
        payload = data["createJob"]
        if payload.get("error"):
            raise RuntimeError(f"GraphQL createJob failed:\n{payload['error']}")
        scheduler_id = (payload["node"]["jobId"] or "").strip()
        if not scheduler_id:
            raise RuntimeError("GraphQL createJob returned an empty jobId")
        return JobSubmitResult(job_name=job.name, scheduler_id=scheduler_id)

    async def get_job_statuses(self) -> list[JobStatusInfo]:
        query = f"""
        query ActiveJobs($owner: String!, $cursor: Cursor) {{
            jobs (
                filter: {{owner: $owner, withHistoryJobs: false}}
                count: {_STATUS_PAGE_SIZE}
                from: $cursor
            ) {{
                edges {{
                    node {{
                        jobId
                        name
                        owner
                        holdType
                        env {{ name value }}
                        queue {{ name }}
                        accountingId
                        outputPath
                        submitTime
                        startTime
                        extension
                        status {{
                            state
                        }}
                        resourcesRequested {{
                            jobResources {{
                                wallClockTime
                            }}
                        }}
                        allocatedMachines {{
                            name
                            hostname
                            resourcesAvail {{
                                customResources {{ name value }}
                            }}
                        }}
                    }}
                    error {{
                        errorCode
                        errorMessage
                    }}
                    cursor
                }}
                pageInfo {{
                    hasNextPage
                    endCursor
                }}
            }}
        }}
        """
        results: list[JobStatusInfo] = []
        seen_ids: set[str] = set()
        seen_cursors: set[str] = set()
        cursor: str | None = None
        for _ in range(_STATUS_MAX_PAGES):
            data = await self._post(
                query,
                {"owner": self.owner, "cursor": cursor},
            )
            edges: list[dict[str, Any]] = data["jobs"]["edges"]
            for edge in edges:
                if edge.get("error"):
                    raise RuntimeError(f"GraphQL jobs edge failed:\n{edge['error']}")
                node: dict[str, Any] = edge["node"]
                if _auxiliary_node(node):
                    continue  # Infrastructure metadata must not gate model polling.
                status = _job_status_from_node(node)
                if status.id not in seen_ids:
                    seen_ids.add(status.id)
                    results.append(status)

            page_info = data["jobs"]["pageInfo"]
            if not page_info["hasNextPage"]:
                return results
            next_cursor = page_info.get("endCursor")
            if (
                not isinstance(next_cursor, str)
                or not next_cursor
                or next_cursor in seen_cursors
            ):
                raise RuntimeError("GraphQL jobs pagination cursor did not advance")
            seen_cursors.add(next_cursor)
            cursor = next_cursor

        raise RuntimeError(
            "GraphQL active-job listing exceeded bounded pagination "
            f"({_STATUS_MAX_PAGES} pages)"
        )

    async def terminate_job(self, job_id: str) -> None:
        job_id = job_id.strip()
        if _PBS_JOB_ID.fullmatch(job_id) is None:
            raise ValueError(f"invalid PBS scheduler job ID: {job_id!r}")

        # A quarantine/control job is never a model orphan. Query its identity
        # even in legacy mode before allowing ordinary lifecycle deletion.
        if self.owner == "openinference_svc":
            protected = await self.get_exact_job_status(job_id)
            if protected is not None and (
                protected.is_auxiliary or protected.name.startswith(AUXILIARY_PREFIXES)
            ):
                raise RuntimeError(
                    "ordinary model cleanup cannot delete quarantine jobs"
                )
        await self._legacy_delete_job(job_id)

    async def _legacy_delete_job(self, job_id: str) -> None:
        """Normal PBS cleanup, invoked only after its caller's identity checks."""

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
        job_id = job_id.strip()
        if _PBS_JOB_ID.fullmatch(job_id) is None:
            raise ValueError(f"invalid PBS scheduler job ID: {job_id!r}")
        query = """
        query ExactJobState($jobId: String!) {
            jobs(
                filter: {jobIds: [$jobId], withHistoryJobs: true}
                count: 1
            ) {
                edges {
                    node {
                        jobId
                        name
                        owner
                        holdType
                        env { name value }
                        queue { name }
                        accountingId
                        outputPath
                        submitTime
                        startTime
                        extension
                        status { state }
                        resourcesRequested {
                            jobResources { wallClockTime }
                        }
                        allocatedMachines {
                            name
                            hostname
                            resourcesAvail {
                                customResources { name value }
                            }
                        }
                    }
                    error {
                        errorCode
                        errorMessage
                    }
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
        """
        data = await self._post(query, {"jobId": job_id})
        edges = data["jobs"]["edges"]
        if not edges:
            return None
        edge = edges[0]
        if edge.get("error"):
            raise RuntimeError(f"GraphQL exact-job edge failed:\n{edge['error']}")
        node = edge["node"]
        status = _job_status_from_node(node)
        if status.id != job_id:
            raise RuntimeError(
                "GraphQL exact-job query returned a different job ID: "
                f"expected {job_id!r}, got {status.id!r}"
            )
        return status

    async def _get_exact_job_state(self, job_id: str) -> SchedulerJobState | None:
        status = await self.get_exact_job_status(job_id)
        return None if status is None else status.state

    async def put_file(self, content: str, path: Path, mode: int) -> None:
        raise NotImplementedError

    async def list_files(self, directory: Path) -> list[str]:
        raise NotImplementedError

    async def read_file(self, path: Path) -> str:
        raise NotImplementedError
