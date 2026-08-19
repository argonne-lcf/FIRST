import asyncio
import base64
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Self

from httpx import AsyncClient

from first_common.schema.base_scheduler import (
    JobStatusInfo,
    JobSubmitPayload,
    JobSubmitResult,
    SchedulerAdapter,
    SchedulerJobState,
)
from first_gateway.settings import ClientState

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
_DELETE_POLL_ATTEMPTS = 45
_DELETE_POLL_INTERVAL_SEC = 1.0
_STATUS_PAGE_SIZE = 500
_STATUS_MAX_PAGES = 10


def _parse_epoch_micros(raw: int | None) -> datetime | None:
    """Convert an EpochTime (microseconds since epoch) to a UTC datetime."""
    if not raw:
        return None
    return datetime.fromtimestamp(raw / 1_000_000, tz=timezone.utc)


def _head_node_ip(machines: list[dict[str, Any]]) -> str | None:
    """
    Pull the head node's first hsn_ips address off the job's allocated machines.

    The head node is the primary execution host = the first vnode in the allocation.
    """
    if not machines:
        return None
    head = machines[0]
    resources_avail = head.get("resourcesAvail") or {}
    for pair in resources_avail.get("customResources") or []:
        if pair.get("name") == _HSN_RESOURCE_NAME:
            ips = pair["value"].replace(",", " ").split()
            return ips[0] if ips else None
    return None


def _head_node_hostname(machines: list[dict[str, Any]]) -> str | None:
    """Pull the head node's hostname off the job's allocated machines."""
    if not machines:
        return None
    return machines[0].get("hostname") or None


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
        head_ip = _head_node_ip(machines)
        head_hostname = _head_node_hostname(machines)

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
    def __init__(self, client: AsyncClient, owner: str, url: str) -> None:
        self.client = client
        self.owner = owner
        self.url = url

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
        return cls(client=deps.keycloak_clients[name], owner=owner, url=graphql_url)

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

        node_index = f"0-{job.num_nodes - 1}" if job.num_nodes > 1 else "0"

        query = f"""
        mutation {{
            createJob (
                input: {{
                    scriptContent: "{script_b64}"
                    name: "{job.name}"
                    resourcesRequested: {{
                        jobResources: {{
                            index: ""
                            wallClockTime: {job.walltime_min * 60}
                        }}
                        taskCount: {{
                            min: {job.num_nodes}
                            max: {job.num_nodes}
                        }}
                        tasksResources: [
                            {{
                                index: "{node_index}"
                                gpus: {job.gpus_per_node}
                            }}
                        ]
                    }}
                    queue: {{
                        name: "{job.queue}"
                    }}
                    accountingId: "{job.account}"
                    errorPath: "{job.log_path}"
                    outputPath: "{job.log_path}"
                    joinFiles: true
                }}
            ) {{
                node {{
                    jobId
                }}
                error {{
                    errorCode
                    errorMessage
                }}
            }}
        }}
        """
        data = await self._post(query)
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
                        submitTime
                        startTime
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
                status = _job_status_from_node(node)
                if status.id not in seen_ids:
                    seen_ids.add(status.id)
                    results.append(status)

            page_info = connection.get("pageInfo")
            if not isinstance(page_info, dict) or not isinstance(
                page_info.get("hasNextPage"), bool
            ):
                raise RuntimeError("GraphQL jobs query returned malformed pageInfo")
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

        query = """
        mutation DeleteJob($jobId: String!) {
            deleteJob (jobId: $jobId, input: {force: true}) {
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

        # A successful mutation is only an acknowledgement. Retain the DB
        # allocation until this exact scheduler ID is terminal or absent.
        for attempt in range(_DELETE_POLL_ATTEMPTS):
            state = await self._get_exact_job_state(job_id)
            if state is None or state == SchedulerJobState.gone:
                return
            if attempt + 1 < _DELETE_POLL_ATTEMPTS:
                await asyncio.sleep(_DELETE_POLL_INTERVAL_SEC)

        raise TimeoutError(
            f"scheduler job {job_id!r} did not become absent/gone after "
            f"{_DELETE_POLL_ATTEMPTS} polls"
        )

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
                        submitTime
                        startTime
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
        connection = data.get("jobs")
        if not isinstance(connection, dict):
            raise RuntimeError("GraphQL exact-job query returned no connection")
        edges = connection.get("edges")
        if not isinstance(edges, list):
            raise RuntimeError("GraphQL exact-job query returned malformed edges")
        page_info = connection.get("pageInfo")
        if not isinstance(page_info, dict) or page_info.get("hasNextPage") is not False:
            raise RuntimeError("GraphQL exact-job query was not an exact page")
        if not edges:
            return None
        if len(edges) != 1 or not isinstance(edges[0], dict):
            raise RuntimeError("GraphQL exact-job query returned non-exact edges")
        edge = edges[0]
        if edge.get("error"):
            raise RuntimeError(f"GraphQL exact-job edge failed:\n{edge['error']}")
        node = edge.get("node")
        if not isinstance(node, dict):
            raise RuntimeError("GraphQL exact-job edge returned no node")
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
