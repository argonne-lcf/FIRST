import base64
import logging
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
    8: SchedulerJobState.gone,  # Suspended
    9: SchedulerJobState.exiting,  # Exiting
    10: SchedulerJobState.gone,  # Done
    11: SchedulerJobState.gone,  # Failed
    12: SchedulerJobState.gone,  # Deleted
    13: SchedulerJobState.gone,  # Moved
    14: SchedulerJobState.queued,  # Unlicensed
}

# States in which the job is actually placed on machines, so allocatedMachines
# (and thus hsn_ips) is meaningful
_ACTIVE_STATES = frozenset({5, 6, 7, 9})

_HSN_RESOURCE_NAME = "hsn_ips"


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

    async def _post(self, query: str) -> dict[str, Any]:
        resp = await self.client.post(self.url, json={"query": query})
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
        query {{
            jobs ( filter: {{owner: "{self.owner}", withHistoryJobs: true}} ) {{
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
                }}
            }}
        }}
        """
        data = await self._post(query)
        edges = data.get("jobs", {}).get("edges") or []

        results: list[JobStatusInfo] = []
        for edge in edges:
            if edge.get("error"):
                logger.warning("Skipping job with error: %s", edge["error"])
                continue
            node = edge["node"]
            job_id = (node["jobId"] or "").strip()

            state_code: int | None = (node.get("status") or {}).get("state")
            state = _STATE_MAP.get(state_code) if state_code is not None else None
            if state is None:
                logger.warning("Unknown job state %r for job %r", state_code, job_id)
                state = SchedulerJobState.gone

            resources = (node.get("resourcesRequested") or {}).get("jobResources") or {}
            walltime_sec = resources.get("wallClockTime") or 0

            head_ip = None
            head_hostname = None
            if state_code in _ACTIVE_STATES:
                machines = node.get("allocatedMachines") or []
                head_ip = _head_node_ip(machines)
                head_hostname = _head_node_hostname(machines)

            results.append(
                JobStatusInfo(
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
            )

        return results

    async def terminate_job(self, job_id: str) -> None:
        query = f"""
        mutation {{
            deleteJob (jobId: "{job_id}", input: {{force: true}}) {{
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
        payload = data["deleteJob"]
        if payload.get("error"):
            raise RuntimeError(f"GraphQL deleteJob failed:\n{payload['error']}")

    async def put_file(self, content: str, path: Path, mode: int) -> None:
        raise NotImplementedError

    async def list_files(self, directory: Path) -> list[str]:
        raise NotImplementedError

    async def read_file(self, path: Path) -> str:
        raise NotImplementedError
