import base64
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from first_common.schema.base_scheduler import (
    JobStatusInfo,
    JobSubmitPayload,
    SchedulerJobState,
)
from first_common.schema.pilot import PilotResources
from first_common.schema.resources.read import PilotJob
from first_common.schema.types import HealthCheckResult, PilotConfig
from first_gateway.platforms.schedulers.graphql_pbs import GraphQLPBSAdapter
from first_gateway.services.pilot_submitter import PilotSubmitter


def _successful_graphql_transport(queries: list[str]) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        query = json.loads(request.content)["query"]
        queries.append(query)
        return httpx.Response(
            200,
            json={
                "data": {
                    "createJob": {
                        "node": {"jobId": "1234.tara"},
                        "error": None,
                    }
                }
            },
        )

    return httpx.MockTransport(handler)


def _submitted_script(query: str) -> str:
    match = re.search(r'scriptContent:\s*"([A-Za-z0-9_=-]+)"', query)
    assert match is not None
    return base64.urlsafe_b64decode(match.group(1)).decode()


async def test_graphql_submit_maps_exact_two_node_four_gpu_request() -> None:
    queries: list[str] = []
    async with httpx.AsyncClient(
        transport=_successful_graphql_transport(queries)
    ) as client:
        adapter = GraphQLPBSAdapter(client, "openinference_svc", "https://bridge")
        result = await adapter.submit_job(
            JobSubmitPayload(
                name="first-nemotron",
                queue="workq",
                account="inference_service",
                scheduler_flags="",
                num_nodes=2,
                gpus_per_node=4,
                walltime_min=90,
                log_path=Path("/service/logs/nemotron.log"),
                script="#!/bin/bash\nexec /service/bin/first-pilot\n",
            )
        )

    assert result.scheduler_id == "1234.tara"
    assert len(queries) == 1
    query = queries[0]
    assert "wallClockTime: 5400" in query
    assert re.search(r"taskCount:\s*\{\s*min: 2\s*max: 2", query)
    assert re.search(r'tasksResources:\s*\[\s*\{\s*index: "0-1"\s*gpus: 4', query)
    assert _submitted_script(query) == ("#!/bin/bash\nexec /service/bin/first-pilot\n")


async def test_graphql_submitter_propagates_exact_runtime_allocation() -> None:
    config = PilotConfig.model_validate(
        {
            "scheduler_adapter": (
                "first_gateway.platforms.schedulers.graphql_pbs.GraphQLPBSAdapter"
            ),
            "scheduler_config": {},
            "job_walltime_min": 90,
            "queue": "workq",
            "account": "inference_service",
            "max_num_nodes": 2,
            "gpus_per_node": 4,
            "workdir": "/service/first-v2/workdir",
            "external_port": 18443,
            "nginx_path": "/service/nginx/sbin/nginx",
            "ip_allowlist": ["10.124.176.33/32"],
            "node_file_env": "PBS_NODEFILE",
            "pals_path": "/opt/cray/pals/1.8/bin/mpiexec",
            "submit_script_preamble": "#!/bin/bash\nset -eu",
            "pilot_path": "/service/first v2/bin/first-pilot",
            "pilot_config_path": "/service/first v2/config.yaml",
        }
    )
    pilot_job = PilotJob(
        kind="PilotJob",
        name="nemotron-canary",
        uid=1,
        created_at=datetime.now(timezone.utc),
        scheduler_job_id="",
        cluster_name="tara-production",
        scheduler_state=SchedulerJobState.pending_submit,
        manager_url="",
        manager_health=HealthCheckResult.unknown,
        resources=PilotResources(hosts=[]),
        assigned_replicas=[],
        claimed_gpu_ids=[],
        walltime_min=90,
        num_nodes=2,
        gpus_per_node=4,
    )

    queries: list[str] = []
    async with httpx.AsyncClient(
        transport=_successful_graphql_transport(queries)
    ) as client:
        adapter = GraphQLPBSAdapter(client, "openinference_svc", "https://bridge")
        await PilotSubmitter(config, adapter, "unused-ca", "unused-key").submit(
            pilot_job
        )

    script = _submitted_script(queries[0])
    assert "PILOT_CONFIG_FILE='/service/first v2/config.yaml'" in script
    assert "PILOT_JOB_NAME=nemotron-canary" in script
    assert "PILOT_NUM_NODES=2" in script
    assert "PILOT_GPUS_PER_NODE=4" in script
    assert "PILOT_PALS_PATH=/opt/cray/pals/1.8/bin/mpiexec" in script
    assert script.endswith("'/service/first v2/bin/first-pilot'\n")


async def test_graphql_get_endpoint_rejects_running_to_exiting_race() -> None:
    """A head IP retained during PBS exit is no longer a ready endpoint."""
    status = JobStatusInfo(
        id="1234.tara",
        name="__FIRST_PILOT_nemotron-canary",
        state=SchedulerJobState.exiting,
        created_at=datetime.now(timezone.utc),
        started_at=datetime.now(timezone.utc),
        walltime_minutes=90,
        head_node_ip_address="10.1.2.3",
        head_node_hostname="x3001",
    )
    config = PilotConfig.model_validate(
        {
            "scheduler_adapter": (
                "first_gateway.platforms.schedulers.graphql_pbs.GraphQLPBSAdapter"
            ),
            "job_walltime_min": 90,
            "max_num_nodes": 2,
            "gpus_per_node": 4,
            "queue": "workq",
            "account": "inference_service",
            "workdir": "/service/workdir",
            "external_port": 18443,
            "nginx_path": "/service/nginx",
            "ip_allowlist": ["10.124.176.33/32"],
            "node_file_env": "PBS_NODEFILE",
            "submit_script_preamble": "#!/bin/bash",
            "pilot_path": "/service/first-pilot",
        }
    )
    async with httpx.AsyncClient() as client:
        adapter = GraphQLPBSAdapter(client, "openinference_svc", "https://bridge")
        submitter = PilotSubmitter(config, adapter, "unused-ca", "unused-key")
        with patch.object(
            adapter,
            "get_job_statuses",
            new=AsyncMock(return_value=[status]),
        ):
            with pytest.raises(ValueError, match="No ready endpoint"):
                await submitter.get_endpoint("nemotron-canary")
