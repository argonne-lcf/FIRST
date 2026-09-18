import base64
import json
from datetime import datetime, timezone
from pathlib import Path
from shlex import quote
from typing import Any

import httpx

from first_common.schema.base_scheduler import JobSubmitPayload, SchedulerJobState
from first_common.schema.pilot import PilotResources
from first_common.schema.resources.read import PilotJob
from first_common.schema.types import HealthCheckResult, PilotConfig
from first_gateway.platforms.schedulers.graphql_pbs import GraphQLPBSAdapter
from first_gateway.services.pilot_submitter import PilotSubmitter


def _successful_graphql_transport(
    requests: list[dict[str, Any]],
) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "data": {
                    "createJob": {
                        "node": {"jobId": "1234.test"},
                        "error": None,
                    }
                }
            },
        )

    return httpx.MockTransport(handler)


def _submitted_script(request: dict[str, Any]) -> str:
    return base64.urlsafe_b64decode(
        request["variables"]["input"]["scriptContent"]
    ).decode()


async def test_graphql_submit_maps_exact_two_node_four_gpu_request() -> None:
    requests: list[dict[str, Any]] = []
    async with httpx.AsyncClient(
        transport=_successful_graphql_transport(requests)
    ) as client:
        adapter = GraphQLPBSAdapter(
            client, "test-service", "https://scheduler.example.test/graphql"
        )
        result = await adapter.submit_job(
            JobSubmitPayload(
                name="test-pilot",
                queue="test-queue",
                account="test-account",
                scheduler_flags="",
                num_nodes=2,
                gpus_per_node=4,
                walltime_min=90,
                log_path=Path("/opt/test/logs/pilot.log"),
                script="#!/bin/bash\nexec /opt/test/bin/first-pilot\n",
            )
        )

    assert result.scheduler_id == "1234.test"
    assert len(requests) == 1
    request = requests[0]
    assert request["variables"]["input"]["resourcesRequested"] == {
        "jobResources": {"index": "", "wallClockTime": 5400},
        "taskCount": {"min": 2, "max": 2},
        "tasksResources": [{"index": "0-1", "gpus": 4}],
    }
    assert _submitted_script(request) == (
        "#!/bin/bash\nexec /opt/test/bin/first-pilot\n"
    )


async def test_graphql_submitter_propagates_exact_runtime_allocation() -> None:
    config_input = {
        "scheduler_adapter": (
            "first_gateway.platforms.schedulers.graphql_pbs.GraphQLPBSAdapter"
        ),
        "scheduler_config": {},
        "job_walltime_min": 90,
        "queue": "test-queue",
        "account": "test-account",
        "max_num_nodes": 2,
        "gpus_per_node": 4,
        "workdir": "/opt/test/pilot-workdir",
        "external_port": 18443,
        "nginx_path": "/opt/test/nginx",
        "ip_allowlist": ["192.0.2.10/32"],
        "node_file_env": "TEST_NODEFILE",
        "gpu_discovery": {
            "method": "pals",
            "launcher_path": "/opt/test/mpiexec",
        },
        "submit_script_preamble": "#!/bin/bash\nset -eu",
        "pilot_path": "/opt/test/first pilot",
        "pilot_config_path": "/opt/test/pilot config.yaml",
    }
    config = PilotConfig.model_validate(config_input)
    pilot_job = PilotJob(
        kind="PilotJob",
        name="test-pilot",
        uid=1,
        created_at=datetime.now(timezone.utc),
        scheduler_job_id="",
        cluster_name="test-cluster",
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

    requests: list[dict[str, Any]] = []
    async with httpx.AsyncClient(
        transport=_successful_graphql_transport(requests)
    ) as client:
        adapter = GraphQLPBSAdapter(
            client, "test-service", "https://scheduler.example.test/graphql"
        )
        await PilotSubmitter(config, adapter, "unused-ca", "unused-key").submit(
            pilot_job
        )

    script = _submitted_script(requests[0])
    expected_runtime_env = {
        "PILOT_CONFIG_FILE": str(config.pilot_config_path),
        "PILOT_JOB_NAME": pilot_job.name,
        "PILOT_EXTERNAL_PORT": str(config.external_port),
        "PILOT_NGINX_PATH": str(config.nginx_path),
        "PILOT_IP_ALLOWLIST": json.dumps(config.ip_allowlist, separators=(",", ":")),
        "PILOT_WORKDIR": str(config.workdir),
        "PILOT_NODE_FILE_ENV": config.node_file_env,
        "PILOT_GPU_DISCOVERY": json.dumps(
            config.gpu_discovery.model_dump(mode="json"), separators=(",", ":")
        ),
        "PILOT_NUM_NODES": str(pilot_job.num_nodes),
        "PILOT_GPUS_PER_NODE": str(pilot_job.gpus_per_node),
    }
    for key, value in expected_runtime_env.items():
        assert f"{key}={quote(value)}" in script
    assert "PILOT_PALS_PATH" not in script
    assert "PILOT_IP_ALLOWLIST_JSON" not in script
    assert script.endswith(f"{quote(str(config.pilot_path))}\n")
