import base64
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from pydantic import ValidationError

from first_common.schema.base_scheduler import JobSubmitPayload
from first_gateway.platforms.schedulers.graphql_pbs import GraphQLPBSAdapter
from first_gateway.platforms.schedulers.graphql_pbs_resources import requested_resources
from first_gateway.settings import ClientState

PLACE = "-l place=scatter:exclhost:group=tier1"
TIER = {"tier1": "x4820c7"}
HOSTS = "1:host=x4820c7s3b1n0+1:host=x4820c7s4b0n0"


def job(flags: str = "", nodes: int = 2) -> JobSubmitPayload:
    return JobSubmitPayload(
        name="pilot-canary",
        queue="workq",
        account="service",
        scheduler_flags=flags,
        num_nodes=nodes,
        gpus_per_node=4,
        walltime_min=15,
        log_path=Path('/private/a"b.log'),
        script="#!/bin/bash\nprintf '%s' 'literal $value'\n",
    )


@pytest.mark.parametrize("nodes", [1, 2, 8])
def test_empty_flags_preserve_resource_contract(nodes: int) -> None:
    assert requested_resources(job(nodes=nodes), {}) == {
        "jobResources": {"index": "", "wallClockTime": 900},
        "taskCount": {"min": nodes, "max": nodes},
        "tasksResources": [
            {"index": "0" if nodes == 1 else f"0-{nodes - 1}", "gpus": 4}
        ],
    }


@pytest.mark.parametrize("sharing,value", [("excl", 1), ("exclhost", 2)])
@pytest.mark.parametrize("nodes", [1, 2, 8])
def test_native_group_and_static_task_resources(
    sharing: str, value: int, nodes: int
) -> None:
    result = requested_resources(
        job(f"-lplace=scatter:{sharing}:group=tier1", nodes), TIER
    )
    assert result["jobPlacement"] == 4
    assert result["jobPlacementSharing"] == value
    assert result["jobPlacementRescGroupName"] == "tier1"
    assert result["tasksResources"][0]["customResources"] == [
        {"name": "tier1", "value": "x4820c7"}
    ]


def test_compound_select_and_generic_group_resource() -> None:
    result = requested_resources(
        job("-lselect=1:ngpus=4+1:ngpus=4,place=scatter:excl:group=tier0"),
        {"tier0": "x4820", "service_healthy": "true"},
    )
    assert [task["index"] for task in result["tasksResources"]] == ["0", "1"]
    assert all(
        task["customResources"]
        == [
            {"name": "service_healthy", "value": "true"},
            {"name": "tier0", "value": "x4820"},
        ]
        for task in result["tasksResources"]
    )


def test_group_without_pins_leaves_host_choice_to_scheduler() -> None:
    result = requested_resources(job(PLACE, nodes=8), {})
    assert result["jobPlacementRescGroupName"] == "tier1"
    assert result["tasksResources"] == [{"index": "0-7", "gpus": 4}]


async def test_build_and_submit_temporary_hosts_uses_variables() -> None:
    requests: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "data": {
                    "createJob": {
                        "node": {"jobId": "6270.tara"},
                        "error": None,
                    }
                }
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        deps = cast(ClientState, SimpleNamespace(keycloak_clients={"tara": client}))
        adapter = await GraphQLPBSAdapter.build(
            deps,
            {
                "keycloak_client_name": "tara",
                "job_owner": "service",
                "graphql_url": "https://bridge",
                "task_resources": TIER,
            },
        )
        payload = job(f"{PLACE} -l select={HOSTS}")
        result = await adapter.submit_job(payload)
    assert result.scheduler_id == "6270.tara"
    request = requests[0]
    assert "$input: JobInput!" in request["query"]
    data = request["variables"]["input"]
    assert base64.urlsafe_b64decode(data["scriptContent"]).decode() == payload.script
    assert data["outputPath"] == data["errorPath"] == str(payload.log_path)
    tasks = data["resourcesRequested"]["tasksResources"]
    assert [task["candidateMachineName"] for task in tasks] == [
        "x4820c7s3b1n0",
        "x4820c7s4b0n0",
    ]
    assert [task["index"] for task in tasks] == ["0", "1"]
    assert all(task["gpus"] == 4 for task in tasks)
    assert all(
        task["customResources"] == [{"name": "tier1", "value": "x4820c7"}]
        for task in tasks
    )


@pytest.mark.parametrize(
    "flags",
    [
        "-q other",
        "-l",
        "-lwalltime=30",
        "-ltier1=x4820c7",
        "-lplace=scatter",
        "-lplace=pack:excl:group=tier1",
        f"{PLACE} {PLACE}",
        f"{PLACE}; touch /tmp/unwanted",
        f"{PLACE} -lselect=2:host=^bad",
        "-lselect=0",
        "-lselect=3",
        "-lselect=1",
        "-lselect=2:ngpus=3",
        "-lselect=2:mem=1gb",
        "-lselect=2:tier1=x4820c6",
        "-lselect=2:ngpus=4:ngpus=4",
        "-lselect=2+",
        "-lselect=2 -lselect=2",
        f"{PLACE} -lselect=1:host=x4820c7s3b1n0+1",
        f"{PLACE} -lselect=2:host=x4820c7s3b1n0",
        f"{PLACE} -lselect=1:host=x4820c7s3b1n0+1:host=x4820c7s3b1n0",
        f"{PLACE} -lselect=1:host=x4820c7s3b1n0+1:host=x4820c6s4b0n0",
        f"{PLACE} -lselect=1:host=x4820c7s3b1n0+1:host=x4819c7s4b0n0",
        f"{PLACE} -lselect=1:host=x4820c7s3b1n0+1:host=x4820c7s9b0n0",
        f"{PLACE} -lselect=1:host=x4820c7s3b1n0+1:host=x4820c7s4b0n0.example",
        f"-lselect={HOSTS}",
        "-lplace=scatter:excl:group=$(hostname)",
    ],
)
async def test_invalid_flags_never_submit(flags: str) -> None:
    async with httpx.AsyncClient() as client:
        adapter = GraphQLPBSAdapter(client, "service", "https://bridge", TIER)
        with patch.object(adapter, "_post", new_callable=AsyncMock) as post:
            with pytest.raises(ValueError):
                await adapter.submit_job(job(flags))
            post.assert_not_awaited()


@pytest.mark.parametrize(
    "resources",
    [
        {"host": "x4820c7s3b1n0"},
        {"ngpus": "4"},
        {"mem": "832gb"},
        {"tier1": 7},
        {"tier1": True},
        {"tier1": "^x4820c7"},
        {"tier1": "x4820c7\n"},
        {"bad-name": "value"},
        {"tier1": ""},
    ],
)
async def test_task_resource_config_is_strict(resources: dict[str, Any]) -> None:
    async with httpx.AsyncClient() as client:
        with pytest.raises(ValidationError):
            GraphQLPBSAdapter(client, "service", "https://bridge", resources)


def test_explicit_hosts_need_constrained_tier1_and_conflicts_fail_closed() -> None:
    with pytest.raises(ValueError, match="constrained tier1"):
        requested_resources(job(f"{PLACE} -lselect={HOSTS}"), {})
    with pytest.raises(ValueError, match="same-tier1"):
        requested_resources(
            job(f"{PLACE} -lselect=1:tier1=x4820c7+1:tier1=x4820c6"), {}
        )


def test_native_chunk_tier_and_quoted_flags() -> None:
    result = requested_resources(
        job(
            "-l 'place=scatter:exclhost:group=tier1' -l 'select=2:tier1=x4820c7:ngpus=4'"
        ),
        {},
    )
    assert result["tasksResources"] == [
        {
            "index": "0-1",
            "gpus": 4,
            "customResources": [{"name": "tier1", "value": "x4820c7"}],
        }
    ]
