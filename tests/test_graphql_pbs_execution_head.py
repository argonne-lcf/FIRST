import json
from typing import Any

import httpx
import pytest

from first_common.schema.base_scheduler import AllocatedNode, SchedulerJobState
from first_gateway.platforms.schedulers.graphql_pbs import (
    GraphQLPBSAdapter,
    _job_status_from_node,
)


def node() -> dict[str, Any]:
    return {
        "jobId": "6349.tara",
        "name": "first_pilot_test",
        "status": {"state": 7},
        "extension": None,
        "allocatedMachines": [
            {
                "name": host,
                "hostname": host + ".hostmgmt.example",
                "resourcesAvail": {
                    "customResources": [{"name": "hsn_ips", "value": ip}]
                },
            }
            for host, ip in (
                ("x4820c6s4b0n0", "10.124.165.113"),
                ("x4820c6s1b1n0", "10.124.165.93"),
            )
        ],
    }


def test_status_reports_every_allocated_machine() -> None:
    status = _job_status_from_node(node())
    assert status.state == SchedulerJobState.running
    assert status.allocated_nodes == [
        AllocatedNode("x4820c6s4b0n0.hostmgmt.example", "10.124.165.113"),
        AllocatedNode("x4820c6s1b1n0.hostmgmt.example", "10.124.165.93"),
    ]


@pytest.mark.parametrize("ips", ["10.1.2.3,10.1.2.4", "10.1.2.3 10.1.2.4"])
def test_machine_uses_first_hsn_address(ips: str) -> None:
    raw = node()
    raw["allocatedMachines"][0]["resourcesAvail"]["customResources"][0]["value"] = ips
    assert _job_status_from_node(raw).allocated_nodes[0].ip_address == "10.1.2.3"


@pytest.mark.parametrize(
    "allocation", [{}, {"allocatedMachines": None}, {"allocatedMachines": []}]
)
def test_missing_allocation_keeps_status_without_nodes(
    allocation: dict[str, Any],
) -> None:
    raw = node()
    del raw["allocatedMachines"]
    raw.update(allocation)
    status = _job_status_from_node(raw)
    assert status.state == SchedulerJobState.running
    assert status.allocated_nodes == []


@pytest.mark.parametrize(
    "resources",
    [
        None,
        {},
        {"customResources": None},
        {"customResources": []},
        {"customResources": [{"name": "other", "value": "192.0.2.1"}]},
        {"customResources": [{"name": "hsn_ips", "value": ""}]},
    ],
)
def test_machine_without_hsn_is_skipped_not_given_management_ip(resources: Any) -> None:
    raw = node()
    raw["allocatedMachines"][0]["resourcesAvail"] = resources
    status = _job_status_from_node(raw)
    assert status.state == SchedulerJobState.running
    assert [n.ip_address for n in status.allocated_nodes] == ["10.124.165.93"]


@pytest.mark.parametrize("state", [0, 10])
def test_inactive_job_does_not_advertise_stale_allocation(state: int) -> None:
    raw = node()
    raw["status"]["state"] = state
    status = _job_status_from_node(raw)
    assert status.state in (SchedulerJobState.queued, SchedulerJobState.gone)
    assert status.allocated_nodes == []


@pytest.mark.parametrize("exact", [False, True])
@pytest.mark.parametrize("include_extension", [False, True])
async def test_both_queries_report_nodes_without_extension(
    exact: bool, include_extension: bool
) -> None:
    queries = []
    raw = node()
    if not include_extension:
        del raw["extension"]

    def handler(request: httpx.Request) -> httpx.Response:
        queries.append(json.loads(request.content)["query"])
        return httpx.Response(
            200,
            json={
                "data": {
                    "jobs": {
                        "edges": [{"node": raw, "error": None}],
                        "pageInfo": {"hasNextPage": False, "endCursor": ""},
                    }
                }
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = GraphQLPBSAdapter(client, "service", "https://bridge.invalid")
        status = (
            (await adapter.get_exact_job_status(raw["jobId"]))
            if exact
            else (await adapter.get_job_statuses())[0]
        )
    assert len(queries) == 1 and "extension" not in queries[0]
    assert status is not None
    assert len(status.allocated_nodes) == 2
