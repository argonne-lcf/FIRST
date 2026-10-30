import json
from typing import Any

import httpx
import pytest

from first_common.schema.base_scheduler import SchedulerJobState
from first_gateway.platforms.schedulers.graphql_pbs import (
    GraphQLPBSAdapter,
    _job_status_from_node,
)


def node() -> dict[str, Any]:
    """PBS's first machine deliberately differs from lexical host order."""
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


@pytest.mark.parametrize("reverse", [False, True])
def test_head_follows_first_returned_machine(reverse: bool) -> None:
    raw = node()
    if reverse:
        raw["allocatedMachines"].reverse()
    first = raw["allocatedMachines"][0]
    status = _job_status_from_node(raw)
    assert status.state == SchedulerJobState.running
    assert status.head_node_hostname == first["hostname"]
    assert (
        status.head_node_ip_address
        == first["resourcesAvail"]["customResources"][0]["value"]
    )


@pytest.mark.parametrize("ips", ["10.1.2.3,10.1.2.4", "10.1.2.3 10.1.2.4"])
def test_head_uses_first_hsn_address(ips: str) -> None:
    raw = node()
    raw["allocatedMachines"][0]["resourcesAvail"]["customResources"][0]["value"] = ips
    assert _job_status_from_node(raw).head_node_ip_address == "10.1.2.3"


@pytest.mark.parametrize(
    "allocation", [{}, {"allocatedMachines": None}, {"allocatedMachines": []}]
)
def test_missing_allocation_keeps_status_without_a_head(
    allocation: dict[str, Any],
) -> None:
    raw = node()
    del raw["allocatedMachines"]
    raw.update(allocation)
    status = _job_status_from_node(raw)
    assert status.state == SchedulerJobState.running
    assert status.head_node_hostname is None
    assert status.head_node_ip_address is None


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
def test_missing_head_hsn_never_scans_or_uses_management_ip(resources: Any) -> None:
    raw = node()
    head = raw["allocatedMachines"][0]
    head["resourcesAvail"] = resources
    status = _job_status_from_node(raw)
    assert status.state == SchedulerJobState.running
    assert status.head_node_hostname == head["hostname"]
    assert status.head_node_ip_address is None


@pytest.mark.parametrize("state", [0, 10])
def test_inactive_job_does_not_advertise_stale_allocation(state: int) -> None:
    raw = node()
    raw["status"]["state"] = state
    status = _job_status_from_node(raw)
    assert status.state in (SchedulerJobState.queued, SchedulerJobState.gone)
    assert status.head_node_hostname is None
    assert status.head_node_ip_address is None


@pytest.mark.parametrize("exact", [False, True])
@pytest.mark.parametrize("include_extension", [False, True])
async def test_both_queries_resolve_head_without_extension(
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
    assert status.head_node_ip_address == "10.124.165.113"
