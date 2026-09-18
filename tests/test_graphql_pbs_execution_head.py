import copy
import json
from typing import Any

import httpx
import pytest

from first_common.schema.base_scheduler import SchedulerJobState
from first_gateway.platforms.schedulers.graphql_pbs import (
    GraphQLPBSAdapter,
    _execution_head,
    _job_status_from_node,
)

HOSTS = [
    "x4820c6" + suffix
    for suffix in (
        "s4b0n0",
        "s1b1n0",
        "s3b1n0",
        "s4b1n0",
        "s5b0n0",
        "s5b1n0",
        "s6b0n0",
        "s6b1n0",
    )
]


def node() -> dict[str, Any]:
    """Sanitized 6349 topology: PBS head s4b0 is not the lexical first host."""
    return {
        "jobId": "6349.tara-north-pbs-01.lab.alcf.anl.gov",
        "name": "first_pilot_tara_v2_tara-pilot-test",
        "status": {"state": 7},
        "extension": {
            "exec_vnode": "+".join(
                f"({host}:ngpus=4:ncpus=288:mem=872415232kb)" for host in HOSTS
            ),
            "job_state": "R",
            "run_count": 1,
        },
        "allocatedMachines": [
            {
                "name": host,
                "hostname": host + ".hostmgmt2820.north.tara.alcf.anl.gov",
                "resourcesAvail": {
                    "customResources": [
                        {
                            "name": "hsn_ips",
                            "value": "10.124.165.113"
                            if host == HOSTS[0]
                            else "10.124.165.93",
                        }
                    ]
                },
            }
            for host in sorted(HOSTS)
        ],
    }


@pytest.mark.parametrize("reverse", [False, True])
def test_head_follows_pbs_exec_vnode_not_machine_order(reverse: bool) -> None:
    raw = node()
    if reverse:
        raw["allocatedMachines"].reverse()
    status = _job_status_from_node(raw)
    assert status.state == SchedulerJobState.running
    assert status.head_node_hostname == (
        "x4820c6s4b0n0.hostmgmt2820.north.tara.alcf.anl.gov"
    )
    assert status.head_node_ip_address == "10.124.165.113"


@pytest.mark.parametrize(
    "extension",
    [
        None,
        {},
        [],
        "unparsed",
        {"exec_vnode": None},
        {"exec_vnode": ""},
        {"exec_vnode": "x4820c6s4b0n0:ncpus=288"},
        {"exec_vnode": "(x4820c6s4b0n0:ncpus=288"},
        {"exec_vnode": "(x4820c6s4b0n0:ncpus=288)+malformed"},
        {"exec_vnode": "(x4820c6s4b0n0:ncpus=288)+(bad)"},
        {"exec_vnode": "(x4820c6s4b0n0:ncpus=288)+()"},
        {"exec_vnode": "(x4820c6s4b0n0:ncpus=288\n)"},
        {"exec_vnode": "(x4820c6s4b0n0:ncpus=288)" + " " * 65536},
    ],
)
def test_missing_or_malformed_multi_node_head_never_guesses(extension: Any) -> None:
    raw = node()
    raw["extension"] = extension
    status = _job_status_from_node(raw)
    assert status.state == SchedulerJobState.running  # Disposal/status still work.
    assert status.head_node_hostname is None
    assert status.head_node_ip_address is None


def test_unambiguous_legacy_single_machine_only() -> None:
    machine: dict[str, Any] = node()["allocatedMachines"][0]
    assert _execution_head([machine], None) is machine
    assert _execution_head([machine], {}) is machine
    assert _execution_head([], None) is None
    assert _execution_head([machine], {"exec_vnode": None}) is None
    assert _execution_head([machine], {"exec_vnode": "malformed"}) is None


def test_missing_or_duplicate_head_match_is_not_an_endpoint() -> None:
    raw = node()
    machines = raw["allocatedMachines"]
    head = next(machine for machine in machines if machine["name"] == HOSTS[0])
    assert (
        _execution_head(
            [machine for machine in machines if machine is not head], raw["extension"]
        )
        is None
    )
    assert _execution_head([*machines, copy.deepcopy(head)], raw["extension"]) is None
    duplicate_alias = {"name": "different-vnode", "hostname": HOSTS[0]}
    assert _execution_head([*machines, duplicate_alias], raw["extension"]) is None


def test_multi_vnode_chunk_and_name_or_hostname_identity() -> None:
    machine = {"name": "head[0]", "hostname": "physical-head.example"}
    extension = {"exec_vnode": "(head[0]:ncpus=4+head[1]:ncpus=4)+(other:ncpus=8)"}
    assert _execution_head([{"name": "other"}, machine], extension) is machine
    assert (
        _execution_head([machine], {"exec_vnode": "(physical-head.example:ncpus=4)"})
        is machine
    )
    assert _execution_head([machine], {"exec_vnode": "(physical-head:ncpus=4)"}) is None


@pytest.mark.parametrize("exact", [False, True])
async def test_both_graphql_queries_request_authoritative_extension(
    exact: bool,
) -> None:
    queries = []
    raw = node()

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
        adapter = GraphQLPBSAdapter(
            client, "openinference_svc", "https://bridge.invalid"
        )
        status = (
            (await adapter.get_exact_job_status(raw["jobId"]))
            if exact
            else (await adapter.get_job_statuses())[0]
        )
    assert len(queries) == 1 and "extension" in queries[0]
    assert "\nenv\n" not in queries[0]
    assert status is not None
    assert status.head_node_ip_address == "10.124.165.113"
