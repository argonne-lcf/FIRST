import json
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from first_common.schema.base_scheduler import (
    JobStatusInfo,
    SchedulerJobState,
)
from first_common.schema.types import PilotConfig
from first_gateway.platforms.schedulers import graphql_pbs
from first_gateway.platforms.schedulers.graphql_pbs import GraphQLPBSAdapter
from first_gateway.services.pilot_submitter import PilotSubmitter


def _job_node(job_id: str, state: int) -> dict[str, object]:
    return {
        "jobId": job_id,
        "name": "__FIRST_PILOT_nemotron-canary",
        "submitTime": 1_700_000_000_000_000,
        "startTime": 1_700_000_100_000_000,
        "status": {"state": state},
        "resourcesRequested": {"jobResources": {"wallClockTime": 5400}},
        "allocatedMachines": [
            {
                "hostname": "x3001",
                "resourcesAvail": {
                    "customResources": [{"name": "hsn_ips", "value": "10.1.2.3"}]
                },
            }
        ],
    }


def _jobs_payload(
    edges: list[dict[str, object]], *, has_next: bool = False, cursor: str = ""
) -> dict[str, object]:
    return {
        "data": {
            "jobs": {
                "edges": edges,
                "pageInfo": {
                    "hasNextPage": has_next,
                    "endCursor": cursor,
                },
            }
        }
    }


async def test_graphql_active_statuses_paginate_and_never_query_history() -> None:
    requests: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        requests.append(body)
        cursor = body["variables"]["cursor"]
        if cursor is None:
            payload = _jobs_payload(
                [{"node": _job_node("101.tara", 7), "error": None}],
                has_next=True,
                cursor="page-2",
            )
        else:
            payload = _jobs_payload([{"node": _job_node("102.tara", 9), "error": None}])
        return httpx.Response(200, json=payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        statuses = await GraphQLPBSAdapter(
            client, "openinference_svc", "https://bridge"
        ).get_job_statuses()

    assert [status.id for status in statuses] == ["101.tara", "102.tara"]
    assert [status.state for status in statuses] == [
        SchedulerJobState.running,
        SchedulerJobState.exiting,
    ]
    assert [request["variables"]["cursor"] for request in requests] == [
        None,
        "page-2",
    ]
    assert all(
        "withHistoryJobs: false" in str(request["query"]) for request in requests
    )


async def test_graphql_statuses_fail_closed_on_edge_unknown_and_duplicate() -> None:
    responses = [
        _jobs_payload(
            [
                {
                    "node": None,
                    "error": {"errorCode": 7, "errorMessage": "denied"},
                }
            ]
        ),
        _jobs_payload([{"node": _job_node("101.tara", 99), "error": None}]),
        _jobs_payload(
            [
                {"node": _job_node("101.tara", 7), "error": None},
                {"node": _job_node("101.tara", 7), "error": None},
            ]
        ),
    ]
    for payload, message in zip(
        responses,
        ("edge failed", "unknown GraphQL job state", "duplicated job ID"),
        strict=True,
    ):
        transport = httpx.MockTransport(
            lambda _request, payload=payload: httpx.Response(200, json=payload)
        )
        async with httpx.AsyncClient(transport=transport) as client:
            adapter = GraphQLPBSAdapter(client, "svc", "https://bridge")
            with pytest.raises(RuntimeError, match=message):
                await adapter.get_job_statuses()


async def test_graphql_suspended_state_is_dying_not_gone() -> None:
    payload = _jobs_payload([{"node": _job_node("101.tara", 8), "error": None}])
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, json=payload)
        )
    ) as client:
        status = await GraphQLPBSAdapter(
            client, "svc", "https://bridge"
        ).get_exact_job_status("101.tara")

    assert status is not None
    assert status.state == SchedulerJobState.exiting
    assert status.head_node_ip_address == "10.1.2.3"


async def test_graphql_get_endpoint_rejects_running_to_exiting_race() -> None:
    """A head IP retained during scheduler exit is not a ready endpoint."""
    status = JobStatusInfo(
        id="1234.scheduler",
        name="__FIRST_PILOT_model-canary",
        state=SchedulerJobState.exiting,
        created_at=datetime.now(timezone.utc),
        started_at=datetime.now(timezone.utc),
        walltime_minutes=90,
        head_node_ip_address="10.1.2.3",
        head_node_hostname="node-1",
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
            "account": "service",
            "workdir": "/service/workdir",
            "external_port": 18443,
            "nginx_path": "/service/nginx",
            "ip_allowlist": ["10.0.0.0/8"],
            "node_file_env": "PBS_NODEFILE",
            "submit_script_preamble": "#!/bin/bash",
            "pilot_path": "/service/first-pilot",
        }
    )
    async with httpx.AsyncClient() as client:
        adapter = GraphQLPBSAdapter(client, "service", "https://bridge")
        submitter = PilotSubmitter(config, adapter, "unused-ca", "unused-key")
        with patch.object(
            adapter,
            "get_job_statuses",
            new=AsyncMock(return_value=[status]),
        ):
            with pytest.raises(ValueError, match="No ready endpoint"):
                await submitter.get_endpoint("model-canary")


async def test_graphql_delete_polls_exact_id_through_exiting_to_gone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    states = iter((7, 9, 12))
    requests: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        requests.append(body)
        if "mutation DeleteJob" in body["query"]:
            return httpx.Response(
                200,
                json={
                    "data": {
                        "deleteJob": {
                            "node": {"jobId": "101.tara"},
                            "error": None,
                        }
                    }
                },
            )
        return httpx.Response(
            200,
            json=_jobs_payload(
                [{"node": _job_node("101.tara", next(states)), "error": None}]
            ),
        )

    monkeypatch.setattr(graphql_pbs, "_DELETE_POLL_INTERVAL_SEC", 0)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await GraphQLPBSAdapter(client, "svc", "https://bridge").terminate_job(
            "101.tara"
        )

    assert len(requests) == 4
    assert all(request["variables"]["jobId"] == "101.tara" for request in requests)


async def test_graphql_delete_accepts_explicit_absence_after_lost_ack() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        body = json.loads(request.content)
        if "mutation DeleteJob" in body["query"]:
            return httpx.Response(
                200,
                json={
                    "data": {
                        "deleteJob": {
                            "node": None,
                            "error": {
                                "errorCode": 15001,
                                "errorMessage": "unknown job id",
                            },
                        }
                    }
                },
            )
        return httpx.Response(200, json=_jobs_payload([]))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await GraphQLPBSAdapter(client, "svc", "https://bridge").terminate_job(
            "101.tara"
        )
    assert calls == 2


async def test_graphql_delete_error_with_live_job_remains_failure() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        if "mutation DeleteJob" in body["query"]:
            return httpx.Response(
                200,
                json={
                    "data": {
                        "deleteJob": {
                            "node": None,
                            "error": {"errorCode": 1, "errorMessage": "failed"},
                        }
                    }
                },
            )
        return httpx.Response(
            200,
            json=_jobs_payload([{"node": _job_node("101.tara", 7), "error": None}]),
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(RuntimeError, match="deleteJob failed"):
            await GraphQLPBSAdapter(client, "svc", "https://bridge").terminate_job(
                "101.tara"
            )


async def test_graphql_delete_timeout_and_malformed_id_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        body = json.loads(request.content)
        if "mutation DeleteJob" in body["query"]:
            payload: dict[str, Any] = {
                "data": {
                    "deleteJob": {
                        "node": {"jobId": "101.tara"},
                        "error": None,
                    }
                }
            }
        else:
            payload = _jobs_payload([{"node": _job_node("101.tara", 7), "error": None}])
        return httpx.Response(200, json=payload)

    monkeypatch.setattr(graphql_pbs, "_DELETE_POLL_ATTEMPTS", 2)
    monkeypatch.setattr(graphql_pbs, "_DELETE_POLL_INTERVAL_SEC", 0)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = GraphQLPBSAdapter(client, "svc", "https://bridge")
        with pytest.raises(TimeoutError, match="did not become absent/gone"):
            await adapter.terminate_job("101.tara")
        before = calls
        with pytest.raises(ValueError, match="invalid PBS scheduler job ID"):
            await adapter.terminate_job('101.tara" } mutation { exploit')
        assert calls == before
