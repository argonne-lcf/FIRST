"""Offline coverage of pilot-native quarantine alerts and cleanup protection.

Mocked bridge/Slack results are not claims of PBS isolation or Slack delivery.
"""

import copy
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from first_common.schema.base_scheduler import SchedulerJobState
from first_common.schema.resources.runtime import CommittedAlert, HealthAlertState
from first_gateway.controllers.workers.health_alerter.checks import (
    check_quarantined_nodes,
)
from first_gateway.controllers.workers.health_alerter.slack import build_alert_blocks
from first_gateway.controllers.workers.health_alerter.worker import (
    HealthAlerter,
    advance,
)
from first_gateway.platforms.schedulers.graphql_pbs import GraphQLPBSAdapter

HOST = "x4820c0s4b0n0"
JOB = "100.tara-north-pbs-01.lab.alcf.anl.gov"
SOURCE = "99.tara-north-pbs-01.lab.alcf.anl.gov"
OPERATION = "a1b2c3d4" + "0" * 24


def _row(
    *, state: int = 0, environment: str = "dev", operation: str = OPERATION
) -> dict[str, Any]:
    meta = {
        "FIRST_QUAR_ENV": environment,
        "FIRST_QUAR_HOST": HOST,
        "FIRST_QUAR_OP": operation,
        "FIRST_QUAR_SOURCE": SOURCE,
        "FIRST_QUAR_REASON": "unexplained_memory_loss",
        "FIRST_QUAR_EVIDENCE": "/service/dev/evidence/admission.json",
    }
    return {
        "jobId": JOB,
        "name": f"first_quar_{environment}_{HOST}_{operation[:8]}",
        "owner": "openinference_svc",
        "env": [{"name": k, "value": v} for k, v in meta.items()],
        "extension": {"exec_vnode": f"({HOST}:ncpus=288:ngpus=4)"},
        "startTime": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
        "status": {"state": state},
        "resourcesRequested": {
            "jobPlacementSharing": 2,
            "taskCount": {"min": 0, "max": 1},
            "tasksResources": [
                {"index": "0", "candidateMachineName": HOST, "slots": 288, "gpus": 4}
            ],
            "jobResources": {"wallClockTime": 604800},
        },
        "allocatedMachines": [
            {
                "name": HOST,
                "resourcesAvail": {
                    "slots": 288,
                    "gpus": 4,
                    "customResources": [{"name": "host", "value": HOST}],
                },
                "resourcesAssigned": {"slots": 288, "gpus": 4},
            }
        ],
    }


def _payload(
    rows: list[dict[str, Any]], *, next_page: bool = False, cursor: str | None = None
) -> dict[str, Any]:
    return {
        "data": {
            "jobs": {
                "edges": [{"node": row, "error": None} for row in rows],
                "pageInfo": {"hasNextPage": next_page, "endCursor": cursor},
            }
        }
    }


async def _statuses(rows: list[dict[str, Any]]) -> list[Any]:
    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        assert body["variables"]["owner"] == "openinference_svc"
        return httpx.Response(200, json=_payload(rows))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        return await GraphQLPBSAdapter(
            client, "openinference_svc", "https://bridge"
        ).get_quarantine_statuses()


@pytest.mark.parametrize("state", [0, 1, 2, 3, 5, 14])
async def test_pending_request_does_not_claim_actual_isolation(state: int) -> None:
    statuses = await _statuses([_row(state=state)])
    assert len(statuses) == 1
    assert not statuses[0].isolated
    assert statuses[0].state in {SchedulerJobState.queued, SchedulerJobState.starting}


async def test_running_trusts_accepted_pbs_request_and_intended_placement() -> None:
    row = _row(state=7)
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content))
        return httpx.Response(200, json=_payload([row]))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        statuses = await GraphQLPBSAdapter(
            client, "openinference_svc", "https://bridge"
        ).get_quarantine_statuses()
    assert statuses[0].isolated
    assert len(requests) == 1
    assert requests[0]["variables"]["owner"] == "openinference_svc"
    assert "resourcesAssigned" not in requests[0]["query"]


async def test_running_does_not_rebuild_a_resource_assignment_census() -> None:
    row = _row(state=7)
    row["allocatedMachines"] = [{"name": HOST}]
    assert (await _statuses([row]))[0].isolated


async def test_exec_vnode_is_the_single_placement_authority() -> None:
    row = _row(state=7)
    row["allocatedMachines"] = []
    assert (await _statuses([row]))[0].isolated


@pytest.mark.parametrize(
    "change",
    [
        "wrong_execution",
        "duplicate_vnode",
        "missing_execution",
        "malformed_execution",
    ],
)
async def test_missing_or_wrong_running_placement_is_unverified(change: str) -> None:
    row = _row(state=7)
    if change == "wrong_execution":
        row["extension"]["exec_vnode"] = "(x4819c0s4b0n0:ncpus=288:ngpus=4)"
    elif change == "duplicate_vnode":
        row["extension"]["exec_vnode"] = (
            f"({HOST}:ncpus=144:ngpus=2)+({HOST}:ncpus=144:ngpus=2)"
        )
    elif change == "missing_execution":
        row["extension"] = {}
    elif change == "malformed_execution":
        row["extension"] = "not a scheduler record"
    assert not (await _statuses([row]))[0].isolated


@pytest.mark.parametrize(
    "change",
    [
        "missing_metadata",
        "wrong_target",
        "shared",
        "wrong_count",
        "duplicate_metadata",
    ],
)
async def test_owned_discovery_requires_attribution_and_exact_request(
    change: str,
) -> None:
    row = _row()
    if change == "missing_metadata":
        row["env"] = []
    elif change == "wrong_target":
        row["resourcesRequested"]["tasksResources"][0]["candidateMachineName"] = (
            "x4819c0s4b0n0"
        )
    elif change == "shared":
        row["resourcesRequested"]["jobPlacementSharing"] = 3
    elif change == "wrong_count":
        row["resourcesRequested"]["taskCount"]["max"] = 2
    elif change == "duplicate_metadata":
        row["env"].append(copy.deepcopy(row["env"][0]))
    with pytest.raises(RuntimeError):
        await _statuses([row])


async def test_foreign_jobs_ignored_and_both_service_environments_validated() -> None:
    unrelated = _row()
    unrelated["owner"] = "other_team"
    unrelated["env"] = []
    assert await _statuses([unrelated]) == []
    statuses = await _statuses([_row(environment="prod")])
    assert statuses[0].request.environment == "prod"


async def test_display_name_is_discovery_not_identity_authority() -> None:
    for name in ["first_quar_dev_truncated", "renamed-for-operations"]:
        row = _row()
        row["name"] = name
        assert (await _statuses([row]))[0].request.host == HOST


async def test_diagnostics_are_not_quarantine_authority() -> None:
    row = _row()
    row["env"] = [
        pair
        for pair in row["env"]
        if pair["name"] in {"FIRST_QUAR_HOST", "FIRST_QUAR_OP"}
    ]
    status = (await _statuses([row]))[0]
    assert status.request.host == HOST and status.request.source is None


async def test_quarantine_lookup_failure_remains_an_infrastructure_error() -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(
                200, json={"errors": [{"message": "denied"}]}
            )
        )
    ) as client:
        with pytest.raises(RuntimeError, match="GraphQL query failed"):
            await GraphQLPBSAdapter(
                client, "openinference_svc", "https://bridge"
            ).get_quarantine_statuses()


async def test_pbs_running_state_not_calculated_expiry_confirms_isolation() -> None:
    row = _row(state=7)
    row["startTime"] = 1_700_000_000_000_000
    status = (await _statuses([row]))[0]
    assert status.isolated and status.expires_at < datetime.now(timezone.utc)
    row["startTime"] = None
    status = (await _statuses([row]))[0]
    assert status.isolated and status.expires_at is None


async def test_ordinary_cleanup_does_not_delete_an_absent_or_unowned_job() -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(json.loads(request.content))
        return httpx.Response(200, json=_payload([]))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await GraphQLPBSAdapter(
            client, "openinference_svc", "https://bridge"
        ).terminate_job(JOB)
    assert len(calls) == 1 and "mutation" not in calls[0]["query"]


@pytest.mark.parametrize("metadata_only", [False, True])
async def test_ordinary_cleanup_protects_both_environments_and_malformed_requests(
    metadata_only: bool,
) -> None:
    row = _row(environment="prod")
    if metadata_only:
        row["name"] = "renamed-by-administrator"
    else:
        row["env"] = []  # Unknown ownership metadata must not permit cleanup.
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content))
        return httpx.Response(200, json=_payload([row]))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = GraphQLPBSAdapter(client, "openinference_svc", "https://bridge")
        with pytest.raises(RuntimeError, match="cannot delete a quarantine"):
            await adapter.terminate_job(JOB)
    assert len(requests) == 1
    assert "mutation" not in requests[0]["query"]


async def test_normal_cleanup_retains_normal_nonforced_deletion() -> None:
    row = _row()
    row.update(name="first_dev_tara_v2_model", env=[])
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        calls.append(body)
        if "mutation DeleteJob" in body["query"]:
            return httpx.Response(
                200,
                json={"data": {"deleteJob": {"node": {"jobId": JOB}, "error": None}}},
            )
        return httpx.Response(200, json=_payload([row]))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await GraphQLPBSAdapter(
            client, "openinference_svc", "https://bridge"
        ).terminate_job(JOB)
    assert len(calls) == 2
    assert "force: false" in calls[1]["query"]


async def test_cleanup_rejects_foreign_identity_returned_by_owned_lookup() -> None:
    row = _row()
    row.update(name="unrelated", env=[], owner="other_team")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, json=_payload([row]))
        )
    ) as client:
        with pytest.raises(RuntimeError, match="ownership is unverified"):
            await GraphQLPBSAdapter(
                client, "openinference_svc", "https://bridge"
            ).terminate_job(JOB)


async def _observations(jobs: list[Any]) -> list[Any]:
    session = MagicMock()
    session.execute = AsyncMock(
        return_value=MagicMock(
            all=MagicMock(
                return_value=[SimpleNamespace(uid=1, name="Tara", pilot_system={})]
            )
        )
    )
    state = MagicMock()
    state.db_sessionmaker.return_value.__aenter__ = AsyncMock(return_value=session)
    state.db_sessionmaker.return_value.__aexit__ = AsyncMock(return_value=None)
    adapter = SimpleNamespace(get_quarantine_statuses=AsyncMock(return_value=jobs))
    pilot = SimpleNamespace(node_admission=SimpleNamespace(on_failure="pbs_quarantine"))
    with (
        patch(
            "first_gateway.controllers.workers.health_alerter.checks.PilotConfig.model_validate",
            return_value=pilot,
        ),
        patch(
            "first_gateway.controllers.workers.health_alerter.checks.build_scheduler",
            new=AsyncMock(return_value=adapter),
        ),
    ):
        return await check_quarantined_nodes(state)


async def test_alerts_distinguish_pending_verified_and_unverified_running() -> None:
    pending = (await _observations(await _statuses([_row()])))[0]
    isolated = (await _observations(await _statuses([_row(state=7)])))[0]
    missing = _row(state=7)
    missing["extension"] = {}
    unverified = (await _observations(await _statuses([missing])))[0]
    assert pending.status == "isolation_pending"
    assert "not yet guaranteed" in pending.summary
    assert isolated.status == "isolated"
    assert "PBS-accepted" in isolated.summary
    assert unverified.status == "isolation_unverified"
    for obs in [pending, isolated, unverified]:
        assert (
            JOB in obs.summary
            and SOURCE in obs.summary
            and "/service/dev/evidence/admission.json" in obs.summary
        )


async def test_visible_duplicates_alert_without_mutating_scheduler() -> None:
    second = _row(environment="prod", operation="b" * 32)
    second["jobId"] = SOURCE
    observations = await _observations(await _statuses([_row(), second]))
    assert len(observations) == 1
    assert observations[0].status == "duplicate_requests"
    assert JOB in observations[0].summary and SOURCE in observations[0].summary


@pytest.mark.parametrize("state", [4, 10, 11, 12, 13])
async def test_terminal_request_never_claims_isolation_or_node_recovery(
    state: int,
) -> None:
    assert await _observations(await _statuses([_row(state=state)])) == []


def test_disappearance_clears_alert_without_claiming_hardware_recovery() -> None:
    now = datetime.now(timezone.utc)
    key = f"cluster/1/quarantine/{HOST}"
    owner = "check_quarantined_nodes"
    state = HealthAlertState(
        committed={
            key: CommittedAlert(
                key=key,
                status="isolated",
                summary=f"running job {JOB}, source {SOURCE}",
                display_name=f"Tara {HOST}",
                owner=owner,
                group="Clusters",
            )
        }
    )
    advance(state, [], {owner}, now, timedelta(seconds=1))
    plan = advance(state, [], {owner}, now + timedelta(seconds=2), timedelta(seconds=1))
    assert plan.degradations == []
    assert len(plan.recoveries) == 1
    text = json.dumps(build_alert_blocks(plan.degradations, plan.recoveries, []))
    assert "recovered" not in text and "Recovery" not in text
    assert "Quarantine ended; node is eligible for startup screening" in text
    assert HOST in text
    HealthAlerter._commit(state, plan)
    assert key not in state.committed
    repeated = advance(
        state, [], {owner}, now + timedelta(seconds=4), timedelta(seconds=1)
    )
    assert repeated.degradations == [] and repeated.recoveries == []


def test_lookup_failure_cannot_manufacture_lost_isolation_or_recovery() -> None:
    key = f"cluster/1/quarantine/{HOST}"
    state = HealthAlertState(
        committed={
            key: CommittedAlert(
                key=key, status="isolated", owner="check_quarantined_nodes"
            )
        }
    )
    plan = advance(state, [], set(), datetime.now(timezone.utc), timedelta(0))
    assert plan.degradations == [] and plan.recoveries == []
    assert state.committed[key].status == "isolated"
