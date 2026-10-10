import base64
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal, cast
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from first_common.schema.base_scheduler import JobSubmitPayload
from first_common.schema.resources.runtime import (
    CommittedAlert,
    HealthAlertState,
    StagedTransition,
)
from first_gateway.controllers.workers.health_alerter.quarantine import (
    quarantine_job_disposition,
    quarantine_observations,
    validated_quarantine_snapshot,
    verify_quarantine_history,
)
from first_gateway.controllers.workers.health_alerter.worker import _is_realtime
from first_gateway.controllers.workers.pilot_job_observer import PilotJobObserver
from first_gateway.platforms.schedulers.graphql_pbs import (
    GraphQLPBSAdapter,
    _job_status_from_node,
)

OWNER = "openinference_svc"
INSTANCE = "a" * 32
TAGS = {
    "FIRST_QUARANTINE_VERSION": "1",
    "FIRST_QUARANTINE_KIND": "model",
    "FIRST_QUARANTINE_ENV": "dev",
}


def _node(**overrides: Any) -> dict[str, Any]:
    node = {
        "jobId": "8123.tara",
        "name": "first_dev_tara_v2_pilot-a",
        "owner": OWNER,
        "submitTime": 1_700_000_000_000_000,
        "startTime": None,
        "status": {"state": 7},
        "env": [{"name": name, "value": value} for name, value in TAGS.items()],
        "holdType": "n",
        "queue": {"name": "workq"},
        "accountingId": "inference_service",
        "outputPath": "/service/dev/submit_scripts/pilot-a.log",
        "resourcesRequested": {"jobResources": {"wallClockTime": 600}},
        "allocatedMachines": [],
    }
    node.update(overrides)
    return node


def _jobs(nodes: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "data": {
            "jobs": {
                "edges": [{"node": node, "error": None} for node in nodes],
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            }
        }
    }


def _cancel(source: dict[str, Any]) -> dict[str, Any]:
    tags = {
        **TAGS,
        "FIRST_QUARANTINE_KIND": "cancel",
        "FIRST_QUARANTINE_SOURCE_ID": source["jobId"],
        "FIRST_QUARANTINE_SOURCE_NAME": source["name"],
        "FIRST_QUARANTINE_SOURCE_CTIME": "1700000000",
    }
    return _node(
        jobId="8124.tara",
        name="first_tara_quarantine_c8123",
        holdType="u",
        status={"state": 3},
        env=[{"name": name, "value": value} for name, value in tags.items()],
    )


async def _termination_requests(
    source: dict[str, Any],
    existing: list[dict[str, Any]] | None = None,
    *,
    environment: Literal["dev", "prod"] | None = "dev",
) -> list[dict[str, Any]]:
    requests: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        requests.append(body)
        if "ExactJobState" in body["query"]:
            payload = _jobs([source])
        elif "ActiveJobs" in body["query"]:
            payload = _jobs(existing or [])
        elif "SubmitCancellation" in body["query"]:
            payload = {
                "data": {"createJob": {"node": {"jobId": "8124.tara"}, "error": None}}
            }
        elif "DeleteJob" in body["query"]:
            payload = {
                "data": {
                    "deleteJob": {"node": {"jobId": source["jobId"]}, "error": None}
                }
            }
        else:
            raise AssertionError("unexpected scheduler mutation")
        return httpx.Response(200, json=payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = GraphQLPBSAdapter(
            client, OWNER, "https://bridge", quarantine_environment=environment
        )
        await adapter.terminate_job(source["jobId"])
    return requests


async def test_managed_model_is_atomically_held_with_native_identity_tags() -> None:
    requests: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "data": {"createJob": {"node": {"jobId": "8123.tara"}, "error": None}}
            },
        )

    payload = JobSubmitPayload(
        name="first_dev_tara_v2_pilot-a",
        queue="workq",
        account="inference_service",
        scheduler_flags="-l place=scatter:exclhost:group=tier0",
        num_nodes=2,
        gpus_per_node=4,
        walltime_min=10,
        log_path=Path("/service/dev/job.log"),
        script="#!/bin/sh\nexit 0\n",
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = GraphQLPBSAdapter(
            client, OWNER, "https://bridge", quarantine_environment="dev"
        )
        await adapter.submit_job(payload)
    job_input = requests[0]["variables"]["input"]
    assert job_input["submitAsHold"] is True
    assert job_input["holdType"] == "u"
    assert {pair["name"]: pair["value"] for pair in job_input["env"]} == TAGS
    assert job_input["resourcesRequested"]["tasksResources"] == [
        {"index": "0-1", "gpus": 4}
    ]


@pytest.mark.parametrize(
    "name", ["first_prod_tara_v2_a", "pilot-a", "first_dev_tara_v2_"]
)
async def test_wrong_environment_never_submits(name: str) -> None:
    async with httpx.AsyncClient() as client:
        adapter = GraphQLPBSAdapter(
            client, OWNER, "https://bridge", quarantine_environment="dev"
        )
        payload = JobSubmitPayload(
            name, "workq", "service", "", 1, 4, 10, Path("/job.log"), script="exit 0"
        )
        with pytest.raises(ValueError, match="environment prefix"):
            await adapter.submit_job(payload)


async def test_cancellation_is_a_held_cpu_only_request_not_model_qdel() -> None:
    requests = await _termination_requests(_node())
    assert all("deleteJob" not in request["query"] for request in requests)
    job_input = requests[-1]["variables"]["input"]
    assert job_input["name"] == "first_tara_quarantine_c8123"
    assert job_input["submitAsHold"] is True and job_input["holdType"] == "u"
    assert job_input["resourcesRequested"]["tasksResources"] == [
        {"index": "0", "slots": 1, "gpus": 0}
    ]
    assert (
        base64.urlsafe_b64decode(job_input["scriptContent"]) == b"#!/bin/sh\nexit 64\n"
    )
    assert job_input["queue"] == {"name": "workq"}
    assert job_input["accountingId"] == "inference_service"
    tags = {pair["name"]: pair["value"] for pair in job_input["env"]}
    assert tags["FIRST_QUARANTINE_SOURCE_CTIME"] == "1700000000"
    assert tags["FIRST_QUARANTINE_SOURCE_ID"] == "8123.tara"


async def test_native_submit_host_output_path_is_preserved() -> None:
    source = _node(outputPath="north-asn-01:/service/dev/pilot.log")
    requests = await _termination_requests(source)
    assert (
        requests[-1]["variables"]["input"]["outputPath"]
        == source["outputPath"] + ".quarantine-cancel.log"
    )


async def test_existing_verified_request_is_adopted_without_duplicate_create() -> None:
    source = _node()
    requests = await _termination_requests(source, [_cancel(source)])
    assert len(requests) == 2
    assert all("mutation" not in request["query"] for request in requests)


async def test_lost_create_response_is_adopted_on_next_reconcile_not_blindly_retried() -> (
    None
):
    source = _node()
    created = False
    create_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal created, create_calls
        body = json.loads(request.content)
        assert "deleteJob" not in body["query"]
        if "ExactJobState" in body["query"]:
            payload = _jobs([source])
        elif "ActiveJobs" in body["query"]:
            payload = _jobs([_cancel(source)] if created else [])
        elif "SubmitCancellation" in body["query"]:
            create_calls += 1
            created = True
            raise httpx.ReadError("response lost after PBS accepted the request")
        else:
            raise AssertionError("unexpected scheduler call")
        return httpx.Response(200, json=payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = GraphQLPBSAdapter(
            client, OWNER, "https://bridge", quarantine_environment="dev"
        )
        with pytest.raises(httpx.ReadError):
            await adapter.terminate_job(source["jobId"])
        await adapter.terminate_job(source["jobId"])
    assert create_calls == 1


async def test_definitively_gone_model_needs_no_cancellation_control_job() -> None:
    requests = await _termination_requests(_node(status={"state": 10}))
    assert len(requests) == 1
    assert "mutation" not in requests[0]["query"]


async def test_rolling_activation_allows_normal_stop_of_verified_untagged_running_model() -> (
    None
):
    requests = await _termination_requests(_node(env=[]))
    assert len(requests) == 2
    assert "DeleteJob" in requests[-1]["query"]
    assert "force: false" in requests[-1]["query"]
    assert requests[-1]["variables"] == {"jobId": "8123.tara"}
    assert all("SubmitCancellation" not in request["query"] for request in requests)


async def test_rolling_activation_never_deletes_legacy_queued_jobs() -> None:
    with pytest.raises(RuntimeError, match="legacy queued"):
        await _termination_requests(_node(env=[], status={"state": 0}))


@pytest.mark.parametrize(
    "change",
    [{"owner": "other"}, {"name": "first_prod_tara_v2_a"}, {"name": "unrelated"}],
)
async def test_running_legacy_fallback_still_requires_exact_owner_environment_namespace(
    change: dict[str, Any],
) -> None:
    with pytest.raises(RuntimeError, match="identity"):
        await _termination_requests(_node(env=[], **change))


@pytest.mark.parametrize(
    "change",
    [
        {"owner": "other"},
        {"submitTime": None},
        {"env": [{"name": "FIRST_QUARANTINE_VERSION", "value": "1"}]},
        {"queue": None},
        {"outputPath": "relative.log"},
        {"accountingId": ""},
    ],
)
async def test_cancellation_unverified_source_fails_closed(
    change: dict[str, Any],
) -> None:
    with pytest.raises(RuntimeError, match="identity"):
        await _termination_requests(_node(**change))


@pytest.mark.parametrize(
    "change",
    [{"holdType": "n"}, {"owner": "other"}, {"env": []}, {"status": {"state": 7}}],
)
async def test_unverified_cancel_request_never_replaced(change: dict[str, Any]) -> None:
    source = _node()
    request = _cancel(source)
    request.update(change)
    with pytest.raises(RuntimeError, match="unverified"):
        await _termination_requests(source, [request])


async def test_duplicate_cancel_requests_fail_closed() -> None:
    source = _node()
    first = _cancel(source)
    second = {**first, "jobId": "8125.tara"}
    with pytest.raises(RuntimeError, match="ambiguous"):
        await _termination_requests(source, [first, second])


@pytest.mark.parametrize("environment", [None, "dev"])
async def test_ordinary_cleanup_cannot_delete_quarantine(
    environment: Literal["dev", "prod"] | None,
) -> None:
    with pytest.raises(RuntimeError, match="cannot delete quarantine"):
        await _termination_requests(
            _node(name="first_tara_quarantine_x4820c0s0b0n0"), environment=environment
        )


def _snapshot(status: str = "pending") -> dict[str, Any]:
    return {
        "schema_version": 2,
        "instance": INSTANCE,
        "generation": 1,
        "parent_generation": 0,
        "previous_job_id": None,
        "updated_unix": 1700000100,
        "notification_environment": "dev",
        "gate_closed": status in {"pending", "failed", "lost"},
        "records": [
            {
                "host": "x4820c0s0b0n0",
                "origin_job": "8123.tara",
                "environment": "dev",
                "code": "missing_gpu",
                "reason": "missing_gpu",
                "evidence": "/service/evidence/node.json",
                "quarantine_job_id": "8124.tara" if status == "isolated" else None,
                "expires_unix": 1700000600 if status == "isolated" else None,
                "status": status,
            }
        ],
    }


def _registry(
    snapshot: dict[str, Any], job_id: str = "8126.tara", submit_time: int = 1700000100
) -> Any:
    return _job_status_from_node(
        _node(
            jobId=job_id,
            submitTime=submit_time * 1_000_000,
            name=f"first_tara_quarantine_s{snapshot['instance']}_{snapshot['generation']}",
            status={"state": 3},
            holdType="u",
            env=[
                {"name": "FIRST_QUARANTINE_VERSION", "value": "2"},
                {"name": "FIRST_QUARANTINE_KIND", "value": "status"},
                {"name": "FIRST_QUARANTINE_INSTANCE", "value": snapshot["instance"]},
                {
                    "name": "FIRST_QUARANTINE_GENERATION",
                    "value": str(snapshot["generation"]),
                },
                {
                    "name": "FIRST_QUARANTINE_STATUS",
                    "value": base64.urlsafe_b64encode(
                        json.dumps(snapshot).encode()
                    ).decode(),
                },
            ],
        )
    )


@pytest.mark.parametrize(
    "status,severity",
    [
        ("pending", "warn"),
        ("isolated", "info"),
        ("failed", "crit"),
        ("lost", "crit"),
        ("released", "info"),
    ],
)
def test_registry_alerts_use_stable_node_identity_and_explicit_state(
    status: str, severity: str
) -> None:
    observations = quarantine_observations(
        [_registry(_snapshot(status))], "dev", 1700000101
    )
    node = observations[0]
    assert node.key == "tara/quarantine/x4820c0s0b0n0"
    assert node.status == status and node.severity == severity
    assert "8123.tara" in node.summary and "/service/evidence/node.json" in node.summary
    assert (
        quarantine_observations([_registry(_snapshot(status))], "prod", 1700000101)
        == []
    )


def test_registry_missing_duplicate_stale_or_invalid_never_reports_recovery() -> None:
    registry = _registry(_snapshot())
    for jobs, now in [
        ([], 1700000101),
        ([registry, registry], 1700000101),
        ([registry], 1700001000),
    ]:
        with pytest.raises(RuntimeError):
            quarantine_observations(jobs, "dev", now)
    registry.coordination_env["FIRST_QUARANTINE_STATUS"] = "not-json"
    with pytest.raises(RuntimeError, match="invalid"):
        quarantine_observations([registry], "dev", 1700000101)


def _next_status(
    previous: dict[str, Any],
    previous_id: str,
    status: str,
    generation: int | None = None,
) -> dict[str, Any]:
    snapshot = _snapshot(status)
    snapshot.update(
        generation=generation or previous["generation"] + 1,
        parent_generation=previous["generation"],
        previous_job_id=previous_id,
        updated_unix=previous["updated_unix"] + 1,
    )
    return snapshot


def test_immutable_status_selects_newest_not_listing_order_and_allows_publish_overlap() -> (
    None
):
    first = _snapshot("pending")
    second = _next_status(first, "8126.tara", "isolated")
    third = _next_status(second, "8127.tara", "lost")
    jobs = [
        _registry(third, "8128.tara", 1700000102),
        _registry(first),
        _registry(second, "8127.tara", 1700000101),
    ]
    assert validated_quarantine_snapshot(jobs, 1700000103).generation == 3
    assert quarantine_observations(jobs, "dev", 1700000103)[0].status == "lost"
    # Deliberately pruned old predecessors do not require all history in PBS.
    assert validated_quarantine_snapshot([jobs[0], jobs[2]], 1700000103).generation == 3


def test_explicitly_recovered_publish_gap_keeps_verified_predecessor() -> None:
    first = _snapshot()
    recovered = _next_status(first, "8126.tara", "isolated", generation=3)
    jobs = [_registry(first), _registry(recovered, "8128.tara", 1700000101)]
    assert validated_quarantine_snapshot(jobs, 1700000102).generation == 3


def test_status_recovery_pair_keeps_deleted_durable_predecessor_lineage() -> None:
    removed = _snapshot("failed")
    removed.update(generation=8, parent_generation=7, previous_job_id="8110.tara")
    first = _next_status(removed, "8126.tara", "failed")
    second = _next_status(first, "8127.tara", "failed")
    jobs = [
        _registry(first, "8127.tara", 1700000101),
        _registry(second, "8128.tara", 1700000102),
    ]
    snapshot = validated_quarantine_snapshot(jobs, 1700000103)
    assert snapshot.generation == 10 and snapshot.gate_closed
    assert snapshot.records[0].status == "failed"


@pytest.mark.parametrize(
    "change",
    [
        {"instance": "b" * 32},
        {"parent_generation": 0},
        {"previous_job_id": "9999.tara"},
        {"updated_unix": 1700000099},
    ],
)
def test_status_lineage_ambiguity_and_timestamp_rollback_fail_closed(
    change: dict[str, Any],
) -> None:
    first = _snapshot()
    second = _next_status(first, "8126.tara", "isolated")
    second.update(change)
    with pytest.raises(RuntimeError):
        validated_quarantine_snapshot(
            [_registry(first), _registry(second, "8127.tara", 1700000101)], 1700000102
        )


def test_missing_latest_predecessor_duplicate_generation_and_native_order_fail_closed() -> (
    None
):
    first = _snapshot()
    second = _next_status(first, "8126.tara", "isolated")
    for jobs in [
        [_registry(second)],
        [_registry(first), _registry(first, "8127.tara")],
        [_registry(first), _registry(second, "8127.tara", 1700000099)],
    ]:
        with pytest.raises(RuntimeError):
            validated_quarantine_snapshot(jobs, 1700000102)


@pytest.mark.parametrize(
    "change",
    [
        {"owner": "other"},
        {"state": "running"},
        {"hold_type": "n"},
        {"submit_time_epoch_s": None},
        {"name": "first_tara_quarantine_registry"},
        {"name": f"first_tara_quarantine_s{INSTANCE}_2"},
    ],
)
def test_invalid_status_jobs_are_not_ignored_beside_valid_latest(
    change: dict[str, Any],
) -> None:
    bad = _registry(_snapshot(), "8127.tara")
    for field, value in change.items():
        setattr(bad, field, value)
    with pytest.raises(RuntimeError):
        validated_quarantine_snapshot([_registry(_snapshot()), bad], 1700000101)


def test_status_tags_payload_and_name_must_agree() -> None:
    for tag, value in [
        ("FIRST_QUARANTINE_VERSION", "1"),
        ("FIRST_QUARANTINE_INSTANCE", "b" * 32),
        ("FIRST_QUARANTINE_GENERATION", "2"),
        ("FIRST_QUARANTINE_KIND", "registry"),
    ]:
        registry = _registry(_snapshot())
        registry.coordination_env[tag] = value
        with pytest.raises(RuntimeError):
            validated_quarantine_snapshot([registry], 1700000101)


def test_unknown_status_name_with_status_tag_is_not_silently_ignored() -> None:
    registry = _registry(_snapshot())
    registry.name = "unknown-held-status"
    with pytest.raises(RuntimeError):
        validated_quarantine_snapshot([registry], 1700000101)


def test_unknown_snapshot_tags_are_a_protocol_error_not_ignored_metadata() -> None:
    registry = _registry(_snapshot())
    registry.coordination_env["FIRST_QUARANTINE_UNKNOWN"] = "unexpected"
    with pytest.raises(RuntimeError, match="identity"):
        validated_quarantine_snapshot([registry], 1700000101)


def test_stale_predecessor_is_allowed_but_latest_heartbeat_must_be_fresh() -> None:
    first = _snapshot()
    second = _next_status(first, "8126.tara", "lost")
    second["updated_unix"] = 1700000400
    jobs = [_registry(first), _registry(second, "8127.tara", 1700000400)]
    assert validated_quarantine_snapshot(jobs, 1700000401).generation == 2


def test_duplicate_host_in_retained_old_snapshot_is_not_ignored() -> None:
    first = _snapshot()
    second = _next_status(first, "8126.tara", "isolated")
    first["records"] *= 2
    with pytest.raises(RuntimeError, match="duplicate physical"):
        validated_quarantine_snapshot(
            [_registry(first), _registry(second, "8127.tara", 1700000101)], 1700000102
        )


@pytest.mark.parametrize(
    "payload",
    [
        b'{"schema_version":1,"schema_version":2}',
        b'{"records":[{"status":"isolated","status":"released"}]}',
    ],
)
def test_registry_duplicate_json_keys_are_rejected_not_last_value_wins(
    payload: bytes,
) -> None:
    registry = _registry(_snapshot())
    registry.coordination_env["FIRST_QUARANTINE_STATUS"] = base64.urlsafe_b64encode(
        payload
    ).decode()
    with pytest.raises(RuntimeError, match="invalid"):
        validated_quarantine_snapshot([registry], 1700000101)


def test_isolated_expired_missing_identity_and_duplicate_hosts_fail_closed() -> None:
    for change in [{"expires_unix": 1700000100}, {"quarantine_job_id": None}]:
        snapshot = _snapshot("isolated")
        snapshot["records"][0].update(change)
        with pytest.raises(RuntimeError, match="unverified"):
            quarantine_observations([_registry(snapshot)], "dev", 1700000101)
    snapshot = _snapshot()
    snapshot["records"] *= 2
    with pytest.raises(RuntimeError, match="duplicate"):
        quarantine_observations([_registry(snapshot)], "dev", 1700000101)


def test_status_parser_never_copies_unrelated_native_environment() -> None:
    node = _node()
    node["env"].append({"name": "SECRET_TOKEN", "value": "must-not-copy"})
    assert _job_status_from_node(node).coordination_env == TAGS


@pytest.mark.parametrize(
    "owner",
    [
        OWNER,
        OWNER + "@north-asn-01.head.north.tara.alcf.anl.gov",
        OWNER + "@north-asn-01",
    ],
)
def test_status_parser_normalizes_verified_native_owner_username(owner: str) -> None:
    assert _job_status_from_node(_node(owner=owner)).owner == OWNER


@pytest.mark.parametrize(
    "owner",
    [
        "",
        "openinference_svc@",
        "@host",
        "openinference_svc@@host",
        "openinference_svc@host@other",
        "openinference_svc@host/other",
        "openinference_svc@host\n",
        "openinference_svc@-host",
        "openinference_svc@host-",
        "openinference_svc@host..example",
        "openinference_svc@" + "a" * 64,
        {"owner": OWNER},
    ],
)
def test_malformed_native_owner_never_becomes_service_identity(owner: Any) -> None:
    with pytest.raises(RuntimeError, match="owner"):
        _job_status_from_node(_node(owner=owner))


async def test_native_user_at_submit_host_owner_preserves_cancellation_verification() -> (
    None
):
    source = _node(owner=OWNER + "@north-asn-01.head.north.tara.alcf.anl.gov")
    request = _cancel(source)
    requests = await _termination_requests(source, [request])
    assert len(requests) == 2
    assert all("mutation" not in request["query"] for request in requests)


def test_null_hold_type_is_unverified_not_inferred_from_held_status() -> None:
    registry = _registry(_snapshot())
    registry.hold_type = None
    with pytest.raises(RuntimeError, match="user hold"):
        validated_quarantine_snapshot([registry], 1700000101)


def test_live_bridge_native_owner_and_hold_extension_shape_is_verified() -> None:
    registry = _registry(_snapshot())
    parsed = _job_status_from_node(
        _node(
            jobId=registry.id,
            name=registry.name,
            owner=OWNER + "@north-asn-01.head.north.tara.alcf.anl.gov",
            status={"state": 3},
            holdType=None,
            env=[
                {"name": name, "value": value}
                for name, value in registry.coordination_env.items()
            ],
            extension={
                "job_state": "H",
                "Hold_Types": "u",
                "Checkpoint": "u",
                "Submit_arguments": "must-not-retain-this-private-submission-data",
            },
        )
    )
    assert parsed.owner == OWNER and parsed.hold_type == "u"
    assert validated_quarantine_snapshot([parsed], 1700000101).generation == 1
    assert "must-not-retain" not in str(vars(parsed))


@pytest.mark.parametrize(
    "typed,extension,state",
    [
        ("u", {"Hold_Types": "n", "job_state": "H"}, 3),
        ("n", {"Hold_Types": "u", "job_state": "H"}, 3),
        (None, {"Hold_Types": "u", "job_state": "R"}, 3),
        (None, {"Hold_Types": "u", "job_state": "Q"}, 0),
        (None, {"Hold_Types": "u"}, 3),
        (None, {"Hold_Types": ["u"], "job_state": "H"}, 3),
        (None, {"Hold_Types": "uu", "job_state": "H"}, 3),
        (None, {"Hold_Types": "user", "job_state": "H"}, 3),
        (None, {"Hold_Types": "u", "job_state": ["H"]}, 3),
        (None, {"Hold_Types": "u", "job_state": "UNKNOWN"}, 3),
        ("u", {"job_state": "R"}, 3),
    ],
)
def test_native_hold_fallback_rejects_contradictory_or_malformed_proof(
    typed: Any, extension: dict[str, Any], state: int
) -> None:
    with pytest.raises(RuntimeError, match="hold"):
        _job_status_from_node(
            _node(holdType=typed, extension=extension, status={"state": state})
        )


@pytest.mark.parametrize("extension", [None, {}, {"job_state": "H"}])
def test_native_held_state_without_hold_types_never_implies_user_hold(
    extension: Any,
) -> None:
    parsed = _job_status_from_node(
        _node(holdType=None, extension=extension, status={"state": 3})
    )
    assert parsed.hold_type is None


@pytest.mark.parametrize("hold", ["o", "s", "us"])
def test_native_other_or_combined_holds_are_not_user_only_snapshot_proof(
    hold: str,
) -> None:
    parsed = _job_status_from_node(
        _node(
            holdType=None,
            extension={"Hold_Types": hold, "job_state": "H"},
            status={"state": 3},
        )
    )
    registry = _registry(_snapshot())
    registry.hold_type = parsed.hold_type
    with pytest.raises(RuntimeError, match="user hold"):
        validated_quarantine_snapshot([registry], 1700000101)


def test_native_running_user_hold_race_does_not_hide_running_allocation() -> None:
    parsed = _job_status_from_node(
        _node(
            holdType=None,
            extension={"Hold_Types": "u", "job_state": "R"},
            status={"state": 7},
        )
    )
    assert parsed.state.value == "running" and parsed.hold_type == "u"


def test_omitting_unresolved_records_is_not_recovery_but_released_can_be_archived() -> (
    None
):
    key = "tara/quarantine/x4820c0s0b0n0"
    alert = CommittedAlert(key=key, status="isolated", owner="check_tara_quarantine")
    state = HealthAlertState(committed={key: alert})
    with pytest.raises(RuntimeError, match="no recovery inferred"):
        verify_quarantine_history(state, [])
    observations = quarantine_observations(
        [_registry(_snapshot("released"))], "dev", 1700000101
    )
    verify_quarantine_history(state, observations)
    state.committed[key].status = "released"
    verify_quarantine_history(state, [])


def test_invalid_quarantine_configuration_is_rejected() -> None:
    client = cast(httpx.AsyncClient, None)
    with pytest.raises(ValueError):
        GraphQLPBSAdapter(
            client, OWNER, "https://bridge", quarantine_environment=cast(Any, "bad")
        )
    with pytest.raises(ValueError, match="service PBS owner"):
        GraphQLPBSAdapter(
            client, "other", "https://bridge", quarantine_environment="dev"
        )


def test_quarantine_confirmations_and_releases_alert_without_broadening_info_policy() -> (
    None
):
    now = datetime(2026, 10, 9, tzinfo=timezone.utc)
    transition = StagedTransition(
        key="tara/quarantine/x4820c0s0b0n0",
        status="isolated",
        severity="info",
        owner="check_tara_quarantine",
        first_seen=now,
    )
    assert _is_realtime(transition)
    transition.status = "released"
    assert _is_realtime(transition)
    transition.owner = "check_pilot_job"
    assert not _is_realtime(transition)


@pytest.mark.parametrize(
    "status,expected",
    [
        ("pending", "blocked"),
        ("failed", "blocked"),
        ("lost", "blocked"),
        ("isolated", "isolated"),
        ("released", "ordinary"),
    ],
)
def test_only_validated_typed_node_faults_change_failure_accounting(
    status: str, expected: str
) -> None:
    snapshot = validated_quarantine_snapshot([_registry(_snapshot(status))], 1700000101)
    assert quarantine_job_disposition(snapshot, "8123.tara") == expected
    # Model OOM/auth/config failures with no attributed node evidence retain
    # FIRST's ordinary bounded launch-failure accounting.
    assert quarantine_job_disposition(snapshot, "9999.tara") == "ordinary"
    assert quarantine_job_disposition(None, "8123.tara") == "ordinary"


def test_partial_isolation_blocks_origin_and_unknown_fault_code_is_rejected() -> None:
    snapshot = _snapshot("isolated")
    second = {**snapshot["records"][0], "host": "x4820c0s0b1n0", "status": "pending"}
    snapshot["records"].append(second)
    verified = validated_quarantine_snapshot([_registry(snapshot)], 1700000101)
    assert quarantine_job_disposition(verified, "8123.tara") == "blocked"
    snapshot["records"][0]["code"] = "model_oom"
    with pytest.raises(RuntimeError, match="invalid"):
        validated_quarantine_snapshot([_registry(snapshot)], 1700000101)


@pytest.mark.parametrize(
    "disposition,state,charges",
    [
        ("ordinary", "gone", 1),
        ("blocked", "running", 0),
        ("isolated", "gone", 0),
    ],
)
async def test_observer_defers_pending_fault_without_resetting_normal_failure_counters(
    disposition: Literal["ordinary", "blocked", "isolated"],
    state: str,
    charges: int,
) -> None:
    current = SimpleNamespace(
        uid=1,
        name="pilot-a",
        scheduler_job_id="8123.tara",
        scheduler_state="running",
        time_started=None,
        manager_url=None,
        scheduled_deletion_at=None,
        deleted_at=None,
    )
    sess = SimpleNamespace(get=AsyncMock(return_value=current))
    context = AsyncMock()
    context.__aenter__.return_value = sess
    observer = PilotJobObserver.__new__(PilotJobObserver)
    observer.client_state = cast(
        Any, SimpleNamespace(db_sessionmaker=SimpleNamespace(begin=lambda: context))
    )
    with patch.object(
        observer, "_record_pre_manager_launch_failure", new_callable=AsyncMock
    ) as charge:
        await observer._update_job(
            cast(Any, current), None, quarantine_disposition=disposition
        )
    assert current.scheduler_state == state
    assert charge.await_count == charges


async def test_intentional_cleanup_is_not_deferred_or_charged_by_node_fault() -> None:
    current = SimpleNamespace(
        uid=1,
        name="pilot-a",
        scheduler_job_id="8123.tara",
        scheduler_state="running",
        time_started=None,
        manager_url=None,
        scheduled_deletion_at=datetime.now(timezone.utc),
        deleted_at=None,
    )
    sess = SimpleNamespace(get=AsyncMock(return_value=current))
    context = AsyncMock()
    context.__aenter__.return_value = sess
    observer = PilotJobObserver.__new__(PilotJobObserver)
    observer.client_state = cast(
        Any, SimpleNamespace(db_sessionmaker=SimpleNamespace(begin=lambda: context))
    )
    with patch.object(
        observer, "_record_pre_manager_launch_failure", new_callable=AsyncMock
    ) as charge:
        await observer._update_job(
            cast(Any, current), None, quarantine_disposition="blocked"
        )
    assert current.scheduler_state == "gone" and current.deleted_at is not None
    charge.assert_not_awaited()
