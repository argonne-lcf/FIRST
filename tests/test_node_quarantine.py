"""Pure/mocked qualification: no scheduler, service jobs, or live DB writes."""

import copy
import hashlib
import json
import time
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import sqlalchemy as sa
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy.dialects import postgresql

from first_common.schema.base_scheduler import SchedulerJobState
from first_common.schema.quarantine import (
    NodeQuarantinePolicy,
    QuarantineActivationPayload,
    QuarantineFaultPayload,
    QuarantineJobIdentity,
    QuarantineJobStatus,
    QuarantineReport,
    QuarantineReportRejected,
    QuarantineScreenPayload,
    QuarantineSource,
    QuarantineSubmissionRejected,
    WorkerIdentity,
)
from first_common.schema.quarantine_read import QuarantineOperatorAction
from first_common.schema.types import PilotConfig
from first_gateway.apiserver.dependencies import get_admin_user
from first_gateway.controllers.control_server import app
from first_gateway.controllers.workers.health_alerter.checks import (
    check_node_quarantine,
)
from first_gateway.database.models import (
    Cluster,
    NodeQuarantine,
    PilotJob,
    QuarantineReportInbox,
)
from first_gateway.platforms.schedulers.graphql_pbs_quarantine import activation_proof
from first_gateway.services.pilot_submitter import PilotSubmitter
from first_gateway.services.quarantine import QuarantineService, ReportPending, isolated
from first_gateway.services.quarantine_evidence import (
    cleanup_proof,
    fault_report_time,
    gpu_fault,
    paired,
    publication,
)
from first_gateway.settings import ClientState

HOST = "x4820c0s4b0n0"
PEER = "x4820c1s0b0n0"
OPERATION = "a" * 32
COMMIT = "b" * 40
NOW_NS = time.time_ns()
SOURCE = QuarantineSource(id="1.pbs", name="first_dev_tara_v2_source", ctime=1700000000)
IDENTITY = QuarantineJobIdentity(
    job_id="2.pbs",
    job_name=f"first_quar_dev_{HOST}_aaaaaaaa",
    ctime=1700000100,
    environment="dev",
    cluster="tara",
    host=HOST,
    operation=OPERATION,
    source=SOURCE,
)
POLICY_JSON = json.dumps(
    {
        "schema_version": 3,
        "kernel": "kernel",
        "page_size": 65536,
        "driver": "driver",
        "allowed_racks": ["x4820"],
        "topology": {
            HOST: {"tier0": "x4820", "tier1": "x4820c0"},
            PEER: {"tier0": "x4820", "tier1": "x4820c1"},
        },
    }
)
POLICY_SHA = hashlib.sha256(POLICY_JSON.encode()).hexdigest()
POLICY = NodeQuarantinePolicy(
    environment="dev",
    worker_path=f"/bundles/{COMMIT}/quarantine-wait",
    worker_sha256="c" * 64,
    admission_path=f"/bundles/{COMMIT}/admission",
    admission_sha256="d" * 64,
    policy_path=f"/bundles/{COMMIT}/policy.json",
    policy_sha256=POLICY_SHA,
)


def native_sample(
    host: str = HOST, *, faulty: bool = True, collected: int = NOW_NS - 4_000_000_000
) -> dict[str, Any]:
    rows = [
        {
            "index": index,
            "uuid": f"GPU-00000000-0000-0000-0000-{index:012x}",
            "total_mib": 100000,
            "free_mib": 90000 if faulty and index == 0 else 99999,
            "uncorrected_volatile": 0,
            "uncorrected_aggregate": 0,
            "driver": "driver",
        }
        for index in range(4)
    ]
    fault = gpu_fault(rows, 4)
    result = {
        "schema_version": 1,
        "attempt": OPERATION,
        "host": host,
        "collected_unix_ns": collected,
        "outcome": "FAIL" if fault else "PASS",
        "hsn_ip": "10.0.0.1" if host == HOST else "10.0.0.2",
        "master_hsn": "10.0.0.1",
        "kernel": "kernel",
        "page_size": 65536,
        "compute_pids": "",
        "gpu_query": "\n".join(
            f"{row['index']}, {row['uuid']}, {row['total_mib']}, {row['free_mib']}, 0, 0, driver"
            for row in rows
        ),
    }
    if fault:
        result["fault"] = fault
    return result


def fault_payload(host: str = HOST) -> QuarantineFaultPayload:
    return QuarantineFaultPayload(
        schema_version=3,
        event="node_fault",
        environment="dev",
        cluster="tara",
        operation=OPERATION,
        origin=SOURCE,
        host=host,
        observed_unix_ns=NOW_NS,
        source="pre_pilot_admission",
        attempt=OPERATION,
        allocation_hosts=[HOST, PEER],
        master_hsn="10.0.0.1",
        bundle_commit=COMMIT,
        policy_sha256=POLICY_SHA,
        policy_json=POLICY_JSON,
        evidence_path="/evidence/attempt",
        reports=[native_sample(host)],
        confirmation=[native_sample(host, collected=NOW_NS - 2_000_000_000)],
    )


def status() -> QuarantineJobStatus:
    return QuarantineJobStatus(
        IDENTITY,
        SchedulerJobState.running,
        (HOST,),
        True,
        int(time.time()) + 600,
        request_verified=True,
    )


def activation() -> QuarantineActivationPayload:
    return QuarantineActivationPayload(
        schema_version=3,
        event="activated",
        environment="dev",
        cluster="tara",
        operation=OPERATION,
        origin=SOURCE,
        host=HOST,
        observed_unix_ns=NOW_NS,
        quarantine_job_id=IDENTITY.job_id,
        worker_identity=WorkerIdentity(
            pid=123, pgid=123, sid=123, starttime=123, uid=1000
        ),
        gpu_initialized=False,
        expires_unix=int(time.time()) + 600,
        native_job={
            "Job_Name": IDENTITY.job_name,
            "Job_Owner": "service@submit",
            "job_state": "R",
            "ctime": IDENTITY.ctime,
            "exec_vnode": f"({HOST}:ncpus=288:ngpus=4)",
            "Resource_List": {
                "select": f"1:host={HOST}:ncpus=288:ngpus=4",
                "place": "exclhost",
            },
        },
        native_nodes={
            HOST: {
                "state": "job-exclusive",
                "jobs": [f"{IDENTITY.job_id}/0"],
                "resources_available": {"host": HOST},
            }
        },
    )


def pilot() -> PilotConfig:
    return PilotConfig.model_validate(
        {
            "scheduler_adapter": "first_gateway.platforms.schedulers.graphql_pbs.GraphQLPBSAdapter",
            "scheduler_config": {},
            "quarantine": POLICY.model_dump(),
            "job_walltime_min": 60,
            "max_num_nodes": 20,
            "gpus_per_node": 4,
            "queue": "workq",
            "account": "inference_service",
            "workdir": "/dev/pilot",
            "external_port": 18443,
            "nginx_path": "/bin/nginx",
            "ip_allowlist": ["10.0.0.0/8"],
            "node_file_env": "PBS_NODEFILE",
            "submit_script_preamble": "#!/bin/sh",
            "pilot_path": "/bundles/pilot",
            "pilot_config_path": "/dev/pilot.yaml",
            "job_name_prefix": "first_dev_tara_v2_",
        }
    )


def row(**overrides: Any) -> NodeQuarantine:
    values: dict[str, Any] = dict(
        uid=1,
        name=f"tara/{HOST}",
        cluster_name="tara",
        host=HOST,
        environment="dev",
        operation=OPERATION,
        origin=SOURCE.model_dump(),
        fault={"code": "missing_gpu", "reason": "test"},
        evidence={"path": "/evidence/attempt", "policy_sha256": POLICY_SHA},
        state="isolated",
        state_message="test",
        faulted_at=datetime.now(timezone.utc),
        transitioned_at=datetime.now(timezone.utc),
        submission_attempts=0,
        submission_started_at=None,
        submission_uncertain=False,
        job_identity=IDENTITY.model_dump(),
        runtime_policy=POLICY.model_dump(),
        activation=activation().model_dump(mode="json"),
        expires_at=None,
        repair_reference=None,
        screen_nonce=None,
        screen_requested_at=None,
        screen_submission_started_at=None,
        screen_result=None,
        release_requested_at=None,
        deletion_started_at=None,
        deletion_acknowledged_at=None,
    )
    values.update(overrides)
    return NodeQuarantine(**values)


def mock_service(node: NodeQuarantine) -> tuple[QuarantineService, Any, Any]:
    session = AsyncMock()
    session.get.return_value = node
    session.execute.return_value = MagicMock(rowcount=1)
    session.scalar.return_value = Cluster(
        uid=1, name="tara", pilot_system=pilot().model_dump(mode="json")
    )
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=session)
    context.__aexit__ = AsyncMock(return_value=None)
    maker = MagicMock(return_value=context)
    maker.begin.return_value = context
    service = QuarantineService(cast(ClientState, MagicMock(db_sessionmaker=maker)))
    adapter = AsyncMock()
    adapter.list_quarantine_jobs.return_value = [status()]
    adapter.get_quarantine_status.return_value = status()
    service.adapter = AsyncMock(return_value=adapter)  # type: ignore[method-assign]
    service._state = AsyncMock()  # type: ignore[method-assign]
    return service, session, adapter


@pytest.mark.parametrize("host", [HOST, PEER])
def test_persistent_fault_in_real_allocation_context(host: str) -> None:
    fault = paired(fault_payload(host), POLICY_SHA, 4, now_ns=NOW_NS)
    assert fault is not None and fault["code"] == "unexplained_gpu_memory_loss"


def test_immutable_fault_survives_delayed_receipt_without_reusing_stale_samples() -> (
    None
):
    payload = fault_payload()
    receipt = NOW_NS + 600_000_000_000
    historical = fault_report_time(
        payload, NOW_NS // 1_000_000_000 + 2, receipt_ns=receipt
    )
    assert historical == payload.observed_unix_ns
    assert paired(payload, POLICY_SHA, 4, now_ns=historical) is not None
    with pytest.raises(ValueError, match="freshness"):
        paired(payload, POLICY_SHA, 4, now_ns=receipt)


@pytest.mark.parametrize(
    "mutation",
    [
        "future-carrier",
        "future-observation",
        "delayed-publication",
        "copied-observation",
        "source-after-report",
        "sample-before-source",
    ],
)
def test_fault_time_binding_rejects_future_copied_or_pre_source_evidence(
    mutation: str,
) -> None:
    payload = fault_payload()
    carrier = NOW_NS // 1_000_000_000 + 2
    receipt = NOW_NS + 600_000_000_000
    if mutation == "future-carrier":
        carrier = receipt // 1_000_000_000 + 10
    elif mutation == "future-observation":
        payload.observed_unix_ns = receipt + 10_000_000_000
    elif mutation == "delayed-publication":
        carrier = NOW_NS // 1_000_000_000 + 121
    elif mutation == "copied-observation":
        payload.observed_unix_ns = receipt
    elif mutation == "source-after-report":
        payload.origin = SOURCE.model_copy(update={"ctime": carrier + 1})
    else:
        payload.origin = SOURCE.model_copy(
            update={"ctime": NOW_NS // 1_000_000_000 - 3}
        )
    with pytest.raises(ValueError):
        historical = fault_report_time(payload, carrier, receipt_ns=receipt)
        paired(payload, POLICY_SHA, 4, now_ns=historical)


async def test_actual_fault_ingestion_records_after_restart_delay_without_model_rpc() -> (
    None
):
    service, session, adapter = mock_service(row())
    session.scalar.side_effect = [
        PilotJob(name="source", scheduler_job_id=SOURCE.id),
        None,
    ]
    session.add = MagicMock()
    adapter.get_source_allocation.return_value = (HOST, PEER)
    report = QuarantineReport(
        "3.pbs", "fault", NOW_NS // 1_000_000_000 + 2, fault_payload(), "1" * 64
    )
    with patch(
        "first_gateway.services.quarantine_evidence.time.time_ns",
        return_value=NOW_NS + 600_000_000_000,
    ):
        await service._fault(Cluster(uid=1, name="tara"), pilot(), adapter, report)
    session.add.assert_called_once()
    recorded = session.add.call_args.args[0]
    assert recorded.host == HOST and recorded.state == "pending"
    assert recorded.fault["code"] == "unexplained_gpu_memory_loss"
    adapter.submit_job.assert_not_awaited()
    adapter.terminate_job.assert_not_awaited()


def test_deliberate_repair_screen_retains_current_clock_freshness_limit() -> None:
    payload = QuarantineScreenPayload(
        schema_version=3,
        event="screen_result",
        environment="dev",
        cluster="tara",
        operation=OPERATION,
        origin=SOURCE,
        host=HOST,
        observed_unix_ns=NOW_NS,
        quarantine_job_id=IDENTITY.job_id,
        nonce=OPERATION,
        outcome="PASS",
        reports=[native_sample(faulty=False)],
        confirmation=[native_sample(faulty=False, collected=NOW_NS - 2_000_000_000)],
        master_hsn="10.0.0.1",
        bundle_commit=COMMIT,
        policy_sha256=POLICY_SHA,
        policy_json=POLICY_JSON,
        error=None,
    )
    assert paired(payload, POLICY_SHA, 4, now_ns=NOW_NS) is None
    with pytest.raises(ValueError, match="freshness"):
        paired(payload, POLICY_SHA, 4, now_ns=NOW_NS + 600_000_000_000)


@pytest.mark.parametrize(
    "mutation",
    [
        "policy",
        "oom",
        "auth",
        "gpu-client",
        "uuid",
        "stale",
        "not-member",
        "rack",
        "bypass",
    ],
)
def test_non_hardware_or_unverified_fault_never_classifies(mutation: str) -> None:
    payload = fault_payload().model_dump(mode="json")
    if mutation == "policy":
        payload["policy_json"] += " "
    elif mutation in {"oom", "auth"}:
        payload["reports"][0]["fault"]["reason"] = mutation
    elif mutation == "gpu-client":
        payload["reports"][0]["compute_pids"] = "1234"
    elif mutation == "uuid":
        payload["confirmation"][0]["gpu_query"] = payload["confirmation"][0][
            "gpu_query"
        ].replace("000000000000", "000000000005")
    elif mutation == "stale":
        payload["reports"][0]["collected_unix_ns"] = NOW_NS - 121_000_000_000
    elif mutation == "not-member":
        payload["allocation_hosts"] = [PEER]
    elif mutation == "rack":
        payload["allocation_hosts"].append("x4818c0s0b0n0")
    else:
        payload["reports"][0]["gpu_headroom_bypassed"] = True
    with pytest.raises((ValueError, ValidationError)):
        paired(
            QuarantineFaultPayload.model_validate(payload), POLICY_SHA, 4, now_ns=NOW_NS
        )


def test_publication_pin_and_passing_screen() -> None:
    assert POLICY.worker_path is not None
    publication(fault_payload(), POLICY.worker_path)
    with pytest.raises(ValueError):
        publication(fault_payload(), f"/bundles/{'f' * 40}/quarantine-wait")
    samples = [
        native_sample(faulty=False, collected=NOW_NS - 4_000_000_000),
        native_sample(faulty=False, collected=NOW_NS - 2_000_000_000),
    ]
    payload = QuarantineScreenPayload(
        schema_version=3,
        event="screen_result",
        environment="dev",
        cluster="tara",
        operation=OPERATION,
        origin=SOURCE,
        host=HOST,
        observed_unix_ns=NOW_NS,
        quarantine_job_id=IDENTITY.job_id,
        nonce=OPERATION,
        outcome="PASS",
        reports=[samples[0]],
        confirmation=[samples[1]],
        master_hsn="10.0.0.1",
        bundle_commit=COMMIT,
        policy_sha256=POLICY_SHA,
        policy_json=POLICY_JSON,
    )
    assert paired(payload, POLICY_SHA, 4, now_ns=NOW_NS) is None


@pytest.mark.parametrize("change", ["queued", "host", "shared", "no-worker", "expired"])
def test_submission_is_not_isolation(change: str) -> None:
    allocation = status()
    proof: dict[str, Any] | None = activation().model_dump(mode="json")
    if change == "queued":
        allocation = replace(allocation, state=SchedulerJobState.queued)
    elif change == "host":
        allocation = replace(allocation, allocated_hosts=(PEER,))
    elif change == "shared":
        allocation = replace(allocation, exclusive=False)
    elif change == "expired":
        allocation = replace(allocation, expires_unix=int(time.time()) - 1)
    else:
        proof = None
    assert not isolated(allocation, proof)


@pytest.mark.parametrize(
    "change", ["competing", "shared", "wrong-host", "wrong-ctime", "multi-vnode-excl"]
)
def test_native_activation_requires_whole_physical_host(change: str) -> None:
    proof = activation().model_dump(mode="json")
    if change == "competing":
        proof["native_nodes"][HOST]["jobs"].append("other.pbs/1")
    elif change == "shared":
        proof["native_nodes"][HOST]["state"] = "free"
    elif change == "wrong-host":
        proof["native_nodes"][HOST]["resources_available"]["host"] = PEER
    elif change == "wrong-ctime":
        proof["native_job"]["ctime"] += 1
    else:
        proof["native_job"]["Resource_List"]["place"] = "excl"
        proof["native_nodes"][HOST + "-vnode"] = copy.deepcopy(
            proof["native_nodes"][HOST]
        )
    with pytest.raises(ValueError):
        activation_proof(QuarantineActivationPayload.model_validate(proof), status())


def test_measured_tier0_group_activation_requires_one_physical_vnode() -> None:
    proof = activation().model_dump(mode="json")
    proof["native_job"]["Resource_List"]["place"] = "scatter:excl:group=tier0"
    activation_proof(QuarantineActivationPayload.model_validate(proof), status())
    proof["native_nodes"][HOST + "-vnode"] = copy.deepcopy(proof["native_nodes"][HOST])
    with pytest.raises(ValueError, match="whole-host"):
        activation_proof(QuarantineActivationPayload.model_validate(proof), status())


@pytest.mark.parametrize(
    "selector",
    [
        f"1:host={HOST}:host={HOST}:ncpus=288",
        f"1:host={HOST}:host={PEER}:ncpus=288",
        f"2:host={HOST}:ncpus=288",
        f"1:host={HOST}:ncpus=288+1:host={PEER}:ncpus=288",
        "1:host=x4820*:ncpus=288",
    ],
)
def test_native_activation_requires_one_exact_nonduplicated_selector(
    selector: str,
) -> None:
    proof = activation().model_dump(mode="json")
    proof["native_job"]["Resource_List"]["select"] = selector
    with pytest.raises(ValueError, match="exact-host"):
        activation_proof(QuarantineActivationPayload.model_validate(proof), status())


def test_unknown_placement_group_and_missing_pbs_evidence_are_rejected() -> None:
    proof = activation().model_dump(mode="json")
    proof["native_job"]["Resource_List"]["place"] = "scatter:excl:group=unknown"
    with pytest.raises(ValueError, match="exact-host"):
        activation_proof(QuarantineActivationPayload.model_validate(proof), status())
    # Common protocol does not force future scheduler adapters to fake PBS.
    proof.pop("native_job")
    proof.pop("native_nodes")
    generic = QuarantineActivationPayload.model_validate(proof)
    assert generic.native_job is None and generic.native_nodes is None
    with pytest.raises(ValueError, match="PBS activation requires"):
        activation_proof(generic, status())


async def test_activation_verification_is_delegated_to_optional_adapter() -> None:
    service, session, adapter = mock_service(row())
    session.scalar.return_value = row()
    current = status()
    adapter.get_quarantine_status.return_value = current
    proof = activation().model_copy(update={"expires_unix": current.expires_unix})
    report = QuarantineReport(
        "activation.pbs", "activation", SOURCE.ctime, proof, "1" * 64
    )
    await service._worker_report(Cluster(uid=1, name="tara"), pilot(), adapter, report)
    adapter.verify_quarantine_activation.assert_awaited_once_with(proof, current)
    adapter.submit_job.assert_not_awaited()
    adapter.terminate_job.assert_not_awaited()


async def test_unverified_live_isolation_defers_activation_without_rejecting_carrier() -> (
    None
):
    service, session, adapter = mock_service(row())
    session.scalar.return_value = row()
    current = replace(status(), exclusive=False)
    adapter.get_quarantine_status.return_value = current
    proof = activation().model_copy(update={"expires_unix": current.expires_unix})
    report = QuarantineReport(
        "activation.pbs", "activation", SOURCE.ctime, proof, "1" * 64
    )
    with pytest.raises(ReportPending, match="independent exact-host"):
        await service._worker_report(
            Cluster(uid=1, name="tara"), pilot(), adapter, report
        )
    adapter.verify_quarantine_activation.assert_not_awaited()
    session.execute.assert_not_awaited()


def test_native_activation_and_all_node_cleanup() -> None:
    activation_proof(activation(), status())
    markers = [
        "tara_model_postcheck_pals_group_query_valid=true rank=0 replicas=1",
        "tara_model_postcheck_uds_listener_query_valid=true rank=0 replicas=1 absent=true",
    ]
    for rank in range(2):
        markers += [
            f"tara_model_postcheck_engine_group_query_valid=true rank={rank} replicas=1",
            f"tara_model_postcheck_listener_query_valid=true rank={rank} replicas=1 ports_per_replica=2 absent=true",
            f"tara_model_postcheck_gpu_query_valid=true rank={rank} unique_pids=0",
        ]
    cleanup_proof("\n".join(markers), [HOST, PEER])
    for text in (
        "\n".join(markers[:-1]),
        "\n".join(markers + [markers[-1]]),
        "\n".join(markers) + "\ntara_model_postcheck_engine_remaining pid=2",
    ):
        with pytest.raises(ValueError):
            cleanup_proof(text, [HOST, PEER])


async def test_failed_deletion_then_expiration_is_lost_not_repaired() -> None:
    service, _, adapter = mock_service(
        row(deletion_started_at=datetime.now(timezone.utc))
    )
    adapter.get_quarantine_status.return_value = None
    adapter.list_quarantine_jobs.return_value = []
    await service.reconcile(1)
    cast(AsyncMock, service._state).assert_awaited_once_with(
        1, "lost", "Quarantine job disappeared; no node recovery is inferred."
    )
    adapter.submit_quarantine.assert_not_awaited()
    adapter.delete_quarantine.assert_not_awaited()


async def test_verified_delete_ack_and_disappearance_is_explicit_release() -> None:
    service, _, adapter = mock_service(
        row(
            deletion_started_at=datetime.now(timezone.utc),
            deletion_acknowledged_at=datetime.now(timezone.utc),
        )
    )
    adapter.get_quarantine_status.return_value = None
    adapter.list_quarantine_jobs.return_value = []
    await service.reconcile(1)
    call = cast(AsyncMock, service._state).await_args
    assert call is not None and call.args[1] == "released"


async def test_duplicate_quarantines_alert_without_cancel_or_resubmit() -> None:
    service, _, adapter = mock_service(row())
    adapter.list_quarantine_jobs.return_value = [status(), status()]
    await service.reconcile(1)
    call = cast(AsyncMock, service._state).await_args
    assert call is not None and call.args[1] == "failed"
    adapter.submit_quarantine.assert_not_awaited()
    adapter.delete_quarantine.assert_not_awaited()


async def test_uncertain_submission_and_retry_exhaustion_are_bounded() -> None:
    node = row(submission_uncertain=True, job_identity=None)
    service, _, adapter = mock_service(node)
    await service._submit(node, pilot(), adapter)
    call = cast(AsyncMock, service._state).await_args
    assert call is not None and call.args[1] == "failed"
    adapter.submit_quarantine.assert_not_awaited()
    node.submission_uncertain = False
    node.submission_attempts = 3
    await service._submit(node, pilot(), adapter)
    assert node.state == "failed"
    adapter.submit_quarantine.assert_not_awaited()


async def test_explicit_rejection_retries_but_ambiguous_error_never_resubmits() -> None:
    node = row(job_identity=None)
    service, session, adapter = mock_service(node)
    session.scalar.side_effect = [0, 0]
    adapter.submit_quarantine.side_effect = QuarantineSubmissionRejected("rejected")
    with pytest.raises(QuarantineSubmissionRejected):
        await service._submit(node, pilot(), adapter)
    assert node.submission_attempts == 1
    assert "submission_started_at" in str(session.execute.await_args.args[0])
    adapter.submit_quarantine.assert_awaited_once()


async def test_foreign_environment_is_read_only_for_operator_actions() -> None:
    node = row(
        job_identity=IDENTITY.model_copy(update={"environment": "prod"}).model_dump()
    )
    service, _, adapter = mock_service(node)
    with pytest.raises(ValueError, match="other environment"):
        await service.request_screen(1, "OPS-123")
    adapter.request_quarantine_screen.assert_not_awaited()
    adapter.delete_quarantine.assert_not_awaited()


async def test_bad_carrier_does_not_starve_valid_reports_or_touch_models() -> None:
    service, _, adapter = mock_service(row())
    service.discover_jobs = AsyncMock()  # type: ignore[method-assign]
    service.ingest_report = AsyncMock()  # type: ignore[method-assign]
    service.rejected_report = AsyncMock()  # type: ignore[method-assign]
    bad = QuarantineReportRejected(
        "bad.pbs",
        "first_qreport_dev_fault_bad",
        None,
        None,
        "0" * 64,
        "malformed_report",
    )
    valid = QuarantineReport(
        "valid.pbs",
        "first_qreport_dev_fault_aaaaaaaa",
        SOURCE.ctime,
        fault_payload(),
        "1" * 64,
    )
    adapter.list_quarantine_reports.return_value = [bad, valid]
    cluster = Cluster(uid=1, name="tara", pilot_system=pilot().model_dump(mode="json"))
    await service.ingest_cluster(cluster, pilot())
    service.rejected_report.assert_awaited_once()
    service.ingest_report.assert_awaited_once_with(cluster, pilot(), adapter, valid)
    adapter.submit_job.assert_not_awaited()
    adapter.terminate_job.assert_not_awaited()


async def test_one_report_transport_failure_does_not_starve_later_reports() -> None:
    service, _, adapter = mock_service(row())
    service.discover_jobs = AsyncMock()  # type: ignore[method-assign]
    service.ingest_report = AsyncMock(side_effect=[TimeoutError(), None])  # type: ignore[method-assign]
    service._infrastructure_receipt = AsyncMock()  # type: ignore[method-assign]
    reports = [
        QuarantineReport(
            f"{number}.pbs", "fault", SOURCE.ctime, fault_payload(), str(number) * 64
        )
        for number in (1, 2)
    ]
    adapter.list_quarantine_reports.return_value = reports
    await service.ingest_cluster(Cluster(uid=1, name="tara"), pilot())
    assert service.ingest_report.await_count == 2
    service._infrastructure_receipt.assert_awaited_once()
    adapter.submit_job.assert_not_awaited()
    adapter.terminate_job.assert_not_awaited()


async def test_unregistered_origin_is_infrastructure_not_node_fault() -> None:
    service, session, adapter = mock_service(row())
    session.scalar.return_value = None
    payload = fault_payload()
    report = QuarantineReport("fault.pbs", "fault", SOURCE.ctime, payload, "1" * 64)
    with pytest.raises(ValueError, match="registered allocation"):
        await service._fault(Cluster(uid=1, name="tara"), pilot(), adapter, report)
    adapter.get_source_allocation.assert_not_awaited()
    session.add.assert_not_called()


@pytest.mark.parametrize("mismatch", ["environment", "source", "request"])
async def test_submit_acceptance_requires_full_verified_identity(mismatch: str) -> None:
    node = row(job_identity=None)
    service, session, adapter = mock_service(node)
    session.scalar.side_effect = [0, 0]
    adapter.submit_quarantine.return_value = SimpleNamespace(
        scheduler_id=IDENTITY.job_id
    )
    accepted = status()
    if mismatch == "environment":
        accepted = replace(
            accepted, identity=IDENTITY.model_copy(update={"environment": "prod"})
        )
    elif mismatch == "source":
        accepted = replace(
            accepted,
            identity=IDENTITY.model_copy(
                update={"source": SOURCE.model_copy(update={"ctime": SOURCE.ctime + 1})}
            ),
        )
    else:
        accepted = replace(accepted, request_verified=False)
    adapter.get_quarantine_status.return_value = accepted
    with pytest.raises(ValueError, match="exact requested identity"):
        await service._submit(node, pilot(), adapter)
    adapter.submit_quarantine.assert_awaited_once()
    adapter.delete_quarantine.assert_not_awaited()


@pytest.mark.parametrize("environment", ["dev", "prod"])
async def test_activation_retained_for_other_control_plane(environment: str) -> None:
    service, session, adapter = mock_service(row())
    payload = activation().model_copy(update={"environment": environment})
    report = QuarantineReport(
        "activation.pbs", "activation", SOURCE.ctime, payload, "1" * 64
    )
    inbox = QuarantineReportInbox(
        uid=2,
        name="activation",
        cluster_name="tara",
        environment=environment,
        report_job_id=report.job_id,
        fingerprint=report.fingerprint,
        payload=payload.model_dump(mode="json"),
        processed_at=datetime.now(timezone.utc),
        acknowledged_at=None,
        reconcile_failures=0,
        reconcile_retry_at=None,
    )
    session.scalar.side_effect = [2, inbox]
    await service.ingest_report(Cluster(uid=1, name="tara"), pilot(), adapter, report)
    adapter.acknowledge_quarantine_report.assert_not_awaited()


async def test_invalid_native_evidence_notice_survives_transport_ack() -> None:
    service, session, adapter = mock_service(row())
    payload = fault_payload()
    report = QuarantineReport("fault.pbs", "fault", SOURCE.ctime, payload, "1" * 64)
    inbox = QuarantineReportInbox(
        uid=2,
        name="fault",
        cluster_name="tara",
        environment="dev",
        report_job_id=report.job_id,
        fingerprint=report.fingerprint,
        payload=payload.model_dump(mode="json"),
        processed_at=None,
        acknowledged_at=None,
        validation_error=None,
        reconcile_failures=0,
        reconcile_retry_at=None,
    )
    session.scalar.side_effect = [2, inbox]
    service._fault = AsyncMock(  # type: ignore[method-assign]
        side_effect=ValueError("native allocation ownership differs")
    )
    await service.ingest_report(Cluster(uid=1, name="tara"), pilot(), adapter, report)
    adapter.acknowledge_quarantine_report.assert_awaited_once()
    final_update = session.execute.await_args.args[0].compile().params
    assert final_update["validation_error"] == "native allocation ownership differs"


async def test_report_acknowledgement_retries_are_bounded_without_model_mutation() -> (
    None
):
    service, session, adapter = mock_service(row())
    payload = fault_payload()
    report = QuarantineReport("fault.pbs", "fault", SOURCE.ctime, payload, "1" * 64)
    inbox = QuarantineReportInbox(
        uid=2,
        name="fault",
        cluster_name="tara",
        environment="dev",
        report_job_id=report.job_id,
        fingerprint=report.fingerprint,
        payload=payload.model_dump(mode="json"),
        processed_at=datetime.now(timezone.utc),
        acknowledged_at=None,
        validation_error="ack unavailable",
        reconcile_failures=3,
        reconcile_retry_at=None,
    )
    session.scalar.side_effect = [2, inbox]
    await service.ingest_report(Cluster(uid=1, name="tara"), pilot(), adapter, report)
    adapter.acknowledge_quarantine_report.assert_not_awaited()
    adapter.submit_job.assert_not_awaited()
    adapter.terminate_job.assert_not_awaited()


async def test_explicit_reestablish_gets_new_operation_budget_and_records_old_count() -> (
    None
):
    node = row(state="lost", submission_attempts=3)
    service, _, adapter = mock_service(node)
    adapter.list_quarantine_jobs.return_value = []
    await service.request_reestablish(1, "OPS-REESTABLISH")
    assert node.operation != OPERATION and len(node.operation) == 32
    assert node.submission_attempts == 0 and node.state == "pending"
    assert node.job_identity is None and node.activation is None
    assert node.evidence["isolation_history"][-1]["operation"] == OPERATION
    assert node.evidence["isolation_history"][-1]["submission_attempts"] == 3
    adapter.submit_job.assert_not_awaited()
    adapter.delete_quarantine.assert_not_awaited()


async def test_automatic_lost_isolation_never_resets_operation_or_retry_budget() -> (
    None
):
    node = row(submission_attempts=3)
    service, _, adapter = mock_service(node)
    adapter.list_quarantine_jobs.return_value = []
    adapter.get_quarantine_status.return_value = None
    with patch.object(service, "_state", QuarantineService._state.__get__(service)):
        await service.reconcile(node.uid)
    assert node.state == "lost"
    assert node.operation == OPERATION and node.submission_attempts == 3
    adapter.submit_quarantine.assert_not_awaited()
    adapter.submit_job.assert_not_awaited()
    adapter.terminate_job.assert_not_awaited()


async def test_stale_activation_cannot_confirm_reestablished_operation() -> None:
    new_identity = IDENTITY.model_copy(
        update={"operation": "f" * 32, "job_id": "3.pbs"}
    )
    service, session, adapter = mock_service(
        row(operation="f" * 32, job_identity=new_identity.model_dump())
    )
    session.scalar.return_value = row(
        operation="f" * 32, job_identity=new_identity.model_dump()
    )
    report = QuarantineReport(
        "activation.pbs", "activation", SOURCE.ctime, activation(), "1" * 64
    )
    with pytest.raises(ValueError, match="current quarantine operation"):
        await service._worker_report(
            Cluster(uid=1, name="tara"), pilot(), adapter, report
        )
    adapter.get_quarantine_status.assert_not_awaited()


def test_normal_initial_job_only_adds_reporting_tags() -> None:
    job = PilotJob(
        name="source",
        cluster_name="tara",
        num_nodes=2,
        gpus_per_node=4,
        walltime_min=60,
    )
    script = PilotSubmitter(pilot(), MagicMock())._render_script(job)
    assert (
        "FIRST_QUARANTINE_VERSION=3" in script
        and "FIRST_QUARANTINE_KIND=model" in script
    )
    assert (
        "FIRST_QUARANTINE_ENV=dev" in script
        and "FIRST_QUARANTINE_CLUSTER=tara" in script
    )
    assert "hold" not in script and "exec /bundles/pilot" in script


async def test_health_alerts_stable_and_fault_evidence_present() -> None:
    service, session, _ = mock_service(row(state="lost"))
    values = MagicMock()
    values.all.return_value = [row(state="lost")]
    empty = MagicMock()
    empty.all.return_value = []
    session.scalars.side_effect = [values, empty, values, empty]
    first = await check_node_quarantine(service.clients)
    second = await check_node_quarantine(service.clients)
    assert first == second
    assert (
        first[0].severity == "crit"
        and "1.pbs" in first[0].detail
        and "/evidence/attempt" in first[0].detail
    )


async def test_operator_routes_require_normal_admin_authentication() -> None:
    async def forbidden() -> None:
        raise HTTPException(status_code=403, detail="admin required")

    app.dependency_overrides[get_admin_user] = forbidden
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            for endpoint in (
                "/control/v1/node-quarantines/1/screen",
                "/control/v1/node-quarantines/1/release",
                "/control/v1/node-quarantines/1/reestablish",
            ):
                response = await client.post(endpoint, json={"reference": "OPS-123"})
                assert response.status_code == 403
    finally:
        app.dependency_overrides.pop(get_admin_user)


def test_schema_has_separate_delete_ack_and_operator_inputs_are_bounded() -> None:
    ddl = str(
        sa.schema.CreateTable(cast(sa.Table, NodeQuarantine.__table__)).compile(
            dialect=cast(type[sa.engine.Dialect], postgresql.dialect)()
        )
    )
    assert "deletion_acknowledged_at" in ddl and "UNIQUE (cluster_name, host)" in ddl
    for reference in ("", "a" * 129, "\nOPS-123"):
        with pytest.raises(ValidationError):
            QuarantineOperatorAction(reference=reference)
    assert QuarantineOperatorAction(reference="OPS-123").reference == "OPS-123"
