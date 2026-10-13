"""Controlled native-PBS doubles, separate from actual scheduler qualification."""

import copy
import json
import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest

from first_common.pbs_quarantine import (
    TAG,
    discover_quarantines,
    prove_isolation,
    validate_quarantine,
)
from first_common.schema.pilot import PilotRuntimeConfig
from first_pilot import admission

HOST = "x4820c2s2b1n0"
SOURCE = "100.tara-north-pbs-01.lab.alcf.anl.gov"
JOB = "101.tara-north-pbs-01.lab.alcf.anl.gov"
OWNER = "openinference_svc"
OP = "a" * 32


def quarantine(
    host: str = HOST, *, environment: str = "dev", state: str = "Q"
) -> dict[str, Any]:
    return {
        "Job_Name": f"first_quar_{environment}_{host}_{OP[:8]}",
        "Job_Owner": f"{OWNER}@pilot",
        "job_state": state,
        "Resource_List": {
            "select": f"1:host={host}:ncpus=288:ngpus=4",
            "place": "exclhost",
            "walltime": "168:00:00",
        },
        "Variable_List": {
            TAG + "VERSION": "1",
            TAG + "ENV": environment,
            TAG + "CLUSTER": "tara",
            TAG + "HOST": host,
            TAG + "OP": OP,
            TAG + "SOURCE": SOURCE,
            TAG + "REASON": "gpu_memory_free",
            TAG + "EVIDENCE": "/service/evidence/fault.json",
        },
        "exec_vnode": f"({host}:ncpus=288:ngpus=4)",
    }


def node(host: str = HOST) -> dict[str, Any]:
    return {
        "resources_available": {"host": host, "ncpus": 288, "ngpus": 4},
        "resources_assigned": {"ncpus": 288, "ngpus": 4},
        "jobs": [JOB],
        "state": "job-exclusive",
    }


@pytest.fixture
def native(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[PilotRuntimeConfig, dict[str, dict[str, Any]], Mock]:
    path = tmp_path / "nodes"
    path.write_text(HOST + "\n")
    monkeypatch.setenv("PBS_NODEFILE", str(path))
    monkeypatch.setenv("PBS_JOBID", SOURCE)
    monkeypatch.setenv("TARA_DEPLOYMENT_ENV", "dev")
    monkeypatch.setattr(admission, "_owner", lambda: OWNER)
    monkeypatch.setattr("first_pilot.admission.time.sleep", lambda _duration: None)
    config = PilotRuntimeConfig.model_validate(
        {
            "ca_crt": "ca",
            "server_crt": "crt",
            "server_key": "key",
            "external_port": 8443,
            "nginx_path": "/opt/nginx",
            "ip_allowlist": [],
            "workdir": tmp_path,
            "node_file_env": "PBS_NODEFILE",
            "num_nodes": 1,
            "gpus_per_node": 4,
            "job_name": "pilot-test",
            "job_name_prefix": "first_dev_",
            "walltime_min": 60,
            "network_interfaces": ["hsn0"],
            "queue": "prod",
            "account": "inference_service",
            "node_admission": {"checks": [], "on_failure": "pbs_quarantine"},
        }
    )
    jobs: dict[str, dict[str, Any]] = {
        SOURCE: {
            "Job_Name": config.job_name_prefix + config.job_name,
            "Job_Owner": f"{OWNER}@pilot",
            "job_state": "R",
            "exec_vnode": f"({HOST}:ncpus=288:ngpus=4)",
        }
    }

    def pbs(command: list[str], **_kwargs: Any) -> str:
        if command[0] == "qstat":
            return json.dumps({"Jobs": jobs})
        if command[0] == "pbsnodes":
            return json.dumps({"nodes": {HOST: node()}})
        if command[0] == "qsub":
            meta = dict(
                part.split("=", 1)
                for part in command[command.index("-v") + 1].split(",")
            )
            attrs = [
                command[index + 1] for index, item in enumerate(command) if item == "-l"
            ]
            jobs[JOB] = {
                "Job_Name": command[command.index("-N") + 1],
                "Job_Owner": f"{OWNER}@pilot",
                "job_state": "Q",
                "Variable_List": meta,
                "Resource_List": dict(attr.split("=", 1) for attr in attrs),
            }
            return JOB + "\n"
        raise AssertionError(command)

    mock = Mock(side_effect=pbs)
    monkeypatch.setattr(admission, "_pbs", mock)
    return config, jobs, mock


def test_native_submission_accepted_while_source_still_owned_and_pending(
    native: tuple[PilotRuntimeConfig, dict[str, dict[str, Any]], Mock],
) -> None:
    config, jobs, pbs = native
    admission.pbs_quarantine(HOST, config)
    assert jobs[SOURCE]["job_state"] == "R"
    assert jobs[JOB]["job_state"] == "Q"
    assert validate_quarantine(JOB, jobs[JOB], OWNER).host == HOST
    commands = [call.args[0] for call in pbs.call_args_list]
    assert sum(command[0] == "qsub" for command in commands) == 1
    assert not any(command[0] in {"qhold", "qrls", "qdel"} for command in commands)
    assert commands[-1][0] == "qstat"
    assert commands[-2][-3:] == ["--", "/bin/sleep", "infinity"]


@pytest.mark.parametrize("state", list("QHWTRBSE"))
@pytest.mark.parametrize("environment", ["dev", "prod"])
def test_start_guard_rejects_active_both_environment_requests_before_checks(
    native: tuple[PilotRuntimeConfig, dict[str, dict[str, Any]], Mock],
    monkeypatch: pytest.MonkeyPatch,
    state: str,
    environment: str,
) -> None:
    config, jobs, _pbs = native
    jobs[JOB] = quarantine(environment=environment, state=state)
    check = Mock()
    monkeypatch.setattr(admission, "run_node_checks", check)
    with pytest.raises(admission.AdmissionDeferredKnownQuarantine):
        admission.admit_nodes(config)
    check.assert_not_called()


def test_start_guard_lookup_error_is_infrastructure_not_fault(
    native: tuple[PilotRuntimeConfig, dict[str, dict[str, Any]], Mock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, _jobs, pbs = native
    pbs.side_effect = subprocess.TimeoutExpired("qstat", 1)
    action = Mock()
    monkeypatch.setitem(admission.ACTIONS, "pbs_quarantine", action)
    with pytest.raises(subprocess.TimeoutExpired):
        admission.admit_nodes(config)
    action.assert_not_called()


def test_existing_valid_other_environment_request_prevents_duplicate(
    native: tuple[PilotRuntimeConfig, dict[str, dict[str, Any]], Mock],
) -> None:
    config, jobs, pbs = native
    jobs[JOB] = quarantine(environment="prod")
    admission.pbs_quarantine(HOST, config)
    assert not any(call.args[0][0] == "qsub" for call in pbs.call_args_list)


def test_ambiguous_acknowledgement_looks_up_same_operation_without_resubmitting(
    native: tuple[PilotRuntimeConfig, dict[str, dict[str, Any]], Mock],
) -> None:
    config, jobs, pbs = native
    original = pbs.side_effect

    def ambiguous(command: list[str], **kwargs: Any) -> str:
        result: str = original(command, **kwargs)
        if command[0] == "qsub":
            raise subprocess.TimeoutExpired("qsub", 1)
        return result

    pbs.side_effect = ambiguous
    admission.pbs_quarantine(HOST, config)
    assert JOB in jobs
    assert sum(call.args[0][0] == "qsub" for call in pbs.call_args_list) == 1


def test_ambiguous_ack_without_visible_request_is_bounded_no_resubmission(
    native: tuple[PilotRuntimeConfig, dict[str, dict[str, Any]], Mock],
) -> None:
    config, _jobs, pbs = native
    original = pbs.side_effect

    def ambiguous(command: list[str], **kwargs: Any) -> str:
        if command[0] == "qsub":
            return "not a job id\n"
        result: str = original(command, **kwargs)
        return result

    pbs.side_effect = ambiguous
    with pytest.raises(RuntimeError, match="acceptance unresolved"):
        admission.pbs_quarantine(HOST, config)
    assert sum(call.args[0][0] == "qsub" for call in pbs.call_args_list) == 1
    assert sum(call.args[0][0] == "qstat" for call in pbs.call_args_list) == 4


@pytest.mark.parametrize(
    "mutation",
    [
        {"Job_Name": "first_quar_dev_x4820c2s2b1"},
        {"Job_Owner": "other@pilot"},
        {"job_state": "?"},
        {
            "Resource_List": {
                "select": f"1:host={HOST}",
                "place": "exclhost",
                "walltime": "168:00:00",
            }
        },
        {
            "Resource_List": {
                "select": f"1:host={HOST}:ncpus=288:ngpus=4",
                "place": "shared",
                "walltime": "168:00:00",
            }
        },
    ],
)
def test_request_validation_rejects_unverified_identity(
    mutation: dict[str, Any],
) -> None:
    job = quarantine()
    job.update(mutation)
    with pytest.raises(RuntimeError):
        validate_quarantine(JOB, job, OWNER)


@pytest.mark.parametrize(
    "select",
    [
        f"1:host={HOST}:host={HOST}:ncpus=288:ngpus=4",
        f"2:host={HOST}:ncpus=288:ngpus=4",
        f"1:host={HOST}:ncpus=288:ngpus=4+1:host=other",
        "1:host=other:ncpus=288:ngpus=4",
    ],
)
def test_duplicate_or_ambiguous_host_selection_rejected(select: str) -> None:
    job = quarantine()
    job["Resource_List"]["select"] = select
    with pytest.raises(RuntimeError):
        validate_quarantine(JOB, job, OWNER)


def test_unrelated_jobs_and_foreign_named_jobs_are_ignored() -> None:
    foreign = quarantine()
    foreign["Job_Owner"] = "another@host"
    assert (
        discover_quarantines({JOB: foreign, SOURCE: {"Job_Name": "model"}}, OWNER) == []
    )


def test_owned_quarantine_like_invalid_job_is_lookup_error() -> None:
    invalid = quarantine()
    invalid.pop("Variable_List")
    with pytest.raises(RuntimeError):
        discover_quarantines({JOB: invalid}, OWNER)


def test_isolation_requires_actual_full_host_and_all_owner_occupancy() -> None:
    job = quarantine(state="R")
    request = validate_quarantine(JOB, job, OWNER)
    assert prove_isolation(request, job, {JOB: job}, {HOST: node()})
    competitor = {
        "Job_Name": "other-user",
        "job_state": "R",
        "exec_vnode": f"({HOST}:ncpus=1:ngpus=1)",
    }
    assert not prove_isolation(
        request, job, {JOB: job, SOURCE: competitor}, {HOST: node()}
    )
    incomplete = copy.deepcopy(node())
    incomplete["resources_assigned"]["ngpus"] = 3
    assert not prove_isolation(request, job, {JOB: job}, {HOST: incomplete})
    incomplete["state"] = "free"
    assert not prove_isolation(request, job, {JOB: job}, {HOST: incomplete})


def test_requested_exclusive_flag_alone_does_not_confirm_isolation() -> None:
    job = quarantine()
    request = validate_quarantine(JOB, job, OWNER)
    assert not prove_isolation(request, job, {JOB: job}, {HOST: node()})


def test_unknown_census_owner_state_cannot_confirm_isolation() -> None:
    job = quarantine(state="R")
    request = validate_quarantine(JOB, job, OWNER)
    with pytest.raises(RuntimeError, match="unknown job state"):
        prove_isolation(
            request, job, {JOB: job, SOURCE: {"job_state": "?"}}, {HOST: node()}
        )


def test_wrong_source_owner_or_allocation_cannot_create_quarantine(
    native: tuple[PilotRuntimeConfig, dict[str, dict[str, Any]], Mock],
) -> None:
    config, jobs, pbs = native
    jobs[SOURCE]["Job_Owner"] = "another@pilot"
    with pytest.raises(RuntimeError, match="ownership"):
        admission.pbs_quarantine(HOST, config)
    assert not any(call.args[0][0] == "qsub" for call in pbs.call_args_list)


def test_quarantine_safety_cap_is_action_error_not_scheduling_mutation(
    native: tuple[PilotRuntimeConfig, dict[str, dict[str, Any]], Mock],
) -> None:
    config, jobs, pbs = native
    config.node_admission.max_quarantined_nodes = 1
    jobs[JOB] = quarantine(host="x4820c2s2b0n0")
    with pytest.raises(RuntimeError, match="safety budget"):
        admission.pbs_quarantine(HOST, config)
    assert not any(
        call.args[0][0] in {"qsub", "qdel", "qhold"} for call in pbs.call_args_list
    )


def test_fault_actions_complete_before_failed_pilot_exits(
    native: tuple[PilotRuntimeConfig, dict[str, dict[str, Any]], Mock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, jobs, _pbs = native
    config.node_admission.checks = ["gpu_count"]
    monkeypatch.setattr(
        admission,
        "run_node_checks",
        lambda *_args, **_kwargs: {HOST: ["gpu_count: missing GPU"]},
    )
    with pytest.raises(admission.NodeChecksFailed):
        admission.admit_nodes(config)
    assert jobs[SOURCE]["job_state"] == "R"
    assert validate_quarantine(JOB, jobs[JOB], OWNER).state == "Q"


def test_duplicate_metadata_and_missing_jobs_fail_closed() -> None:
    with pytest.raises(RuntimeError, match="duplicate"):
        admission._json_rows('{"Jobs": {}, "Jobs": {}}', "Jobs")
    with pytest.raises(RuntimeError, match="incomplete"):
        admission._json_rows("{}", "Jobs")


def test_owned_tagged_renamed_quarantine_is_not_silently_ignored() -> None:
    job = quarantine()
    job["Job_Name"] = "renamed"
    with pytest.raises(RuntimeError, match="name differs"):
        discover_quarantines({JOB: job}, OWNER)


@pytest.mark.parametrize(
    "field,value",
    [
        ("Job_Name", "another-model"),
        ("job_state", "Q"),
        ("exec_vnode", "(x4819c0s0b0n0:ncpus=288:ngpus=4)"),
        ("exec_vnode", f"({HOST}:ncpus=288:ngpus=3)"),
    ],
)
def test_incoherent_source_identity_prevents_quarantine_submission(
    native: tuple[PilotRuntimeConfig, dict[str, dict[str, Any]], Mock],
    field: str,
    value: str,
) -> None:
    config, jobs, pbs = native
    jobs[SOURCE][field] = value
    with pytest.raises(RuntimeError):
        admission.pbs_quarantine(HOST, config)
    assert not any(call.args[0][0] == "qsub" for call in pbs.call_args_list)


@pytest.mark.parametrize(
    "field,value",
    [
        ("Job_Name", "first_quar_dev_truncated"),
        (TAG + "REASON", "different_fault"),
        (TAG + "EVIDENCE", "/different/evidence.json"),
    ],
)
def test_changed_accepted_identity_never_claims_submission_success(
    native: tuple[PilotRuntimeConfig, dict[str, dict[str, Any]], Mock],
    field: str,
    value: str,
) -> None:
    config, jobs, pbs = native
    original = pbs.side_effect

    def altered(command: list[str], **kwargs: Any) -> str:
        result: str = original(command, **kwargs)
        if command[0] == "qsub":
            if field.startswith(TAG):
                jobs[JOB]["Variable_List"][field] = value
            else:
                jobs[JOB][field] = value
        return result

    pbs.side_effect = altered
    with pytest.raises(RuntimeError):
        admission.pbs_quarantine(HOST, config)
    assert sum(call.args[0][0] == "qsub" for call in pbs.call_args_list) == 1


def test_environment_disagreement_is_configuration_not_node_fault(
    native: tuple[PilotRuntimeConfig, dict[str, dict[str, Any]], Mock],
) -> None:
    config, _jobs, pbs = native
    config.node_admission.environment = "prod"
    with pytest.raises(RuntimeError, match="disagrees"):
        admission.pbs_quarantine(HOST, config)
    assert not any(call.args[0][0] == "qsub" for call in pbs.call_args_list)


def test_exit_only_cluster_has_no_native_pbs_requirement(
    native: tuple[PilotRuntimeConfig, dict[str, dict[str, Any]], Mock],
) -> None:
    config, _jobs, pbs = native
    config.node_admission.on_failure = "exit"
    admission.admit_nodes(config)
    pbs.assert_not_called()


def test_native_calls_have_bounded_timeouts(
    native: tuple[PilotRuntimeConfig, dict[str, dict[str, Any]], Mock],
) -> None:
    config, _jobs, pbs = native
    admission.pbs_quarantine(HOST, config)
    assert all(0 < call.kwargs["timeout"] <= 15 for call in pbs.call_args_list)
