import json
from pathlib import Path
from subprocess import CompletedProcess
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from first_common.schema.pilot import PilotRuntimeConfig
from first_pilot.admission import ACTIONS, NodeChecksFailed, admit_nodes, pbs_quarantine
from first_pilot.host_commands import run_on_hosts
from first_pilot.node_checks import (
    CHECKS,
    gpu_count,
    gpu_ecc_errors,
    gpu_memory_free,
    node_check,
    run_node_checks,
)

HEALTHY = "0, 81559, 4, 0, 0\n1, 81559, 4, 0, 0\n"


def _config(tmp_path: Path, **overrides: Any) -> PilotRuntimeConfig:
    values: dict[str, Any] = {
        "ca_crt": "ca",
        "server_crt": "crt",
        "server_key": "key",
        "external_port": 8443,
        "nginx_path": "/opt/test/nginx",
        "ip_allowlist": [],
        "workdir": tmp_path,
        "node_file_env": "TEST_NODEFILE",
        "num_nodes": 2,
        "gpus_per_node": 2,
        "job_name": "test-pilot",
        "walltime_min": 60,
        "network_interfaces": ["lo"],
        "queue": "prod",
        "account": "TestAcct",
    }
    return PilotRuntimeConfig.model_validate({**values, **overrides})


def _nodefile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *hosts: str) -> None:
    nodefile = tmp_path / "nodes"
    nodefile.write_text("".join(f"{host}\n" for host in hosts))
    monkeypatch.setenv("TEST_NODEFILE", str(nodefile))


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------


def test_gpu_checks_pass_a_healthy_node(tmp_path: Path) -> None:
    config = _config(tmp_path)
    assert gpu_count(HEALTHY, config) is None
    assert gpu_memory_free(HEALTHY, config) is None
    assert gpu_ecc_errors(HEALTHY, config) is None


def test_gpu_count_reports_a_missing_gpu(tmp_path: Path) -> None:
    reason = gpu_count("0, 81559, 4, 0, 0\n", _config(tmp_path))
    assert reason == "found 1 GPUs, expected 2"


def test_gpu_memory_free_reports_memory_already_in_use(tmp_path: Path) -> None:
    output = "0, 81559, 4, 0, 0\n1, 81559, 30000, 0, 0\n"
    reason = gpu_memory_free(output, _config(tmp_path))
    assert reason == "GPU 1 already has 30000 of 81559 MiB in use"


def test_gpu_ecc_errors_reports_uncorrected_errors_and_tolerates_na(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    assert gpu_ecc_errors("0, 81559, 4, [N/A], [N/A]\n", config) is None
    reason = gpu_ecc_errors("0, 81559, 4, 0, 3\n", config)
    assert reason == "GPU 0 has 3 uncorrected ECC errors"


# ---------------------------------------------------------------------------
# Running checks across hosts
# ---------------------------------------------------------------------------


@patch("first_pilot.node_checks.run_on_hosts")
def test_run_node_checks_reports_failures_per_host(
    run: MagicMock, tmp_path: Path
) -> None:
    run.return_value = {"n1": HEALTHY, "n2": "0, 81559, 30000, 0, 0\n"}

    failures = run_node_checks(
        ["gpu_count", "gpu_memory_free"], ["n1", "n2"], _config(tmp_path)
    )

    assert failures == {
        "n2": [
            "gpu_count: found 1 GPUs, expected 2",
            "gpu_memory_free: GPU 0 already has 30000 of 81559 MiB in use",
        ]
    }
    # Both checks use the same command, so it is launched only once.
    run.assert_called_once()


def test_run_node_checks_rejects_unknown_names(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unknown node checks"):
        run_node_checks(["no_such_check"], ["n1"], _config(tmp_path))


@patch("first_pilot.host_commands.subprocess.run")
def test_run_on_hosts_runs_locally_on_a_single_node(
    run: MagicMock, tmp_path: Path
) -> None:
    run.return_value = CompletedProcess([], 0, stdout="out\n", stderr="")

    assert run_on_hosts(["uname", "-r"], ["n1"], _config(tmp_path)) == {"n1": "out\n"}
    assert run.call_args.args[0] == ["uname", "-r"]


@patch("first_pilot.host_commands.subprocess.run")
def test_run_on_hosts_uses_ssh_for_ssh_discovery(
    run: MagicMock, tmp_path: Path
) -> None:
    run.side_effect = lambda command, **_: CompletedProcess(
        command, 0, stdout=f"from {command[1]}\n", stderr=""
    )

    outputs = run_on_hosts(["uname", "-r"], ["n1", "n2"], _config(tmp_path))

    assert outputs == {"n1": "from n1\n", "n2": "from n2\n"}
    assert run.call_args.args[0][0] == "ssh"
    assert run.call_args.args[0][2] == "uname -r"


@patch("first_pilot.host_commands.shutil.which", return_value="/usr/bin/uname")
@patch("first_pilot.host_commands._require_executable", return_value="/opt/mpiexec")
@patch("first_pilot.host_commands.subprocess.run")
def test_run_on_hosts_splits_pals_output_by_host_label(
    run: MagicMock, _require: MagicMock, _which: MagicMock, tmp_path: Path
) -> None:
    run.return_value = CompletedProcess(
        [], 0, stdout="n2 1: second\nn1 0: first\nn1 0: more\n", stderr=""
    )
    config = _config(
        tmp_path, gpu_discovery={"method": "pals", "launcher_path": "/opt/mpiexec"}
    )

    outputs = run_on_hosts(["uname", "-r"], ["n1", "n2"], config)

    assert outputs == {"n1": "first\nmore", "n2": "second"}
    assert run.call_args.args[0][0] == "/opt/mpiexec"


@patch("first_pilot.host_commands.subprocess.run")
def test_run_on_hosts_raises_when_the_command_fails(
    run: MagicMock, tmp_path: Path
) -> None:
    run.return_value = CompletedProcess([], 9, stdout="", stderr="broken")

    with pytest.raises(RuntimeError, match="exited 9: broken"):
        run_on_hosts(["uname", "-r"], ["n1"], _config(tmp_path))


# ---------------------------------------------------------------------------
# Admission
# ---------------------------------------------------------------------------


@patch("first_pilot.admission.run_node_checks")
def test_admit_nodes_does_nothing_without_configured_checks(
    run: MagicMock, tmp_path: Path
) -> None:
    admit_nodes(_config(tmp_path))
    run.assert_not_called()


@patch("first_pilot.admission.run_node_checks", return_value={})
def test_admit_nodes_passes_healthy_nodes(
    run: MagicMock, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _nodefile(tmp_path, monkeypatch, "n1", "n2")
    config = _config(tmp_path, node_admission={"checks": ["gpu_count"]})

    admit_nodes(config)

    run.assert_called_once_with(["gpu_count"], ["n1", "n2"], config)


@patch("first_pilot.admission.run_node_checks")
def test_admit_nodes_acts_on_each_bad_node_then_refuses_to_start(
    run: MagicMock, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _nodefile(tmp_path, monkeypatch, "n1", "n2")
    run.return_value = {"n2": ["gpu_count: found 1 GPUs, expected 2"]}
    config = _config(
        tmp_path,
        node_admission={"checks": ["gpu_count"], "on_failure": "pbs_quarantine"},
    )
    action = MagicMock()

    with patch.dict(ACTIONS, {"pbs_quarantine": action}):
        with pytest.raises(NodeChecksFailed, match="n2"):
            admit_nodes(config)

    action.assert_called_once_with("n2", config)


@patch("first_pilot.admission.run_node_checks", return_value={"n1": ["bad"]})
def test_admit_nodes_refuses_to_start_even_if_the_action_fails(
    _run: MagicMock, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _nodefile(tmp_path, monkeypatch, "n1")
    config = _config(tmp_path, num_nodes=1, node_admission={"checks": ["gpu_count"]})

    with patch.dict(ACTIONS, {"exit": MagicMock(side_effect=RuntimeError("boom"))}):
        with pytest.raises(NodeChecksFailed):
            admit_nodes(config)


def test_a_new_check_is_registered_by_name() -> None:
    with patch.dict(CHECKS):

        @node_check("kernel", command=["uname", "-r"])
        def kernel(output: str, config: PilotRuntimeConfig) -> str | None:
            return None if output.strip() == "6.4.0" else "unexpected kernel"

        assert CHECKS["kernel"].command == ("uname", "-r")
        assert CHECKS["kernel"].evaluate("5.14\n", MagicMock()) == "unexpected kernel"


# ---------------------------------------------------------------------------
# PBS quarantine
# ---------------------------------------------------------------------------


def _fake_pbs(existing_job_names: list[str]) -> MagicMock:
    """A stand-in for first_pilot.admission._pbs that records the qsub call."""
    jobs = {f"{i}.pbs": {"Job_Name": name} for i, name in enumerate(existing_job_names)}

    def pbs(command: list[str]) -> str:
        if command[0] == "qstat":
            return json.dumps({"Jobs": jobs})
        return "99.pbs\n"

    return MagicMock(side_effect=pbs)


def _qsub_calls(pbs: MagicMock) -> list[list[str]]:
    return [c.args[0] for c in pbs.call_args_list if c.args[0][0] == "qsub"]


def test_pbs_quarantine_parks_an_exclusive_sleep_job_on_the_node(
    tmp_path: Path,
) -> None:
    pbs = _fake_pbs(["first_dev_tara_v2_pilot-1"])

    with patch("first_pilot.admission._pbs", pbs):
        pbs_quarantine("x4820c7s3b1n0.lab.example", _config(tmp_path))

    assert _qsub_calls(pbs) == [
        [
            "qsub",
            *("-N", "first_quar_x4820c7s3b1n0"),
            *("-q", "prod"),
            *("-A", "TestAcct"),
            *("-l", "select=1:host=x4820c7s3b1n0"),
            *("-l", "place=exclhost"),
            *("-l", "walltime=168:00:00"),
            *("-j", "oe"),
            *("-o", "/dev/null"),
            *("--", "/bin/sleep", "infinity"),
        ]
    ]


def test_pbs_quarantine_skips_a_node_that_is_already_quarantined(
    tmp_path: Path,
) -> None:
    pbs = _fake_pbs(["first_quar_n1"])

    with patch("first_pilot.admission._pbs", pbs):
        pbs_quarantine("n1", _config(tmp_path))

    assert _qsub_calls(pbs) == []


def test_pbs_quarantine_stops_at_the_configured_limit(tmp_path: Path) -> None:
    pbs = _fake_pbs(["first_quar_n1", "first_quar_n2"])
    config = _config(tmp_path, node_admission={"max_quarantined_nodes": 2})

    with patch("first_pilot.admission._pbs", pbs):
        pbs_quarantine("n3", config)

    assert _qsub_calls(pbs) == []


def test_pbs_quarantine_requires_queue_and_account(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="queue and account"):
        pbs_quarantine("n1", _config(tmp_path, queue=None))
