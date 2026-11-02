"""TCP admission requires complete observations and one matching confirmation."""

import json
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from first_common.schema.pilot import PilotRuntimeConfig
from first_common.schema.types import NodeAdmission, PalsDiscovery
from first_pilot.admission import NodeChecksFailed, admit_nodes
from first_pilot.node_checks import run_node_checks

from .test_pilot_node_admission import _config, _nodefile

HOSTS = ["n1", "n2"]


def _observation(host: str, result: str = "pass") -> dict[str, Any]:
    return {
        "host": host,
        "address": {"n1": "10.20.0.1", "n2": "10.20.0.2"}[host],
        "port": 20005,
        "result": result,
        "elapsed_sec": 0.001 if result == "pass" else 2.0,
    }


def _outputs(**results: str) -> dict[str, str]:
    return {
        host: json.dumps(_observation(host, results.get(host, "pass")))
        for host in HOSTS
    }


@pytest.fixture
def config(tmp_path: Path) -> PilotRuntimeConfig:
    return _config(
        tmp_path,
        network_interfaces=["hsn0", "hsn1"],
        gpu_discovery=PalsDiscovery(launcher_path=Path("/opt/test/mpiexec")),
        node_admission={"checks": ["tcp_self_connect"], "tcp_probe_port": 20005},
    )


@pytest.fixture(autouse=True)
def _no_gpu_queries_or_delay(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("query_gpus", "query_gpu_clients"):
        monkeypatch.setattr(
            "first_pilot.node_checks." + name,
            MagicMock(side_effect=AssertionError("network-only screen queried GPUs")),
        )
    monkeypatch.setattr("first_pilot.node_checks.time.sleep", lambda _delay: None)


def test_network_pass_uses_one_complete_sample_and_configured_launch_inputs(
    config: PilotRuntimeConfig,
) -> None:
    evidence: dict[str, object] = {}
    with patch("first_pilot.node_checks.run_on_hosts", return_value=_outputs()) as run:
        assert (
            run_node_checks(
                config.node_admission.checks, HOSTS, config, evidence=evidence
            )
            == {}
        )

    (command, hosts, discovery), _kwargs = run.call_args
    assert run.call_count == 1
    # env keeps the venv interpreter path intact when PALS canonicalizes the
    # executable; resolving the Python symlink itself would lose its packages.
    assert command[:2] == ["/usr/bin/env", sys.executable] and "-m" in command
    assert "first_pilot.tcp_probe" in command
    assert "20005" in command
    assert "hsn0" in " ".join(command) and "hsn1" in " ".join(command)
    assert "127.0.0.1" not in command and "localhost" not in command
    assert hosts == HOSTS and discovery is config.gpu_discovery
    samples = evidence["tcp_self_connect"]
    assert isinstance(samples, list) and len(samples) == 1


@pytest.mark.parametrize("timeout", ["connect_timeout", "accept_timeout"])
def test_matching_timeout_is_confirmed_once_on_every_host(
    config: PilotRuntimeConfig, timeout: str
) -> None:
    first = _outputs(n1=timeout)
    second = _outputs(n1=timeout)
    evidence: dict[str, object] = {}

    with patch(
        "first_pilot.node_checks.run_on_hosts", side_effect=[first, second]
    ) as run:
        failures = run_node_checks(
            config.node_admission.checks, HOSTS, config, evidence=evidence
        )

    assert run.call_count == 2
    assert all(call.args[1] == HOSTS for call in run.call_args_list)
    assert set(failures) == {"n1"}
    assert len(failures["n1"]) == 1
    assert failures["n1"][0].startswith(f"tcp_self_connect: {timeout} on ")
    samples = evidence["tcp_self_connect"]
    assert isinstance(samples, list) and len(samples) == 2


@pytest.mark.parametrize("change", ["recovered", "other_host", "address"])
def test_changed_timeout_evidence_is_infrastructure_not_a_node_fault(
    config: PilotRuntimeConfig, change: str
) -> None:
    first = _outputs(n1="connect_timeout")
    second = _outputs(n1="connect_timeout")
    if change == "recovered":
        second = _outputs()
    elif change == "other_host":
        second = _outputs(n1="connect_timeout", n2="connect_timeout")
    else:
        bad = json.loads(second["n1"])
        bad["address"] = "10.20.0.9"
        second["n1"] = json.dumps(bad)

    with patch(
        "first_pilot.node_checks.run_on_hosts", side_effect=[first, second]
    ) as run:
        with pytest.raises(RuntimeError):
            run_node_checks(config.node_admission.checks, HOSTS, config)
    assert run.call_count == 2


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("host", "not-allocated"),
        ("address", "::1"),
        ("port", 20006),
        ("result", "ERROR"),
        ("elapsed_sec", -1),
    ],
)
def test_unexpected_probe_fields_are_infrastructure_errors(
    config: PilotRuntimeConfig, field: str, value: object
) -> None:
    outputs = _outputs()
    bad = json.loads(outputs["n1"])
    bad[field] = value
    outputs["n1"] = json.dumps(bad)

    with patch("first_pilot.node_checks.run_on_hosts", return_value=outputs) as run:
        with pytest.raises(RuntimeError):
            run_node_checks(config.node_admission.checks, HOSTS, config)
    assert run.call_count == 1


@pytest.mark.parametrize("failure", ["missing_rank", "malformed_json", "missing_field"])
def test_incomplete_probe_observations_are_infrastructure_errors(
    config: PilotRuntimeConfig, failure: str
) -> None:
    outputs = _outputs()
    if failure == "missing_rank":
        del outputs["n2"]
    elif failure == "malformed_json":
        outputs["n1"] = "not JSON"
    else:
        bad = json.loads(outputs["n1"])
        del bad["result"]
        outputs["n1"] = json.dumps(bad)

    with patch("first_pilot.node_checks.run_on_hosts", return_value=outputs):
        with pytest.raises(RuntimeError):
            run_node_checks(config.node_admission.checks, HOSTS, config)


def test_command_or_bind_error_does_not_reach_the_quarantine_action(
    config: PilotRuntimeConfig, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _nodefile(tmp_path, monkeypatch, *HOSTS)
    config.node_admission.on_failure = "pbs_quarantine"
    config.node_admission.environment = "dev"

    with (
        patch(
            "first_pilot.node_checks.run_on_hosts",
            side_effect=RuntimeError("bind collision"),
        ),
        patch("first_pilot.admission._pbs") as pbs,
    ):
        with pytest.raises(RuntimeError, match="bind collision"):
            admit_nodes(config)
    pbs.assert_not_called()


def test_confirmed_network_fault_submits_quarantine_before_admission_exit(
    config: PilotRuntimeConfig, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _nodefile(tmp_path, monkeypatch, *HOSTS)
    monkeypatch.setenv("PBS_JOBID", "7.pbs")
    monkeypatch.setattr(
        "first_pilot.admission.pwd.getpwuid", lambda _uid: MagicMock(pw_name="svc")
    )
    config.node_admission.on_failure = "pbs_quarantine"
    config.node_admission.environment = "dev"
    outputs = _outputs(n1="connect_timeout")

    with (
        patch("first_pilot.node_checks.run_on_hosts", side_effect=[outputs, outputs]),
        patch(
            "first_pilot.admission._pbs", side_effect=['{"Jobs": {}}', "99.pbs\n"]
        ) as pbs,
    ):
        with pytest.raises(NodeChecksFailed):
            admit_nodes(config)

    assert [call.args[0][0] for call in pbs.call_args_list] == ["qstat", "qsub"]
    qsub = pbs.call_args_list[1].args[0]
    assert "select=1:host=n1" in qsub and "place=exclhost" in qsub
    variables = dict(
        item.split("=", 1) for item in qsub[qsub.index("-v") + 1].split(",")
    )
    assert variables["FIRST_QUAR_SOURCE"] == "7.pbs"
    assert variables["FIRST_QUAR_REASON"] == "tcp_self_connect"
    evidence = json.loads(Path(variables["FIRST_QUAR_EVIDENCE"]).read_text())
    assert evidence["source"] == "7.pbs"
    assert "tcp_self_connect" in evidence["observations"]


def test_network_check_requires_an_explicit_unprivileged_port() -> None:
    assert NodeAdmission().tcp_probe_port is None
    with pytest.raises(ValidationError, match="tcp_probe_port"):
        NodeAdmission(checks=["tcp_self_connect"])
    for port in (0, 1023, 65536):
        with pytest.raises(ValidationError):
            NodeAdmission(checks=["tcp_self_connect"], tcp_probe_port=port)
    assert (
        NodeAdmission(checks=["tcp_self_connect"], tcp_probe_port=20005).tcp_probe_port
        == 20005
    )
