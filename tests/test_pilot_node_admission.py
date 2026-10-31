import json
from pathlib import Path
from subprocess import CompletedProcess
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from first_common.schema.pilot import GpuInfo, PilotRuntimeConfig
from first_common.schema.types import NodeAdmission, PalsDiscovery, SSHDiscovery
from first_pilot.admission import (
    ACTIONS,
    NodeChecksFailed,
    admit_nodes,
    pbs_quarantine,
)
from first_pilot.gpu_inventory import parse_gpus
from first_pilot.host_commands import run_on_hosts
from first_pilot.node_checks import (
    CHECKS,
    gpu_count,
    gpu_ecc_errors,
    gpu_memory_free,
    run_node_checks,
)

from .fixtures.nvidia_smi import gpu_row, gpu_rows

A100 = {"memory_total_mib": 81559, "memory_free_mib": 81555}
HEALTHY = gpu_rows(2, host="n1", **A100)
MISSING = gpu_row(0, host="n2", **{**A100, "memory_free_mib": 51559})


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
        "cluster": "alpha",
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
    gpus = parse_gpus(HEALTHY)
    assert gpu_count(gpus, config) == {}
    assert gpu_memory_free(gpus, config) == {}
    assert gpu_ecc_errors(gpus, config) == {}


def test_gpu_count_reports_a_missing_gpu(tmp_path: Path) -> None:
    faults = gpu_count(parse_gpus(gpu_row(0, host="n1", **A100)), _config(tmp_path))
    assert faults == {None: "found 1 GPUs, expected 2"}


def test_gpu_memory_free_reports_memory_already_in_use(tmp_path: Path) -> None:
    output = HEALTHY.replace("81555", "51559", 1)
    faults = gpu_memory_free(parse_gpus(output), _config(tmp_path))
    assert faults == {"0": "GPU 0 has 51559 MiB free; requires 77738 of 81559 MiB"}


def test_gpu_ecc_errors_reports_uncorrected_errors_and_rejects_na(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    unavailable = parse_gpus(gpu_rows(2, ecc_uncorrected_aggregate=None))
    with pytest.raises(RuntimeError, match="does not report"):
        gpu_ecc_errors(unavailable, config)
    faults = gpu_ecc_errors(
        parse_gpus(gpu_rows(2, ecc_uncorrected_aggregate=3)), config
    )
    assert faults == {
        "0": "GPU 0 has 3 uncorrected ECC errors",
        "1": "GPU 1 has 3 uncorrected ECC errors",
    }


# ---------------------------------------------------------------------------
# Running checks across hosts
# ---------------------------------------------------------------------------


@patch("first_pilot.gpu_inventory.run_on_hosts")
def test_run_node_checks_reports_failures_per_host(
    run: MagicMock, tmp_path: Path
) -> None:
    def output(command: list[str], *_args: Any, **_kwargs: Any) -> dict[str, str]:
        if command[1].startswith("--query-compute-apps"):
            return {"n1": "", "n2": ""}
        return {"n1": HEALTHY, "n2": MISSING}

    run.side_effect = output

    failures = run_node_checks(
        ["gpu_count", "gpu_memory_free"], ["n1", "n2"], _config(tmp_path)
    )

    assert failures == {
        "n2": [
            "gpu_count: found 1 GPUs, expected 2 in both observations",
            "gpu_memory_free: GPU 0 has 51559 MiB free; requires 77738 of 81559 MiB in both observations with no GPU clients",
        ]
    }
    # The GPU checks share two coherent samples, each with a GPU-client query.
    assert run.call_count == 4


def test_run_node_checks_rejects_unknown_names(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unknown node checks"):
        run_node_checks(["no_such_check"], ["n1"], _config(tmp_path))


MARKER = "FIRST_HOST_COMMAND_testnonce"
_testnonce = patch(
    "first_pilot.host_commands.uuid.uuid4",
    return_value=SimpleNamespace(hex="testnonce"),
)


def _completed(host: str, output: str) -> str:
    return f"{output}\n{MARKER}:{host}:0\n"


@_testnonce
@patch("first_pilot.host_commands.socket.gethostname", return_value="n1")
@patch("first_pilot.host_commands.subprocess.run")
def test_run_on_hosts_runs_locally_on_a_single_node(
    run: MagicMock, _hostname: MagicMock, _uuid: MagicMock
) -> None:
    run.return_value = CompletedProcess(
        [], 0, stdout=_completed("n1", "out\n"), stderr=""
    )

    assert run_on_hosts(["uname", "-r"], ["n1"], SSHDiscovery()) == {"n1": "out"}
    command = run.call_args.args[0]
    assert command[:2] == ["/bin/sh", "-c"]
    assert command[-2:] == ["uname", "-r"]


@_testnonce
@patch("first_pilot.host_commands.subprocess.run")
def test_run_on_hosts_uses_ssh_for_ssh_discovery(
    run: MagicMock, _uuid: MagicMock
) -> None:
    run.side_effect = lambda command, **_: CompletedProcess(
        command, 0, stdout=_completed(command[1], f"from {command[1]}\n"), stderr=""
    )

    outputs = run_on_hosts(["uname", "-r"], ["n1", "n2"], SSHDiscovery())

    assert outputs == {"n1": "from n1", "n2": "from n2"}
    assert run.call_args.args[0][0] == "ssh"
    assert run.call_args.args[0][2].endswith(" uname -r")


@_testnonce
@patch(
    "first_pilot.host_commands.shutil.which",
    side_effect=lambda name: name if name.startswith("/") else "/usr/bin/" + name,
)
@patch("first_pilot.host_commands._require_executable", return_value="/opt/mpiexec")
@patch("first_pilot.host_commands.subprocess.run")
def test_run_on_hosts_splits_pals_output_by_rank(
    run: MagicMock, _require: MagicMock, _which: MagicMock, _uuid: MagicMock
) -> None:
    run.return_value = CompletedProcess(
        [],
        0,
        stdout=(
            f"n2 1: second\nn1 0: first\nn1 0: more\n"
            f"n2 1: {MARKER}:n2:0\nn1 0: {MARKER}:n1:0\n"
        ),
        stderr="",
    )
    discovery = PalsDiscovery(launcher_path=Path("/opt/mpiexec"))

    outputs = run_on_hosts(["uname", "-r"], ["n1", "n2"], discovery)

    assert outputs == {"n1": "first\nmore", "n2": "second"}
    assert run.call_args.args[0][0] == "/opt/mpiexec"
    # Ubuntu 26.04's /usr/bin/uname is a symlink into the coreutils multicall
    # tree, so assert the path resolve() gives, not the name the mock returns.
    assert str(Path("/usr/bin/uname").resolve()) in run.call_args.args[0]


@patch("first_pilot.host_commands.subprocess.run")
def test_run_on_hosts_raises_when_the_command_fails(
    run: MagicMock, tmp_path: Path
) -> None:
    run.return_value = CompletedProcess([], 9, stdout="", stderr="broken")

    with pytest.raises(RuntimeError, match="exited 9: broken"):
        run_on_hosts(["uname", "-r"], ["n1"], SSHDiscovery())


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
    action = MagicMock()

    with patch.dict(ACTIONS, {"exit": action}):
        admit_nodes(config)

    assert run.call_args.args == (["gpu_count"], ["n1", "n2"], config)
    action.assert_called_once_with(config, ["n1", "n2"], {}, None)


@patch("first_pilot.admission.run_node_checks")
def test_admit_nodes_acts_on_the_screen_then_refuses_to_start(
    run: MagicMock, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _nodefile(tmp_path, monkeypatch, "n1", "n2")
    failures = {"n2": ["gpu_count: found 1 GPUs, expected 2"]}
    run.return_value = failures
    config = _config(tmp_path, node_admission={"checks": ["gpu_count"]})
    action = MagicMock()

    with patch.dict(ACTIONS, {"exit": action}):
        with pytest.raises(NodeChecksFailed, match="n2"):
            admit_nodes(config)

    _config_arg, hosts, failures_arg, evidence = action.call_args.args
    assert (hosts, failures_arg) == (["n1", "n2"], failures)
    assert json.loads(Path(evidence).read_text())["failures"] == failures


@pytest.mark.parametrize("failures", [{}, {"n1": ["bad"]}])
def test_a_failing_action_does_not_change_the_screen_result(
    failures: dict[str, list[str]], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _nodefile(tmp_path, monkeypatch, "n1")
    config = _config(tmp_path, num_nodes=1, node_admission={"checks": ["gpu_count"]})

    with (
        patch("first_pilot.admission.run_node_checks", return_value=failures),
        patch.dict(ACTIONS, {"exit": MagicMock(side_effect=RuntimeError("boom"))}),
    ):
        if failures:
            with pytest.raises(NodeChecksFailed):
                admit_nodes(config)
        else:
            admit_nodes(config)


@patch("first_pilot.node_checks.time.sleep")
@patch("first_pilot.gpu_inventory.run_on_hosts", return_value={"n1": HEALTHY})
def test_a_new_check_is_registered_by_name(
    _run: MagicMock, _sleep: MagicMock, tmp_path: Path
) -> None:
    def driver_pinned(
        gpus: list[GpuInfo], config: PilotRuntimeConfig
    ) -> dict[str | None, str]:
        return {None: "unexpected driver"} if gpus[0].driver_version != "1.0" else {}

    with patch.dict(CHECKS, {"driver_pinned": driver_pinned}):
        failures = run_node_checks(["driver_pinned"], ["n1"], _config(tmp_path))

    assert failures == {"n1": ["driver_pinned: unexpected driver in both observations"]}


# ---------------------------------------------------------------------------
# PBS quarantine
# ---------------------------------------------------------------------------


def _quarantine_config(tmp_path: Path, **admission: Any) -> PilotRuntimeConfig:
    return _config(
        tmp_path,
        node_admission={
            "on_failure": "pbs_quarantine",
            "environment": "dev",
            **admission,
        },
    )


def _quarantine_job(env: str, host: str, owner: str = "svc") -> dict[str, Any]:
    return {
        "Job_Name": f"first_quar_{env}_{host}",
        "Job_Owner": f"{owner}@login1",
        "Variable_List": {"FIRST_QUAR_ENV": env, "FIRST_QUAR_HOST": host},
    }


def _fake_pbs(jobs: dict[str, dict[str, Any]]) -> MagicMock:
    """A stand-in for first_pilot.admission._pbs."""

    def pbs(command: list[str]) -> str:
        if command[0] == "qstat":
            return json.dumps({"Jobs": jobs})
        return "99.pbs\n" if command[0] == "qsub" else ""

    return MagicMock(side_effect=pbs)


def _calls(pbs: MagicMock, name: str) -> list[list[str]]:
    return [c.args[0] for c in pbs.call_args_list if c.args[0][0] == name]


@pytest.fixture(autouse=True)
def _service_user(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "first_pilot.admission.pwd.getpwuid", lambda _uid: MagicMock(pw_name="svc")
    )
    monkeypatch.setenv("PBS_JOBID", "7.pbs")


def test_pbs_quarantine_parks_an_exclusive_sleep_job_on_the_node(
    tmp_path: Path,
) -> None:
    pbs = _fake_pbs({})
    failures = {"x4820c7s3b1n0.lab.example": ["gpu_count: x", "gpu_ecc_errors: y"]}

    with patch("first_pilot.admission._pbs", pbs):
        pbs_quarantine(
            _quarantine_config(tmp_path),
            ["x4820c7s3b1n0.lab.example"],
            failures,
            "/w/evidence.json",
        )

    assert _calls(pbs, "qsub") == [
        [
            "qsub",
            *("-N", "first_quar_dev_x4820c7s3b1n0"),
            *("-q", "prod"),
            *("-A", "TestAcct"),
            *("-l", "select=1:host=x4820c7s3b1n0"),
            *("-l", "place=exclhost"),
            *("-l", "walltime=168:00:00"),
            *(
                "-v",
                "FIRST_QUAR_ENV=dev,FIRST_QUAR_HOST=x4820c7s3b1n0,"
                "FIRST_QUAR_SOURCE=7.pbs,FIRST_QUAR_REASON=gpu_count+gpu_ecc_errors,"
                "FIRST_QUAR_EVIDENCE=/w/evidence.json",
            ),
            *("-j", "oe"),
            *("-o", "/dev/null"),
            *("--", "/bin/sleep", "infinity"),
        ]
    ]


def test_pbs_quarantine_skips_a_node_already_quarantined_by_its_environment(
    tmp_path: Path,
) -> None:
    pbs = _fake_pbs({"1.pbs": _quarantine_job("dev", "n1")})

    with patch("first_pilot.admission._pbs", pbs):
        pbs_quarantine(_quarantine_config(tmp_path), ["n1"], {"n1": ["bad"]}, None)

    assert _calls(pbs, "qsub") == []


def test_pbs_quarantine_ignores_other_environments_and_owners(
    tmp_path: Path,
) -> None:
    pbs = _fake_pbs(
        {
            "1.pbs": _quarantine_job("prod", "n1"),
            "2.pbs": _quarantine_job("dev", "n2", owner="someone"),
        }
    )
    config = _quarantine_config(tmp_path, max_quarantined_nodes=1)

    with patch("first_pilot.admission._pbs", pbs):
        pbs_quarantine(config, ["n1", "n2"], {"n1": ["bad"]}, None)

    # Prod's n1 job neither dedupes Dev's request nor counts toward Dev's cap.
    # Neither foreign job is deleted, though n2 passed.
    assert len(_calls(pbs, "qsub")) == 1
    assert _calls(pbs, "qdel") == []


def test_pbs_quarantine_stops_at_the_configured_limit(tmp_path: Path) -> None:
    pbs = _fake_pbs(
        {"1.pbs": _quarantine_job("dev", "n8"), "2.pbs": _quarantine_job("dev", "n9")}
    )
    config = _quarantine_config(tmp_path, max_quarantined_nodes=2)

    with patch("first_pilot.admission._pbs", pbs):
        pbs_quarantine(config, ["n1"], {"n1": ["bad"]}, None)

    assert _calls(pbs, "qsub") == []


def test_pbs_quarantine_deletes_queued_quarantines_of_passing_nodes(
    tmp_path: Path,
) -> None:
    pbs = _fake_pbs(
        {
            "1.pbs": _quarantine_job("dev", "n1"),
            "2.pbs": _quarantine_job("dev", "n1"),  # a duplicate request
            "3.pbs": _quarantine_job("dev", "n2"),  # n2 failed again
            "4.pbs": _quarantine_job("dev", "n3"),  # not in this allocation
        }
    )

    with patch("first_pilot.admission._pbs", pbs):
        pbs_quarantine(
            _quarantine_config(tmp_path), ["n1.lab", "n2"], {"n2": ["bad"]}, None
        )

    assert _calls(pbs, "qdel") == [["qdel", "1.pbs"], ["qdel", "2.pbs"]]
    assert _calls(pbs, "qsub") == []


def test_pbs_quarantine_requires_an_environment() -> None:
    with pytest.raises(ValidationError, match="requires environment"):
        NodeAdmission(on_failure="pbs_quarantine")


@pytest.mark.parametrize("field", ["queue", "account"])
def test_runtime_config_requires_the_pilot_queue_and_account(
    field: str, tmp_path: Path
) -> None:
    values = _config(tmp_path).model_dump(exclude={field})
    with pytest.raises(ValidationError, match=field):
        PilotRuntimeConfig.model_validate(values)


def test_pbs_quarantine_sanitizes_informational_variables(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PBS_JOBID", "7.pbs,X=1")
    pbs = _fake_pbs({})

    with patch("first_pilot.admission._pbs", pbs):
        pbs_quarantine(
            _quarantine_config(tmp_path),
            ["n1"],
            {"n1": ["odd,check: a, b", "gpu_count: x"]},
            "/w/my evidence,'1'.json",
        )

    (qsub,) = _calls(pbs, "qsub")
    variables = qsub[qsub.index("-v") + 1].split(",")
    assert variables == [
        "FIRST_QUAR_ENV=dev",
        "FIRST_QUAR_HOST=n1",
        "FIRST_QUAR_SOURCE=7.pbs_X_1",
        "FIRST_QUAR_REASON=odd_check+gpu_count",
        "FIRST_QUAR_EVIDENCE=/w/my_evidence__1_.json",
    ]


def test_pbs_quarantine_refuses_a_host_it_cannot_identify(tmp_path: Path) -> None:
    pbs = _fake_pbs({})

    with patch("first_pilot.admission._pbs", pbs):
        with pytest.raises(ValueError, match="cannot quarantine host"):
            pbs_quarantine(
                _quarantine_config(tmp_path), ["n1,n2"], {"n1,n2": ["bad"]}, None
            )

    assert _calls(pbs, "qsub") == []
