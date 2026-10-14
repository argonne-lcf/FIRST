"""A missing observation must never become an automatic hardware diagnosis."""

import socket
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from first_common.schema.pilot import PilotRuntimeConfig
from first_common.schema.types import PalsDiscovery, SSHDiscovery
from first_pilot.host_commands import run_on_hosts
from first_pilot.node_checks import (
    CHECKS,
    GPU_QUERY,
    gpu_rows,
    node_check,
    run_node_checks,
)
from first_pilot.replica_manager import discover_hosts

GPU0 = "0, GPU-00000000-0000-0000-0000-000000000000, 580.65.06, 97871, 97800, 0, 0"
GPU1 = "1, GPU-11111111-1111-1111-1111-111111111111, 580.65.06, 97871, 97800, 0, 0"
HEALTHY = GPU0 + "\n" + GPU1
LOW = HEALTHY.replace("97800", "93233")  # ceil(97871*.95)+256 == 93234
MARKER = "FIRST_HOST_COMMAND_testnonce"


def config(tmp_path: Path) -> PilotRuntimeConfig:
    return PilotRuntimeConfig.model_validate(
        {
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
    )


@pytest.mark.parametrize(
    "output",
    [
        "",
        "\n",
        "nvidia-smi failed",
        GPU0 + "\n" + GPU0,
        GPU0
        + "\n"
        + GPU1.replace(
            "GPU-11111111-1111-1111-1111-111111111111",
            "GPU-00000000-0000-0000-0000-000000000000",
        ),
        GPU0.replace("97871, 97800", "0, 0"),
        GPU0.replace("97871, 97800", "97871, 99999"),
        GPU0.replace(", 0, 0", ", [N/A], 0"),
        GPU0.replace("GPU-00000000-0000-0000-0000-000000000000", ""),
        GPU0.replace("580.65.06", ""),
        GPU0 + "\n" + GPU1.replace("580.65.06", "580.65.07"),
        GPU0.replace("97871", "-1"),
        GPU0 + ", unexpected",
    ],
)
def test_malformed_or_empty_gpu_observations_are_infrastructure_errors(
    output: str,
) -> None:
    with pytest.raises(RuntimeError, match="malformed GPU observation"):
        gpu_rows(output)


@patch("first_pilot.node_checks.time.sleep")
@patch("first_pilot.node_checks.run_on_hosts")
def test_healthy_gpu_checks_share_one_complete_snapshot(
    run: MagicMock, sleep: MagicMock, tmp_path: Path
) -> None:
    run.side_effect = [{"n1": HEALTHY}, {"n1": ""}]
    evidence: dict[str, object] = {}
    assert (
        run_node_checks(
            ["gpu_count", "gpu_memory_free", "gpu_ecc_errors"],
            ["n1"],
            config(tmp_path),
            evidence=evidence,
        )
        == {}
    )
    assert run.call_count == 2
    assert run.call_args_list[0].args[0] == GPU_QUERY
    assert all(call.kwargs["require_completion"] for call in run.call_args_list)
    sleep.assert_not_called()
    assert evidence["n1"]["reserve_mib"] == 256  # type: ignore[index]
    assert len(evidence["n1"]["samples"]) == 1  # type: ignore[index]


@patch("first_pilot.node_checks.time.sleep")
@patch("first_pilot.node_checks.run_on_hosts")
def test_verified_partial_count_twice_is_a_node_fault(
    run: MagicMock, _sleep: MagicMock, tmp_path: Path
) -> None:
    run.return_value = {"n1": GPU0}
    assert run_node_checks(["gpu_count"], ["n1"], config(tmp_path)) == {
        "n1": ["gpu_count: found 1 GPUs, expected 2 in both observations"]
    }
    assert run.call_count == 2


@patch("first_pilot.node_checks.time.sleep")
@patch("first_pilot.node_checks.run_on_hosts")
def test_verified_unexplained_low_memory_twice_uses_established_threshold(
    run: MagicMock, _sleep: MagicMock, tmp_path: Path
) -> None:
    run.side_effect = [{"n1": LOW}, {"n1": ""}, {"n1": LOW}, {"n1": ""}]
    reasons = run_node_checks(["gpu_memory_free"], ["n1"], config(tmp_path))["n1"]
    assert len(reasons) == 2
    assert all(
        "requires 93234 of 97871 MiB in both observations with no GPU clients" in reason
        for reason in reasons
    )


@patch("first_pilot.node_checks.time.sleep")
@patch("first_pilot.node_checks.run_on_hosts")
def test_fault_must_persist_on_the_same_gpu(
    run: MagicMock, _sleep: MagicMock, tmp_path: Path
) -> None:
    first = GPU0.replace("97800", "93233") + "\n" + GPU1
    second = GPU0 + "\n" + GPU1.replace("97800", "93233")
    run.side_effect = [{"n1": first}, {"n1": ""}, {"n1": second}, {"n1": ""}]
    with pytest.raises(RuntimeError, match="no persistent fault established"):
        run_node_checks(["gpu_memory_free"], ["n1"], config(tmp_path))


@patch("first_pilot.node_checks.time.sleep")
@patch("first_pilot.node_checks.run_on_hosts")
def test_gpu_rows_are_parsed_once_per_snapshot_not_once_per_check(
    run: MagicMock, _sleep: MagicMock, tmp_path: Path
) -> None:
    run.side_effect = [{"n1": LOW}, {"n1": ""}, {"n1": LOW}, {"n1": ""}]
    with patch("first_pilot.node_checks.gpu_rows", wraps=gpu_rows) as parse:
        failures = run_node_checks(
            ["gpu_count", "gpu_memory_free", "gpu_ecc_errors"], ["n1"], config(tmp_path)
        )
    assert failures
    assert parse.call_count == 2


def test_nonempty_identity_fields_do_not_require_vendor_specific_format() -> None:
    rows = gpu_rows(
        GPU0.replace("GPU-00000000-0000-0000-0000-000000000000", "device-A").replace(
            "580.65.06", "vendor-driver"
        )
    )
    assert rows[0].uuid == "device-a"
    assert rows[0].driver == "vendor-driver"


@pytest.mark.parametrize("kind", ["unset", "missing", "empty", "unreadable"])
def test_configured_pbs_nodefile_does_not_fall_back_to_localhost(
    kind: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    nodefile = tmp_path / "PBS_NODEFILE"
    monkeypatch.delenv("PBS_NODEFILE", raising=False)
    if kind != "unset":
        monkeypatch.setenv("PBS_NODEFILE", str(nodefile))
    if kind in {"empty", "unreadable"}:
        nodefile.write_text(" \n")
    with patch(
        "first_pilot.replica_manager.socket.gethostname",
        side_effect=AssertionError("must not fall back"),
    ):
        if kind == "unreadable":
            with patch("builtins.open", side_effect=PermissionError("denied")):
                with pytest.raises(
                    RuntimeError, match="configured allocation node file"
                ):
                    discover_hosts("PBS_NODEFILE")
        else:
            with pytest.raises(RuntimeError, match="configured allocation node file"):
                discover_hosts("PBS_NODEFILE")


def test_explicitly_local_discovery_remains_available() -> None:
    with patch("first_pilot.replica_manager.socket.gethostname", return_value="local"):
        assert discover_hosts("") == ["local"]


@patch("first_pilot.node_checks.time.sleep")
@patch("first_pilot.node_checks.run_on_hosts")
def test_memory_that_recovers_on_confirmation_is_not_a_hardware_fault(
    run: MagicMock, _sleep: MagicMock, tmp_path: Path
) -> None:
    run.side_effect = [{"n1": LOW}, {"n1": ""}, {"n1": HEALTHY}, {"n1": ""}]
    with pytest.raises(RuntimeError, match="no persistent fault established"):
        run_node_checks(["gpu_memory_free"], ["n1"], config(tmp_path))


@pytest.mark.parametrize("sample", [0, 1])
@patch("first_pilot.node_checks.time.sleep")
@patch("first_pilot.node_checks.run_on_hosts")
def test_legitimate_clients_prevent_unexplained_memory_fault(
    run: MagicMock, _sleep: MagicMock, tmp_path: Path, sample: int
) -> None:
    processes = "GPU-00000000-0000-0000-0000-000000000000, 123, model-server, 8192"
    reads = [{"n1": LOW}, {"n1": ""}, {"n1": LOW}, {"n1": ""}]
    reads[1 + sample * 2] = {"n1": processes}
    run.side_effect = reads
    with pytest.raises(RuntimeError, match="GPU clients present"):
        run_node_checks(["gpu_memory_free"], ["n1"], config(tmp_path))


@pytest.mark.parametrize(
    "second",
    [
        GPU1,
        GPU0.replace("580.65.06", "580.65.07"),
        GPU0.replace("97871", "97872"),
    ],
)
@patch("first_pilot.node_checks.time.sleep")
@patch("first_pilot.node_checks.run_on_hosts")
def test_changed_identity_is_infrastructure_error(
    run: MagicMock, _sleep: MagicMock, tmp_path: Path, second: str
) -> None:
    run.side_effect = [{"n1": GPU0}, {"n1": second}]
    with pytest.raises(RuntimeError, match="GPU identity changed"):
        run_node_checks(["gpu_count"], ["n1"], config(tmp_path))


@patch(
    "first_pilot.node_checks.run_on_hosts", side_effect=RuntimeError("command failed")
)
def test_command_failure_never_returns_node_faults(
    _run: MagicMock, tmp_path: Path
) -> None:
    with pytest.raises(RuntimeError, match="command failed"):
        run_node_checks(["gpu_count"], ["n1"], config(tmp_path))


@patch("first_pilot.node_checks.time.monotonic", side_effect=[0.0, 31.0])
@patch("first_pilot.node_checks.time.sleep")
@patch("first_pilot.node_checks.run_on_hosts", return_value={"n1": GPU0})
def test_unbounded_sample_interval_does_not_establish_fault(
    _run: MagicMock, _sleep: MagicMock, _clock: MagicMock, tmp_path: Path
) -> None:
    with pytest.raises(RuntimeError, match="30s coherence"):
        run_node_checks(["gpu_count"], ["n1"], config(tmp_path))


@patch("first_pilot.node_checks.run_on_hosts", return_value={"n1": "healthy"})
def test_custom_registry_checks_share_one_complete_command(
    run: MagicMock, tmp_path: Path
) -> None:
    with patch.dict(CHECKS):

        @node_check("custom_a", command=["uname", "-r"])
        def custom_a(output: str, _: PilotRuntimeConfig) -> str | None:
            return None if output == "healthy" else "bad"

        @node_check("custom_b", command=["uname", "-r"])
        def custom_b(output: str, _: PilotRuntimeConfig) -> str | None:
            return None

        assert run_node_checks(["custom_a", "custom_b"], ["n1"], config(tmp_path)) == {}
    run.assert_called_once_with(
        ["uname", "-r"], ["n1"], config(tmp_path).gpu_discovery, require_completion=True
    )


@patch("first_pilot.node_checks.time.sleep")
@patch("first_pilot.node_checks.run_on_hosts", return_value={"n1": HEALTHY})
def test_custom_gpu_query_reuses_verified_builtin_snapshot(
    run: MagicMock, _sleep: MagicMock, tmp_path: Path
) -> None:
    with patch.dict(CHECKS):

        @node_check("custom_gpu", command=GPU_QUERY)
        def custom_gpu(output: str, _: PilotRuntimeConfig) -> str | None:
            assert output == HEALTHY
            return None

        assert (
            run_node_checks(["gpu_count", "custom_gpu"], ["n1"], config(tmp_path)) == {}
        )
    assert run.call_count == 1


@patch(
    "first_pilot.node_checks.run_on_hosts", return_value={"n1": HEALTHY, "n2": HEALTHY}
)
def test_gpu_uuid_cannot_belong_to_multiple_physical_hosts(
    run: MagicMock, tmp_path: Path
) -> None:
    with pytest.raises(RuntimeError, match="multiple physical hosts"):
        run_node_checks(["gpu_count"], ["n1", "n2"], config(tmp_path))
    run.assert_called_once()


@patch("first_pilot.node_checks.time.sleep")
@patch("first_pilot.node_checks.run_on_hosts")
def test_verified_ecc_twice_is_a_fault(
    run: MagicMock, _sleep: MagicMock, tmp_path: Path
) -> None:
    run.return_value = {"n1": HEALTHY.replace(", 0, 0", ", 0, 2")}
    reasons = run_node_checks(["gpu_ecc_errors"], ["n1"], config(tmp_path))["n1"]
    assert len(reasons) == 2
    assert all(
        "uncorrected ECC errors in both observations" in reason for reason in reasons
    )


def test_actual_native_empty_command_has_completion_and_preserves_empty_output() -> (
    None
):
    host = socket.gethostname()
    assert run_on_hosts(
        ["/usr/bin/printf", ""], [host], SSHDiscovery(), require_completion=True
    ) == {host: ""}


@pytest.mark.parametrize(
    "output",
    [
        "",
        "n1 0: hello\n",
        f"n1 0: {MARKER}:n1:0\n",
        f"n1 0: {MARKER}:n1:0\nn2 1: {MARKER}:n2:9\n",
        f"n1 0: {MARKER}:n1:0\nn2 1: {MARKER}:unrelated:0\n",
    ],
)
@patch(
    "first_pilot.host_commands.uuid.uuid4",
    return_value=SimpleNamespace(hex="testnonce"),
)
@patch(
    "first_pilot.host_commands.shutil.which",
    side_effect=lambda name: name if name.startswith("/") else "/usr/bin/" + name,
)
@patch(
    "first_pilot.host_commands._require_executable", return_value="/opt/test/mpiexec"
)
@patch("first_pilot.host_commands.subprocess.run")
def test_pals_incomplete_ranks_or_failed_commands_are_infrastructure_errors(
    run: MagicMock,
    _require: MagicMock,
    _which: MagicMock,
    _uuid: MagicMock,
    output: str,
) -> None:
    run.return_value = subprocess.CompletedProcess([], 0, stdout=output, stderr="")
    with pytest.raises(RuntimeError, match="command completion"):
        run_on_hosts(
            ["nvidia-smi"],
            ["n1", "n2"],
            PalsDiscovery(launcher_path=Path("/opt/test/mpiexec")),
            require_completion=True,
        )


@patch(
    "first_pilot.host_commands.uuid.uuid4",
    return_value=SimpleNamespace(hex="testnonce"),
)
@patch(
    "first_pilot.host_commands.shutil.which",
    side_effect=lambda name: name if name.startswith("/") else "/usr/bin/" + name,
)
@patch(
    "first_pilot.host_commands._require_executable", return_value="/opt/test/mpiexec"
)
@patch("first_pilot.host_commands.subprocess.run")
def test_pals_successful_empty_process_queries_are_distinguished_from_missing_ranks(
    run: MagicMock, _require: MagicMock, _which: MagicMock, _uuid: MagicMock
) -> None:
    run.return_value = subprocess.CompletedProcess(
        [],
        0,
        stdout=f"n2 1: \nn2 1: {MARKER}:n2:0\nn1 0: \nn1 0: {MARKER}:n1:0\n",
        stderr="",
    )
    assert run_on_hosts(
        ["nvidia-smi"],
        ["n1", "n2"],
        PalsDiscovery(launcher_path=Path("/opt/test/mpiexec")),
        require_completion=True,
    ) == {"n1": "", "n2": ""}
    command = run.call_args.args[0]
    assert "/usr/bin/nvidia-smi" in command
    assert "--genvnone" in command
    assert "--host" not in command


@patch("first_pilot.host_commands.socket.gethostname", return_value="actual-local")
@patch(
    "first_pilot.host_commands.uuid.uuid4",
    return_value=SimpleNamespace(hex="testnonce"),
)
@patch("first_pilot.host_commands.subprocess.run")
def test_single_remote_host_is_not_misattributed_to_local_gpus(
    run: MagicMock, _uuid: MagicMock, _hostname: MagicMock
) -> None:
    run.return_value = subprocess.CompletedProcess(
        [], 0, stdout=f"\n{MARKER}:remote:0\n", stderr=""
    )
    assert run_on_hosts(
        ["/bin/printf", ""], ["remote"], SSHDiscovery(), require_completion=True
    ) == {"remote": ""}
    assert run.call_args.args[0][0:2] == ["ssh", "remote"]
