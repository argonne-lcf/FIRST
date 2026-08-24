import subprocess
from pathlib import Path
from subprocess import CompletedProcess
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import yaml

from first_common.schema.pilot import (
    GpuInfo,
    HostGpus,
    PilotResources,
    PilotRuntimeConfig,
)
from first_common.schema.types import PalsDiscovery, SSHDiscovery
from first_pilot.replica_manager import (
    ReplicaManager,
    discover_hosts,
    query_gpus_local,
    query_gpus_pals,
    query_gpus_ssh,
)


def _gpu_rows(hostname: str, rank: int, indices: tuple[int, ...]) -> str:
    return "".join(
        f"{hostname} {rank}: {index}, Test GPU, 97871, {index}\n" for index in indices
    )


def test_scheduler_nodefile_inventory_does_not_resolve_dns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    nodefile = tmp_path / "nodes"
    nodefile.write_text("head.example.test\nworker.example.test\n")
    monkeypatch.setenv("TEST_NODEFILE", str(nodefile))
    with patch(
        "first_pilot.replica_manager.socket.getfqdn",
        side_effect=AssertionError("DNS must not be called"),
    ) as getfqdn:
        assert discover_hosts("TEST_NODEFILE") == [
            "head.example.test",
            "worker.example.test",
        ]
        getfqdn.assert_not_called()


@patch("first_pilot.replica_manager.subprocess.run")
def test_single_host_inventory_runs_nvidia_smi_locally(
    run: MagicMock,
) -> None:
    run.return_value = CompletedProcess(
        [],
        0,
        stdout="".join(f"{index}, Test GPU, 97871, {index}\n" for index in range(4)),
        stderr="",
    )

    resources = query_gpus_local("head.example.test", 4)

    assert [gpu.index for gpu in resources.gpus] == ["0", "1", "2", "3"]
    command = run.call_args.args[0]
    assert command[0] == "nvidia-smi"
    assert "ssh" not in command
    assert run.call_args.kwargs["timeout"] == 5.0


@patch("first_pilot.replica_manager.subprocess.run")
def test_ssh_inventory_queries_every_host_and_preserves_scheduler_order(
    run: MagicMock,
) -> None:
    def response(command: list[str], **_kwargs: object) -> CompletedProcess[str]:
        hostname = command[1]
        rows = "".join(
            f"{index}, GPU on {hostname}, 100, {index}\n" for index in range(2)
        )
        return CompletedProcess(command, 0, stdout=rows, stderr="")

    run.side_effect = response
    hostnames = ["worker-b.example.test", "worker-a.example.test"]

    resources = query_gpus_ssh(hostnames, expected_gpus=2, timeout_sec=7.0)

    assert [host.hostname for host in resources] == hostnames
    assert [[gpu.index for gpu in host.gpus] for host in resources] == [
        ["0", "1"],
        ["0", "1"],
    ]
    assert {call.args[0][1] for call in run.call_args_list} == set(hostnames)
    assert all(call.kwargs["timeout"] == 7.0 for call in run.call_args_list)
    assert all(call.args[0][0] == "ssh" for call in run.call_args_list)


@patch(
    "first_pilot.replica_manager.shutil.which",
    return_value="/usr/bin/nvidia-smi",
)
@patch(
    "first_pilot.replica_manager._require_executable",
    return_value="/opt/test/mpiexec",
)
@patch("first_pilot.replica_manager.subprocess.run")
def test_pals_inventory_validates_and_preserves_rank_order(
    run: MagicMock, _executable: MagicMock, which: MagicMock
) -> None:
    run.return_value = CompletedProcess(
        [],
        0,
        stdout=(
            _gpu_rows("worker", 1, (3, 2, 1, 0)) + _gpu_rows("head", 0, (2, 0, 3, 1))
        ),
        stderr="",
    )

    resources = query_gpus_pals(
        ["head.example.test", "worker.example.test"],
        Path("/opt/test/mpiexec"),
        4,
    )

    assert [host.hostname for host in resources] == [
        "head.example.test",
        "worker.example.test",
    ]
    assert [[gpu.index for gpu in host.gpus] for host in resources] == [
        ["0", "1", "2", "3"],
        ["0", "1", "2", "3"],
    ]
    command = run.call_args.args[0]
    assert command[0] == "/opt/test/mpiexec"
    assert command[command.index("--cpu-bind=none") + 1] == "/usr/bin/nvidia-smi"
    assert "ssh" not in command
    assert command[command.index("--timeout") + 1] == "30"
    assert command[command.index("-n") + 1] == "2"
    assert command[command.index("--ppn") + 1] == "1"
    assert run.call_args.kwargs["timeout"] == 35.0
    which.assert_called_once_with("nvidia-smi")


@patch(
    "first_pilot.replica_manager._require_executable",
    return_value="/opt/test/mpiexec",
)
@patch("first_pilot.replica_manager.subprocess.run")
@patch("first_pilot.replica_manager.shutil.which", return_value=None)
def test_pals_inventory_reports_missing_nvidia_smi(
    _which: MagicMock, run: MagicMock, _executable: MagicMock
) -> None:
    with pytest.raises(
        RuntimeError, match="nvidia-smi is unavailable on the pilot PATH"
    ):
        query_gpus_pals(["head", "worker"], Path("/opt/test/mpiexec"), 4)

    run.assert_not_called()


@pytest.mark.parametrize(
    ("output", "message"),
    [
        ("not-labeled\n", "unlabeled row"),
        (
            "head 0: 0, Test GPU, unknown, 0\n",
            "was malformed",
        ),
        (
            _gpu_rows("worker", 0, (0, 1, 2, 3)) + _gpu_rows("head", 1, (0, 1, 2, 3)),
            "rank 0 reported host",
        ),
        (
            _gpu_rows("head", 0, (0, 1, 2, 3)) + _gpu_rows("worker", 2, (0, 1, 2, 3)),
            "unexpected rank 2",
        ),
    ],
)
@patch(
    "first_pilot.replica_manager.shutil.which",
    return_value="/usr/bin/nvidia-smi",
)
@patch(
    "first_pilot.replica_manager._require_executable",
    return_value="/opt/test/mpiexec",
)
@patch("first_pilot.replica_manager.subprocess.run")
def test_pals_inventory_rejects_malformed_host_and_rank_rows(
    run: MagicMock,
    _executable: MagicMock,
    _which: MagicMock,
    output: str,
    message: str,
) -> None:
    run.return_value = CompletedProcess([], 0, stdout=output, stderr="")

    with pytest.raises(RuntimeError, match=message):
        query_gpus_pals(["head", "worker"], Path("/opt/test/mpiexec"), 4)


@patch(
    "first_pilot.replica_manager.shutil.which",
    return_value="/usr/bin/nvidia-smi",
)
@patch(
    "first_pilot.replica_manager._require_executable",
    return_value="/opt/test/mpiexec",
)
@patch("first_pilot.replica_manager.subprocess.run")
def test_pals_inventory_rejects_incomplete_rank(
    run: MagicMock, _executable: MagicMock, _which: MagicMock
) -> None:
    run.return_value = CompletedProcess(
        [], 0, stdout=_gpu_rows("head", 0, (0, 1, 2, 3)), stderr=""
    )

    with pytest.raises(RuntimeError, match=r"incomplete; missing ranks \[1\]"):
        query_gpus_pals(["head", "worker"], Path("/opt/test/mpiexec"), 4)


@patch(
    "first_pilot.replica_manager.shutil.which",
    return_value="/usr/bin/nvidia-smi",
)
@patch(
    "first_pilot.replica_manager._require_executable",
    return_value="/opt/test/mpiexec",
)
@patch("first_pilot.replica_manager.subprocess.run")
def test_pals_inventory_rejects_duplicate_gpu(
    run: MagicMock, _executable: MagicMock, _which: MagicMock
) -> None:
    run.return_value = CompletedProcess(
        [],
        0,
        stdout=(
            _gpu_rows("head", 0, (0, 1, 2, 2)) + _gpu_rows("worker", 1, (0, 1, 2, 3))
        ),
        stderr="",
    )

    with pytest.raises(RuntimeError, match="duplicated an index on host 'head'"):
        query_gpus_pals(["head", "worker"], Path("/opt/test/mpiexec"), 4)


@patch(
    "first_pilot.replica_manager.shutil.which",
    return_value="/usr/bin/nvidia-smi",
)
@patch(
    "first_pilot.replica_manager._require_executable",
    return_value="/opt/test/mpiexec",
)
@patch("first_pilot.replica_manager.subprocess.run")
def test_pals_inventory_reports_launcher_failure(
    run: MagicMock, _executable: MagicMock, _which: MagicMock
) -> None:
    run.return_value = CompletedProcess(
        [], 127, stdout="", stderr="launcher unavailable"
    )

    with pytest.raises(RuntimeError, match="exited 127: launcher unavailable"):
        query_gpus_pals(["head", "worker"], Path("/opt/test/mpiexec"), 4)


@patch(
    "first_pilot.replica_manager.shutil.which",
    return_value="/usr/bin/nvidia-smi",
)
@patch(
    "first_pilot.replica_manager._require_executable",
    return_value="/opt/test/mpiexec",
)
@patch("first_pilot.replica_manager.subprocess.run")
def test_pals_inventory_reports_launcher_timeout(
    run: MagicMock, _executable: MagicMock, _which: MagicMock
) -> None:
    run.side_effect = subprocess.TimeoutExpired("mpiexec", 35)

    with pytest.raises(RuntimeError, match="timed out after 35s"):
        query_gpus_pals(["head", "worker"], Path("/opt/test/mpiexec"), 4)


@patch("first_pilot.replica_manager.discover_hosts")
def test_replica_manager_deduplicates_hosts_in_scheduler_order(
    discover: MagicMock,
) -> None:
    discover.return_value = [
        "Head.Example.Test",
        "head",
        "HEAD.example.test.",
        "worker",
        "worker.example.test",
    ]
    resources = PilotResources(
        hosts=[
            HostGpus(
                hostname=hostname,
                gpus=[
                    GpuInfo(
                        index="0",
                        name="GPU",
                        memory_total_mib=1,
                        memory_used_mib=0,
                    )
                ],
            )
            for hostname in ("head", "worker")
        ]
    )
    config = SimpleNamespace(
        node_file_env="TEST_NODEFILE",
        gpu_discovery=PalsDiscovery(launcher_path=Path("/opt/test/mpiexec")),
        num_nodes=2,
        gpus_per_node=1,
    )

    with patch.object(ReplicaManager, "query_resources", return_value=resources):
        manager = ReplicaManager(config)  # type: ignore[arg-type]

    assert manager.node_hostnames == ["Head.Example.Test", "worker"]


@patch("first_pilot.replica_manager.discover_hosts", return_value=["head"])
def test_replica_manager_rejects_incomplete_host_inventory(
    _discover: MagicMock,
) -> None:
    config = SimpleNamespace(
        node_file_env="TEST_NODEFILE",
        gpu_discovery=SSHDiscovery(),
        num_nodes=2,
        gpus_per_node=4,
    )

    with pytest.raises(RuntimeError, match="discovered 1, expected 2"):
        ReplicaManager(config)  # type: ignore[arg-type]


def _host_resources(hostnames: list[str], gpu_count: int) -> list[HostGpus]:
    return [
        HostGpus(
            hostname=hostname,
            gpus=[
                GpuInfo(
                    index=str(index),
                    name="Test GPU",
                    memory_total_mib=100,
                    memory_used_mib=0,
                )
                for index in range(gpu_count)
            ],
        )
        for hostname in hostnames
    ]


def test_replica_manager_dispatches_multi_node_ssh_discovery() -> None:
    hostnames = ["head", "worker"]
    config = SimpleNamespace(
        node_file_env="TEST_NODEFILE",
        gpu_discovery=SSHDiscovery(timeout_sec=7.0),
        num_nodes=2,
        gpus_per_node=2,
    )
    with (
        patch("first_pilot.replica_manager.discover_hosts", return_value=hostnames),
        patch(
            "first_pilot.replica_manager.query_gpus_ssh",
            return_value=_host_resources(hostnames, 2),
        ) as query_ssh,
    ):
        manager = ReplicaManager(config)  # type: ignore[arg-type]

    query_ssh.assert_called_once_with(hostnames, 2, 7.0)
    manager._socket_dir.cleanup()


def test_replica_manager_dispatches_multi_node_pals_discovery() -> None:
    hostnames = ["head", "worker"]
    discovery = PalsDiscovery(launcher_path=Path("/opt/test/mpiexec"), timeout_sec=41.0)
    config = SimpleNamespace(
        node_file_env="TEST_NODEFILE",
        gpu_discovery=discovery,
        num_nodes=2,
        gpus_per_node=2,
    )
    with (
        patch("first_pilot.replica_manager.discover_hosts", return_value=hostnames),
        patch(
            "first_pilot.replica_manager.query_gpus_pals",
            return_value=_host_resources(hostnames, 2),
        ) as query_pals,
    ):
        manager = ReplicaManager(config)  # type: ignore[arg-type]

    query_pals.assert_called_once_with(hostnames, discovery.launcher_path, 2, 41.0)
    manager._socket_dir.cleanup()


def test_runtime_config_loads_public_fields_from_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "pilot.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "ca_crt": "ca",
                "server_crt": "crt",
                "server_key": "key",
            }
        )
    )
    monkeypatch.setenv("PILOT_CONFIG_FILE", str(config_path))
    monkeypatch.setenv("PILOT_JOB_NAME", "test-pilot")
    monkeypatch.setenv("PILOT_EXTERNAL_PORT", "19443")
    monkeypatch.setenv("PILOT_NGINX_PATH", "/opt/test/nginx")
    monkeypatch.setenv("PILOT_IP_ALLOWLIST", '["192.0.2.10/32"]')
    monkeypatch.setenv("PILOT_WORKDIR", "/opt/test/workdir")
    monkeypatch.setenv("PILOT_NODE_FILE_ENV", "TEST_NODEFILE")
    monkeypatch.setenv(
        "PILOT_GPU_DISCOVERY",
        '{"method":"pals","launcher_path":"/opt/test/mpiexec"}',
    )
    monkeypatch.setenv("PILOT_NUM_NODES", "2")
    monkeypatch.setenv("PILOT_GPUS_PER_NODE", "4")

    config = PilotRuntimeConfig.load()

    assert config.job_name == "test-pilot"
    assert config.external_port == 19443
    assert config.nginx_path == Path("/opt/test/nginx")
    assert config.ip_allowlist == ["192.0.2.10/32"]
    assert config.workdir == Path("/opt/test/workdir")
    assert config.node_file_env == "TEST_NODEFILE"
    assert config.gpu_discovery == PalsDiscovery(
        launcher_path=Path("/opt/test/mpiexec")
    )
    assert config.num_nodes == 2
    assert config.gpus_per_node == 4
