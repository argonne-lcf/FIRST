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
from first_pilot.replica_manager import ReplicaManager, query_gpus, query_gpus_pals


def _gpu_rows(hostname: str, rank: int, indices: tuple[int, ...]) -> str:
    return "".join(
        f"{hostname} {rank}: {index}, NVIDIA GH200, 97871, {index}\n"
        for index in indices
    )


@patch("first_pilot.replica_manager.subprocess.run")
def test_single_host_inventory_runs_nvidia_smi_locally(run: MagicMock) -> None:
    run.return_value = CompletedProcess(
        [],
        0,
        stdout="".join(
            f"{index}, NVIDIA GH200, 97871, {index}\n" for index in range(4)
        ),
        stderr="",
    )

    resources = query_gpus("head.example.test", 4)

    assert [gpu.index for gpu in resources.gpus] == ["0", "1", "2", "3"]
    command = run.call_args.args[0]
    assert command[0] == "nvidia-smi"
    assert "ssh" not in command


@patch("first_pilot.replica_manager.shutil.which", return_value="/usr/bin/nvidia-smi")
@patch("first_pilot.replica_manager.subprocess.run")
def test_pals_inventory_validates_and_preserves_rank_order(
    run: MagicMock, _which: MagicMock
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
        Path("/opt/site/mpiexec"),
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
    assert command[0] == "/opt/site/mpiexec"
    assert "ssh" not in command
    assert command[command.index("-n") + 1] == "2"
    assert command[command.index("--ppn") + 1] == "1"


@pytest.mark.parametrize(
    ("output", "message"),
    [
        ("not-labeled\n", "unlabeled row"),
        (
            "head 0: 0, NVIDIA GH200, unknown, 0\n",
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
@patch("first_pilot.replica_manager.shutil.which", return_value="/usr/bin/nvidia-smi")
@patch("first_pilot.replica_manager.subprocess.run")
def test_pals_inventory_rejects_malformed_host_and_rank_rows(
    run: MagicMock,
    _which: MagicMock,
    output: str,
    message: str,
) -> None:
    run.return_value = CompletedProcess([], 0, stdout=output, stderr="")

    with pytest.raises(RuntimeError, match=message):
        query_gpus_pals(["head", "worker"], Path("/opt/site/mpiexec"), 4)


@patch("first_pilot.replica_manager.shutil.which", return_value="/usr/bin/nvidia-smi")
@patch("first_pilot.replica_manager.subprocess.run")
def test_pals_inventory_rejects_incomplete_rank(
    run: MagicMock, _which: MagicMock
) -> None:
    run.return_value = CompletedProcess(
        [], 0, stdout=_gpu_rows("head", 0, (0, 1, 2, 3)), stderr=""
    )

    with pytest.raises(RuntimeError, match=r"incomplete; missing ranks \[1\]"):
        query_gpus_pals(["head", "worker"], Path("/opt/site/mpiexec"), 4)


@patch("first_pilot.replica_manager.shutil.which", return_value="/usr/bin/nvidia-smi")
@patch("first_pilot.replica_manager.subprocess.run")
def test_pals_inventory_rejects_duplicate_gpu(
    run: MagicMock, _which: MagicMock
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
        query_gpus_pals(["head", "worker"], Path("/opt/site/mpiexec"), 4)


@patch("first_pilot.replica_manager.shutil.which", return_value="/usr/bin/nvidia-smi")
@patch("first_pilot.replica_manager.subprocess.run")
def test_pals_inventory_reports_launcher_failure(
    run: MagicMock, _which: MagicMock
) -> None:
    run.return_value = CompletedProcess(
        [], 127, stdout="", stderr="CXI endpoint unavailable"
    )

    with pytest.raises(RuntimeError, match="exited 127: CXI endpoint unavailable"):
        query_gpus_pals(["head", "worker"], Path("/opt/site/mpiexec"), 4)


@patch("first_pilot.replica_manager.shutil.which", return_value="/usr/bin/nvidia-smi")
@patch("first_pilot.replica_manager.subprocess.run")
def test_pals_inventory_reports_launcher_timeout(
    run: MagicMock, _which: MagicMock
) -> None:
    run.side_effect = subprocess.TimeoutExpired("mpiexec", 35)

    with pytest.raises(RuntimeError, match="timed out after 35s"):
        query_gpus_pals(["head", "worker"], Path("/opt/site/mpiexec"), 4)


@patch("first_pilot.replica_manager.discover_hosts")
def test_replica_manager_deduplicates_hosts_in_scheduler_order(
    discover: MagicMock,
) -> None:
    discover.return_value = ["head", "head", "worker", "worker"]
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
        pals_path=Path("/opt/site/mpiexec"),
        num_nodes=2,
        gpus_per_node=1,
    )

    with patch.object(ReplicaManager, "query_resources", return_value=resources):
        manager = ReplicaManager(config)  # type: ignore[arg-type]

    assert manager.node_hostnames == ["head", "worker"]


@patch("first_pilot.replica_manager.discover_hosts", return_value=["head"])
def test_replica_manager_rejects_incomplete_host_inventory(
    _discover: MagicMock,
) -> None:
    config = SimpleNamespace(
        node_file_env="TEST_NODEFILE",
        pals_path=Path("/opt/site/mpiexec"),
        num_nodes=2,
        gpus_per_node=4,
    )

    with pytest.raises(RuntimeError, match="discovered 1, expected 2"):
        ReplicaManager(config)  # type: ignore[arg-type]


def test_runtime_config_load_overrides_allocation_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "pilot.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "ca_crt": "ca",
                "server_crt": "crt",
                "server_key": "key",
                "external_port": 18443,
                "nginx_path": "/service/nginx",
                "ip_allowlist": ["10.124.176.33/32"],
                "workdir": "/service/workdir",
                "node_file_env": "PBS_NODEFILE",
                "pals_path": "/stale/mpiexec",
                "num_nodes": 1,
                "gpus_per_node": 1,
                "job_name": "stale-job",
            }
        )
    )
    monkeypatch.setenv("PILOT_CONFIG_FILE", str(config_path))
    monkeypatch.setenv("PILOT_JOB_NAME", "nemotron-canary")
    monkeypatch.setenv("PILOT_PALS_PATH", "/opt/site/mpiexec")
    monkeypatch.setenv("PILOT_NUM_NODES", "2")
    monkeypatch.setenv("PILOT_GPUS_PER_NODE", "4")

    config = PilotRuntimeConfig.load()

    assert config.job_name == "nemotron-canary"
    assert config.pals_path == Path("/opt/site/mpiexec")
    assert config.num_nodes == 2
    assert config.gpus_per_node == 4
