import subprocess
from datetime import datetime, timezone
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
from first_pilot.gpu_inventory import query_gpus
from first_pilot.replica_manager import ReplicaManager, discover_hosts

from .fixtures.nvidia_smi import gpu_row

PALS = PalsDiscovery(launcher_path=Path("/opt/test/mpiexec"))
MARKER = "FIRST_HOST_COMMAND_testnonce"

# Every inventory run proves each host's command completed with this marker.
_testnonce = patch(
    "first_pilot.host_commands.uuid.uuid4",
    return_value=SimpleNamespace(hex="testnonce"),
)
_which = patch(
    "first_pilot.host_commands.shutil.which",
    side_effect=lambda name: name if name.startswith("/") else "/usr/bin/" + name,
)
_mpiexec = patch(
    "first_pilot.host_commands._require_executable",
    return_value="/opt/test/mpiexec",
)


def _completed(hostname: str, indices: range | tuple[int, ...]) -> str:
    rows = [gpu_row(index, host=hostname) for index in indices]
    return "\n".join([*rows, f"{MARKER}:{hostname}:0"]) + "\n"


def _pals_rows(hostname: str, rank: int, indices: tuple[int, ...]) -> str:
    return "".join(
        f"{hostname} {rank}: {row}\n"
        for row in _completed(hostname, indices).splitlines()
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


@_testnonce
@patch("first_pilot.host_commands.socket.gethostname", return_value="head")
@patch("first_pilot.host_commands.subprocess.run")
def test_single_host_inventory_runs_nvidia_smi_locally(
    run: MagicMock, _hostname: MagicMock, _uuid: MagicMock
) -> None:
    run.return_value = CompletedProcess(
        [], 0, stdout=_completed("head", (3, 1, 0, 2)), stderr=""
    )

    inventory = query_gpus(["head.example.test"], SSHDiscovery())

    assert inventory.observed_at is not None
    [resources] = inventory.hosts

    assert [gpu.index for gpu in resources.gpus] == ["0", "1", "2", "3"]
    assert resources.gpus[0] == GpuInfo(
        index="0",
        name="Test GPU",
        uuid="GPU-head-0",
        driver_version="580.82.09",
        memory_total_mib=97871,
        memory_used_mib=0,
        memory_free_mib=97800,
        ecc_uncorrected_volatile=0,
        ecc_uncorrected_aggregate=0,
        pstate="P0",
        temperature_c=35,
        utilization_gpu_pct=0,
        utilization_memory_pct=0,
        power_draw_w=70.5,
        clocks_throttle_reasons="0x0000000000000001",
    )
    command = run.call_args.args[0]
    assert "nvidia-smi" in command
    assert "ssh" not in command
    assert run.call_args.kwargs["timeout"] == 5.0


@_testnonce
@patch("first_pilot.host_commands.subprocess.run")
def test_ssh_inventory_queries_every_host_and_preserves_scheduler_order(
    run: MagicMock, _uuid: MagicMock
) -> None:
    def response(command: list[str], **_kwargs: object) -> CompletedProcess[str]:
        return CompletedProcess(
            command, 0, stdout=_completed(command[1], range(2)), stderr=""
        )

    run.side_effect = response
    hostnames = ["worker-b.example.test", "worker-a.example.test"]

    resources = query_gpus(hostnames, SSHDiscovery(timeout_sec=7.0)).hosts

    assert [host.hostname for host in resources] == hostnames
    assert [[gpu.index for gpu in host.gpus] for host in resources] == [
        ["0", "1"],
        ["0", "1"],
    ]
    assert {call.args[0][1] for call in run.call_args_list} == set(hostnames)
    assert all(call.kwargs["timeout"] == 7.0 for call in run.call_args_list)
    assert all(call.args[0][0] == "ssh" for call in run.call_args_list)


@_testnonce
@_which
@_mpiexec
@patch("first_pilot.host_commands.subprocess.run")
def test_pals_inventory_validates_and_preserves_rank_order(
    run: MagicMock, _executable: MagicMock, which: MagicMock, _uuid: MagicMock
) -> None:
    run.return_value = CompletedProcess(
        [],
        0,
        stdout=(
            _pals_rows("worker", 1, (3, 2, 1, 0)) + _pals_rows("head", 0, (2, 0, 3, 1))
        ),
        stderr="",
    )

    resources = query_gpus(["head.example.test", "worker.example.test"], PALS).hosts

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
    assert "/usr/bin/nvidia-smi" in command
    assert "ssh" not in command
    assert command[command.index("--timeout") + 1] == "30"
    assert command[command.index("-n") + 1] == "2"
    assert command[command.index("--ppn") + 1] == "1"
    assert run.call_args.kwargs["timeout"] == 35.0
    which.assert_any_call("nvidia-smi")


@_mpiexec
@patch("first_pilot.host_commands.subprocess.run")
@patch("first_pilot.host_commands.shutil.which", return_value=None)
def test_pals_inventory_reports_missing_nvidia_smi(
    _which: MagicMock, run: MagicMock, _executable: MagicMock
) -> None:
    with pytest.raises(
        RuntimeError, match="nvidia-smi is unavailable on the pilot PATH"
    ):
        query_gpus(["head", "worker"], PALS)

    run.assert_not_called()


@pytest.mark.parametrize(
    ("output", "message"),
    [
        ("not-labeled\n", "unlabeled row"),
        (
            f"head 0: 0, Test GPU, unknown\nhead 0: {MARKER}:head:0\n"
            + _pals_rows("worker", 1, (0, 1, 2, 3)),
            "malformed GPU inventory from host 'head'",
        ),
        (
            _pals_rows("worker", 0, (0, 1, 2, 3)) + _pals_rows("head", 1, (0, 1, 2, 3)),
            "rank 0 reported host",
        ),
        (
            _pals_rows("head", 0, (0, 1, 2, 3)) + _pals_rows("worker", 2, (0, 1, 2, 3)),
            "unexpected rank 2",
        ),
        (
            _pals_rows("head", 0, (0, 1, 2, 3)),
            "host 'worker' has no unique final command completion",
        ),
        (
            _pals_rows("head", 0, (0, 1, 2, 2)) + _pals_rows("worker", 1, (0, 1, 2, 3)),
            "host 'head': duplicate GPU index",
        ),
    ],
)
@_testnonce
@_which
@_mpiexec
@patch("first_pilot.host_commands.subprocess.run")
def test_pals_inventory_rejects_malformed_incomplete_or_misattributed_rows(
    run: MagicMock,
    _executable: MagicMock,
    _which: MagicMock,
    _uuid: MagicMock,
    output: str,
    message: str,
) -> None:
    run.return_value = CompletedProcess([], 0, stdout=output, stderr="")

    with pytest.raises(RuntimeError, match=message):
        query_gpus(["head", "worker"], PALS)


@_which
@_mpiexec
@patch("first_pilot.host_commands.subprocess.run")
def test_pals_inventory_reports_launcher_failure(
    run: MagicMock, _executable: MagicMock, _which: MagicMock
) -> None:
    run.return_value = CompletedProcess(
        [], 127, stdout="", stderr="launcher unavailable"
    )

    with pytest.raises(RuntimeError, match="exited 127: launcher unavailable"):
        query_gpus(["head", "worker"], PALS)


@_which
@_mpiexec
@patch("first_pilot.host_commands.subprocess.run")
def test_pals_inventory_reports_launcher_timeout(
    run: MagicMock, _executable: MagicMock, _which: MagicMock
) -> None:
    run.side_effect = subprocess.TimeoutExpired("mpiexec", 35)

    with pytest.raises(RuntimeError, match="timed out after 35s"):
        query_gpus(["head", "worker"], PALS)


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


def test_replica_manager_inventories_hosts_with_configured_discovery() -> None:
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
            "first_pilot.replica_manager.query_gpus",
            return_value=PilotResources(hosts=_host_resources(hostnames, 2)),
        ) as query,
    ):
        manager = ReplicaManager(config)  # type: ignore[arg-type]

    query.assert_called_once_with(hostnames, discovery)
    manager._socket_dir.cleanup()


@pytest.mark.parametrize("indices", [(0,), (0, 1, 2), (1, 2)])
def test_replica_manager_requires_exactly_the_requested_gpus(
    indices: tuple[int, ...],
) -> None:
    config = SimpleNamespace(
        node_file_env="TEST_NODEFILE",
        gpu_discovery=SSHDiscovery(),
        num_nodes=1,
        gpus_per_node=2,
    )
    [host] = _host_resources(["head"], 3)
    host.gpus = [host.gpus[index] for index in indices]
    with (
        patch("first_pilot.replica_manager.discover_hosts", return_value=["head"]),
        patch(
            "first_pilot.replica_manager.query_gpus",
            return_value=PilotResources(hosts=[host]),
        ),
        pytest.raises(RuntimeError, match="host 'head' reported indices"),
    ):
        ReplicaManager(config)  # type: ignore[arg-type]


def test_inventory_refresh_swaps_snapshot_and_survives_failure() -> None:
    hostnames = ["head", "worker"]
    config = SimpleNamespace(
        node_file_env="TEST_NODEFILE",
        gpu_discovery=SSHDiscovery(),
        num_nodes=2,
        gpus_per_node=2,
    )
    startup = PilotResources(
        hosts=_host_resources(hostnames, 2),
        observed_at=datetime(2026, 10, 16, 12, 0, tzinfo=timezone.utc),
    )
    refreshed = PilotResources(
        hosts=_host_resources(hostnames, 2),
        observed_at=datetime(2026, 10, 16, 12, 1, tzinfo=timezone.utc),
    )
    refreshed.hosts[0].gpus[0].memory_used_mib = 1
    with (
        patch("first_pilot.replica_manager.discover_hosts", return_value=hostnames),
        patch(
            "first_pilot.replica_manager.query_gpus",
            side_effect=[startup, refreshed, RuntimeError("ssh timed out")],
        ) as query_ssh,
    ):
        manager = ReplicaManager(config)  # type: ignore[arg-type]
        assert manager.get_resources() == startup
        assert query_ssh.call_count == 1  # reads never run discovery

        manager._refresh_inventory()
        assert manager.get_resources() == refreshed

        # A failed refresh keeps the last snapshot, which shows its age.
        manager._refresh_inventory()
        assert manager.get_resources() == refreshed
        assert manager.get_resources().observed_at == refreshed.observed_at

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
                "external_port": 8000,
                "nginx_path": "/stale/nginx",
                "ip_allowlist": ["0.0.0.0/0"],
                "workdir": "/stale/workdir",
                "node_file_env": "STALE_NODEFILE",
                "network_interfaces": ["hsn0", "hsn1"],
            }
        )
    )
    monkeypatch.setenv("PILOT_CONFIG_FILE", str(config_path))
    monkeypatch.setenv("PILOT_CA_CRT", "environment-must-not-replace-static-secret")
    monkeypatch.setenv("PILOT_NETWORK_INTERFACES", '["eth0"]')
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
    monkeypatch.setenv(
        "PILOT_NODE_ADMISSION",
        '{"checks":["gpu_count"],"on_failure":"pbs_quarantine","environment":"dev"}',
    )
    monkeypatch.setenv("PILOT_QUEUE", "prod")
    monkeypatch.setenv("PILOT_ACCOUNT", "TestAcct")
    monkeypatch.setenv("PILOT_NUM_NODES", "2")
    monkeypatch.setenv("PILOT_GPUS_PER_NODE", "4")
    monkeypatch.setenv("PILOT_WALLTIME_MIN", "90")

    config = PilotRuntimeConfig.load()

    assert config.ca_crt == "ca"
    assert config.job_name == "test-pilot"
    assert config.external_port == 19443
    assert config.nginx_path == Path("/opt/test/nginx")
    assert config.ip_allowlist == ["192.0.2.10/32"]
    assert config.workdir == Path("/opt/test/workdir")
    assert config.node_file_env == "TEST_NODEFILE"
    assert config.gpu_discovery == PalsDiscovery(
        launcher_path=Path("/opt/test/mpiexec")
    )
    assert config.node_admission.checks == ["gpu_count"]
    assert config.node_admission.on_failure == "pbs_quarantine"
    assert (config.queue, config.account) == ("prod", "TestAcct")
    assert config.num_nodes == 2
    assert config.gpus_per_node == 4
    assert config.walltime_min == 90
    assert config.network_interfaces == ["hsn0", "hsn1"]
