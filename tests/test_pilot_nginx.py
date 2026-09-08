"""Pilot interface selection must constrain both advertisement and listeners."""

import socket
import subprocess
from pathlib import Path
from shutil import which
from unittest.mock import MagicMock, patch

import pytest

from first_common.schema.pilot import PilotRuntimeConfig
from first_gateway.services.certmanager import gen_ca_pem, generate_server_cert
from first_pilot.control_api import _PilotManager
from first_pilot.nginx_manager import NginxManager, ReplicaUpstream


@pytest.fixture
def config(tmp_path: Path) -> PilotRuntimeConfig:
    return PilotRuntimeConfig(
        ca_crt="test ca",
        server_crt="test certificate",
        server_key="test key",
        external_port=18443,
        nginx_path=Path("/test/nginx"),
        ip_allowlist=["192.0.2.0/24"],
        workdir=tmp_path,
        node_file_env="TEST_NODEFILE",
        num_nodes=1,
        gpus_per_node=4,
        job_name="test-pilot",
    )


@pytest.mark.parametrize(
    ("bind_ip", "listen_address", "probe_ip"),
    [
        (None, "18443", "127.0.0.1"),
        ("192.0.2.10", "192.0.2.10:18443", "192.0.2.10"),
        ("2001:db8::10", "[2001:db8::10]:18443", "2001:db8::10"),
        ("127.0.0.1", "127.0.0.1:18443", "127.0.0.1"),
    ],
)
def test_listener_and_readiness_use_same_address(
    config: PilotRuntimeConfig,
    tmp_path: Path,
    bind_ip: str | None,
    listen_address: str,
    probe_ip: str,
) -> None:
    nginx = NginxManager(config, tmp_path / "nginx", bind_ip=bind_ip)
    for replicas in ([], [ReplicaUpstream("test/model/replica", "/tmp/model.sock")]):
        rendered = nginx.render_config(replicas)
        listeners = [
            line.strip() for line in rendered.splitlines() if "listen " in line
        ]
        assert listeners == [f"listen {listen_address} ssl;"]
        assert "ssl_verify_client on;" in rendered
        assert "ssl_protocols TLSv1.3;" in rendered
        assert "allow 192.0.2.0/24;" in rendered

    nginx._nginx = MagicMock()
    nginx._nginx.poll.return_value = None
    with patch("first_pilot.nginx_manager.socket.create_connection") as connect:
        nginx.wait_until_healthy()
    connect.assert_called_once_with((probe_ip, config.external_port), timeout=1)


def test_explicit_interface_binds_the_advertised_ip(
    config: PilotRuntimeConfig, tmp_path: Path
) -> None:
    config.network_interface = "hsn0"
    with (
        patch.object(_PilotManager, "_interface_ip", return_value="192.0.2.10") as ip,
        patch("first_pilot.control_api.ReplicaManager"),
    ):
        manager = _PilotManager(config, tmp_path / "nginx")
    ip.assert_called_once_with("hsn0")
    assert manager.nginx.bind_ip == manager._endpoint.ip == "192.0.2.10"
    assert "listen 192.0.2.10:18443 ssl;" in manager.nginx.render_config([])


def test_automatic_endpoint_discovery_preserves_wildcard_listener(
    config: PilotRuntimeConfig, tmp_path: Path
) -> None:
    with (
        patch("first_pilot.control_api.socket.socket") as route,
        patch("first_pilot.control_api.ReplicaManager"),
    ):
        route.return_value.__enter__.return_value.getsockname.return_value = (
            "192.0.2.10",
            0,
        )
        manager = _PilotManager(config, tmp_path / "nginx")
    assert manager._endpoint.ip == "192.0.2.10"
    assert manager.nginx.bind_ip is None
    assert "listen 18443 ssl;" in manager.nginx.render_config([])


def test_missing_interface_fails_before_nginx_is_created(
    config: PilotRuntimeConfig, tmp_path: Path
) -> None:
    config.network_interface = "missing0"
    with (
        patch.object(
            _PilotManager, "_interface_ip", side_effect=OSError("no interface")
        ),
        patch("first_pilot.control_api.NginxManager") as nginx,
        pytest.raises(OSError, match="no interface"),
    ):
        _PilotManager(config, tmp_path / "nginx")
    nginx.assert_not_called()


@pytest.mark.skipif(which("nginx") is None, reason="nginx not installed")
@pytest.mark.parametrize("bind_ip", [None, "127.0.0.2", "::1"])
def test_nginx_validates_listener_config(
    config: PilotRuntimeConfig, tmp_path: Path, bind_ip: str | None
) -> None:
    ca_crt, ca_key = gen_ca_pem(name="listener-test", days=1)
    config.ca_crt = ca_crt
    config.server_crt, config.server_key = generate_server_cert(
        cn="listener-test", ca_cert_pem=ca_crt, ca_key_pem=ca_key, days=1
    )
    config.nginx_path = Path(which("nginx") or "nginx")
    nginx = NginxManager(config, tmp_path / "nginx", bind_ip=bind_ip)
    subprocess.run(
        [config.nginx_path, "-t", "-c", nginx.config_path],
        capture_output=True,
        check=True,
        timeout=10,
    )

    if bind_ip == "127.0.0.2":
        with socket.socket() as reserved:
            reserved.bind((bind_ip, 0))
            config.external_port = reserved.getsockname()[1]
        nginx.config_path.write_text(nginx.render_config([]))
        try:
            nginx.start()
            nginx.wait_until_healthy()
            with pytest.raises(OSError):
                socket.create_connection(("127.0.0.1", config.external_port), timeout=1)
        finally:
            nginx.stop()
