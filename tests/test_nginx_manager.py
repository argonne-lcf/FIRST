import re
import shutil
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from first_common.schema.pilot import PilotClientRole, PilotRuntimeConfig
from first_pilot.nginx_manager import (
    NginxManager,
    ReplicaUpstream,
    check_server_cert_expiry,
)

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _self_signed(not_after: datetime) -> tuple[str, str]:
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "first-pilot")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_after - timedelta(days=30))
        .not_valid_after(not_after)
        .sign(key, hashes.SHA256())
    )
    cert_pem = cert.public_bytes(serialization.Encoding.PEM).decode()
    key_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    return cert_pem, key_pem


@pytest.fixture
def manager(tmp_path: Path) -> NginxManager:
    crt, key = _self_signed(datetime.now(timezone.utc) + timedelta(days=1))
    config = PilotRuntimeConfig(
        ca_crt=crt,
        server_crt=crt,
        server_key=key,
        external_port=18443,
        nginx_path=Path(shutil.which("nginx") or "/usr/sbin/nginx"),
        ip_allowlist=["192.0.2.10/32"],
        workdir=tmp_path,
        node_file_env="TEST_NODEFILE",
        num_nodes=1,
        gpus_per_node=1,
        job_name="test-pilot",
        walltime_min=60,
    )
    return NginxManager(config, tmp_path / "nginx")


def _block(rendered: str, header: str) -> str:
    return rendered.split(header + " {", 1)[1].split("}", 1)[0]


def test_rendered_config_authorizes_by_client_role(manager: NginxManager) -> None:
    rendered = manager.render_config(
        [ReplicaUpstream(name="org/model", uds="/tmp/model.sock")]
    )

    assert "ssl_verify_client on;" in rendered
    assert "ssl_verify_depth 1;" in rendered

    role_map = _block(rendered, "map $ssl_client_s_dn $role")
    for role in PilotClientRole:
        assert f'"CN={role.value}" {role.name};' in role_map
    assert "default none;" in role_map

    policy = _block(rendered, 'map "$role:$request_method:$uri" $authorized')
    assert re.findall(r'"~\^(\w+):', policy) == [
        PilotClientRole.control.name,
        PilotClientRole.router.name,
        PilotClientRole.router.name,
        PilotClientRole.metrics.name,
    ]
    assert '"~^router:GET:/control/logs/" 1;' in policy
    assert "default 0;" in policy

    # The deny sits at server level, ahead of every location.
    server = rendered.split("server {", 1)[1]
    deny = server.index("if ($authorized = 0) {\n            return 403;")
    assert deny < server.index("location /")

    # Per-location IP allow-lists are still in place.
    for location in ("location /control/", "location /replicas/org/model/"):
        assert "allow 192.0.2.10/32;" in _block(rendered, location)


@pytest.mark.skipif(shutil.which("nginx") is None, reason="nginx not installed")
def test_rendered_config_passes_nginx_syntax_check(manager: NginxManager) -> None:
    conf = manager.tmpdir / "check.conf"
    conf.write_text(
        manager.render_config([ReplicaUpstream(name="m", uds="/tmp/m.sock")])
    )
    result = subprocess.run(
        [
            manager.pilot_config.nginx_path,
            "-t",
            "-e",
            f"{manager.tmpdir}/nginx-error.log",
            "-c",
            conf.as_posix(),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_cert_expiry_check_accepts_cert_outliving_walltime() -> None:
    crt, _ = _self_signed(NOW + timedelta(minutes=61))
    check_server_cert_expiry(crt, walltime_min=60, now=NOW)


def test_cert_expiry_check_rejects_cert_expiring_before_walltime() -> None:
    crt, _ = _self_signed(NOW + timedelta(minutes=59))
    with pytest.raises(RuntimeError, match="expires at .* before the end"):
        check_server_cert_expiry(crt, walltime_min=60, now=NOW)
