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
from pydantic import ValidationError

from first_common.schema.pilot import (
    PilotClientRole,
    PilotRuntimeConfig,
    ReplicaStartRequest,
)
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


def _runtime_config(tmp_path: Path) -> PilotRuntimeConfig:
    crt, key = _self_signed(datetime.now(timezone.utc) + timedelta(days=1))
    return PilotRuntimeConfig(
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
        network_interfaces=["hsn0", "hsn1"],
    )


@pytest.fixture
def manager(tmp_path: Path) -> NginxManager:
    return NginxManager(
        _runtime_config(tmp_path), tmp_path / "nginx", ["192.0.2.1", "192.0.2.2"]
    )


def test_rendered_config_listens_on_loopback_and_interfaces_only(
    manager: NginxManager,
) -> None:
    rendered = manager.render_config([])
    assert re.findall(r"^\s*(listen .*)$", rendered, re.MULTILINE) == [
        "listen 127.0.0.1:18443 ssl;",
        "listen 192.0.2.1:18443 ssl;",
        "listen 192.0.2.2:18443 ssl;",
    ]


def test_loopback_interface_is_not_listened_twice(tmp_path: Path) -> None:
    manager = NginxManager(
        _runtime_config(tmp_path), tmp_path / "nginx", ["192.0.2.1", "127.0.0.1"]
    )
    assert manager.listen_ips == ["127.0.0.1", "192.0.2.1"]


@pytest.mark.parametrize("ip", ["0.0.0.0 ssl; listen 80", "hsn0", "::1", ""])
def test_unsafe_listen_address_is_refused(tmp_path: Path, ip: str) -> None:
    with pytest.raises(ValueError):
        NginxManager(_runtime_config(tmp_path), tmp_path / "nginx", [ip])


def _block(rendered: str, header: str) -> str:
    return rendered.split(header + " {", 1)[1].split("}", 1)[0]


def test_rendered_config_authorizes_by_client_role(manager: NginxManager) -> None:
    rendered = manager.render_config(
        [ReplicaUpstream("org/model", "/tmp/model.sock", ("/v1/chat/completions",))]
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
    for location in (
        "location /control/",
        "location = /replicas/org/model/v1/chat/completions",
    ):
        assert "allow 192.0.2.10/32;" in _block(rendered, location)


def _replica_request(**overrides: object) -> ReplicaStartRequest:
    fields: dict[str, object] = {
        "name": "m/replica/0",
        "deployment_name": "depl",
        "launch_spec": {
            "served_model_name": "m",
            "gpus_per_node": 1,
            "num_nodes": 1,
            "max_model_len": None,
            "env": {},
            "parameters": {},
            "serve_script_template": "true",
            "pre_stop_script_template": None,
            "post_stop_script_template": None,
            "max_startup_sec": 1,
            "max_unhealthy_sec": None,
            "pre_stop_timeout_sec": 1.0,
            "post_stop_timeout_sec": 1.0,
            "health_check": {"url": ""},
        },
        "gpu_indices": [],
        "supported_endpoints": ["/chat/completions"],
        "prometheus_metrics_path": "/metrics",
    }
    return ReplicaStartRequest.model_validate({**fields, **overrides})


def test_proxy_paths_come_from_model_endpoints_and_metrics_path() -> None:
    request = _replica_request(supported_endpoints=["/messages", "chat/completions/"])
    assert request.proxy_paths == ["/v1/messages", "/v1/chat/completions", "/metrics"]
    assert _replica_request(prometheus_metrics_path=None).proxy_paths == [
        "/v1/chat/completions"
    ]


def test_replica_exposes_only_its_declared_paths(manager: NginxManager) -> None:
    rendered = manager.render_config(
        [
            ReplicaUpstream("chat-only", "/tmp/a.sock", ("/v1/chat/completions",)),
            ReplicaUpstream(
                "multi",
                "/tmp/b.sock",
                ("/v1/chat/completions", "/v1/messages", "/metrics"),
            ),
        ]
    )
    # Every replica location is an exact match pinned to a full upstream path.
    assert re.findall(
        r"location (= \S+) \{\n(?:.*\n)*?\s*proxy_pass (http://replica_.*);", rendered
    ) == [
        (
            "= /replicas/chat-only/v1/chat/completions",
            "http://replica_1/v1/chat/completions",
        ),
        (
            "= /replicas/multi/v1/chat/completions",
            "http://replica_2/v1/chat/completions",
        ),
        ("= /replicas/multi/v1/messages", "http://replica_2/v1/messages"),
        ("= /replicas/multi/metrics", "http://replica_2/metrics"),
    ]
    assert re.search(r"location / \{\s+return 404;", rendered)


UNSAFE_PROXY_PATHS = [
    "",
    "/",
    "chat completions",
    "chat/../../control/status",
    "chat//completions",
    "chat;",
    "chat { return 200; }",
    "chat$uri",
    "ch\nat",
    "chat?x=1",
    "chat%2e",
]


@pytest.mark.parametrize("path", UNSAFE_PROXY_PATHS)
def test_render_config_refuses_unsafe_replica_path(
    manager: NginxManager, path: str
) -> None:
    with pytest.raises(ValueError):
        manager.render_config([ReplicaUpstream("m", "/tmp/m.sock", (f"/{path}",))])


@pytest.mark.parametrize("path", UNSAFE_PROXY_PATHS)
def test_replica_start_request_rejects_unsafe_endpoint(path: str) -> None:
    with pytest.raises(ValidationError) as exc_info:
        _replica_request(supported_endpoints=[path])
    assert ("supported_endpoints",) in [e["loc"] for e in exc_info.value.errors()]
    if path:  # an empty metrics path means "no metrics", like None
        with pytest.raises(ValidationError):
            _replica_request(prometheus_metrics_path=path)


@pytest.mark.skipif(shutil.which("nginx") is None, reason="nginx not installed")
def test_rendered_config_passes_nginx_syntax_check(tmp_path: Path) -> None:
    # `nginx -t` binds each listen address, so it must be local.
    manager = NginxManager(_runtime_config(tmp_path), tmp_path / "nginx", [])
    conf = manager.tmpdir / "check.conf"
    conf.write_text(
        manager.render_config(
            [ReplicaUpstream("m", "/tmp/m.sock", ("/v1/chat/completions", "/metrics"))]
        )
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


UNSAFE_REPLICA_NAMES = [
    "",
    "m/ { return 200; } location /x",
    "m;",
    "m}",
    "m m",
    "m\n",
    "m$uri",
    'm"',
    "m#",
    "m\\",
    "../m",
    "m/../../x",
    "m/./x",
    "/abs/m",
    "m/",
    "m//x",
    ".hidden",
    "-m",
]


@pytest.mark.parametrize("name", UNSAFE_REPLICA_NAMES)
def test_render_config_refuses_unsafe_replica_name(
    manager: NginxManager, name: str
) -> None:
    with pytest.raises(ValueError, match="unsafe replica name"):
        manager.render_config([ReplicaUpstream(name, "/tmp/m.sock", ("/metrics",))])


@pytest.mark.parametrize("name", UNSAFE_REPLICA_NAMES)
def test_replica_start_request_rejects_unsafe_name(name: str) -> None:
    with pytest.raises(ValidationError) as exc_info:
        ReplicaStartRequest.model_validate({"name": name})
    assert ("name",) in [e["loc"] for e in exc_info.value.errors()]


def test_runtime_config_requires_a_network_interface(tmp_path: Path) -> None:
    fields = _runtime_config(tmp_path).model_dump(exclude={"network_interfaces"})
    missing: dict[str, list[str]]
    for missing in ({}, {"network_interfaces": list[str]()}):
        with pytest.raises(ValidationError) as exc_info:
            PilotRuntimeConfig.model_validate({**fields, **missing})
        assert ("network_interfaces",) in [e["loc"] for e in exc_info.value.errors()]


def test_replica_start_request_accepts_generated_name() -> None:
    name = "meta-llama/Meta-Llama-3.1-8B_v2/replica/0a1b2c3d"
    with pytest.raises(ValidationError) as exc_info:
        ReplicaStartRequest.model_validate({"name": name})
    assert ("name",) not in [e["loc"] for e in exc_info.value.errors()]


def test_cert_expiry_check_accepts_cert_outliving_walltime() -> None:
    crt, _ = _self_signed(NOW + timedelta(minutes=61))
    check_server_cert_expiry(crt, walltime_min=60, now=NOW)


def test_cert_expiry_check_rejects_cert_expiring_before_walltime() -> None:
    crt, _ = _self_signed(NOW + timedelta(minutes=59))
    with pytest.raises(RuntimeError, match="expires at .* before the end"):
        check_server_cert_expiry(crt, walltime_min=60, now=NOW)
