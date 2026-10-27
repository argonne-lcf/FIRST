"""StaticDeployment mTLS: client certificate sets for externally-managed upstreams."""

import ipaddress
import ssl
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from first_common.errors import InvalidSpecError
from first_common.health import perform_health_check
from first_common.schema.types import (
    HealthCheckParams,
    HealthCheckResult,
    MTLSClientConfig,
    MTLSLoadError,
)
from first_gateway.apiserver import backend_client_manager as bcm
from first_gateway.controllers.routes import discovery
from first_gateway.controllers.workers.health_observer import HealthObserver
from first_gateway.controllers.workers.router_config_observer import (
    RouterConfigObserver,
)
from first_gateway.database import models as db
from first_gateway.database.redis.router_config import (
    BackendConfig,
    DeploymentConfig,
    ModelConfig,
    RouterConfig,
)
from first_gateway.services.plan_apply import create_plan, validate_resources
from first_gateway.settings import Settings

from .test_resource_apply import _load


@dataclass(frozen=True)
class CertSet:
    mtls: MTLSClientConfig
    server_crt: Path
    server_key: Path


def _name(cn: str) -> x509.Name:
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])


def _issue_set(directory: Path, label: str) -> CertSet:
    """A CA, a server cert for 127.0.0.1, and a client cert, written as PEM."""
    now = datetime.now(timezone.utc)
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_crt = (
        x509.CertificateBuilder()
        .subject_name(_name(f"{label}-ca"))
        .issuer_name(_name(f"{label}-ca"))
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), True)
        .sign(ca_key, hashes.SHA256())
    )

    def leaf(cn: str, eku: x509.ObjectIdentifier, server: bool) -> tuple[Path, Path]:
        key = ec.generate_private_key(ec.SECP256R1())
        builder = (
            x509.CertificateBuilder()
            .subject_name(_name(cn))
            .issuer_name(ca_crt.subject)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=1))
            .not_valid_after(now + timedelta(days=1))
            .add_extension(x509.ExtendedKeyUsage([eku]), False)
        )
        if server:
            san = [x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
            builder = builder.add_extension(x509.SubjectAlternativeName(san), False)
        crt = builder.sign(ca_key, hashes.SHA256())
        crt_path, key_path = directory / f"{cn}.crt", directory / f"{cn}.key"
        crt_path.write_bytes(crt.public_bytes(serialization.Encoding.PEM))
        key_path.write_bytes(
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )
        return crt_path, key_path

    directory.mkdir(parents=True, exist_ok=True)
    (directory / "ca.crt").write_bytes(ca_crt.public_bytes(serialization.Encoding.PEM))
    server_crt, server_key = leaf("server", ExtendedKeyUsageOID.SERVER_AUTH, True)
    client_crt, client_key = leaf("client", ExtendedKeyUsageOID.CLIENT_AUTH, False)
    return CertSet(
        mtls=MTLSClientConfig(
            ca_file=str(directory / "ca.crt"),
            cert_file=str(client_crt),
            key_file=str(client_key),
        ),
        server_crt=server_crt,
        server_key=server_key,
    )


@pytest.fixture(scope="module")
def certs(tmp_path_factory: pytest.TempPathFactory) -> CertSet:
    return _issue_set(tmp_path_factory.mktemp("upstream") / "sophia", "sophia")


@pytest.fixture(scope="module")
def foreign_certs(tmp_path_factory: pytest.TempPathFactory) -> CertSet:
    """A set from an unrelated CA, which the upstream must reject."""
    return _issue_set(tmp_path_factory.mktemp("upstream") / "other", "other")


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        body = b'{"status": "ok"}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: object) -> None:
        pass


@pytest.fixture(scope="module")
def upstream(certs: CertSet) -> Iterator[str]:
    """An HTTPS server that requires a client cert issued by `certs`' CA."""
    ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH, cafile=certs.mtls.ca_file)
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.load_cert_chain(certs.server_crt, certs.server_key)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    server.socket = ctx.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"https://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


# ── MTLSClientConfig ─────────────────────────────────────────────────


def test_paths_must_be_absolute() -> None:
    with pytest.raises(ValidationError, match="absolute path"):
        MTLSClientConfig(
            ca_file="pki/ca.crt", cert_file="/a/client.crt", key_file="/a/client.key"
        )


def test_missing_file_raises_load_error_naming_the_set(certs: CertSet) -> None:
    mtls = certs.mtls.model_copy(update={"key_file": "/nonexistent/client.key"})
    with pytest.raises(MTLSLoadError, match=certs.mtls.cert_file):
        mtls.ssl_context()


def test_mismatched_key_raises_load_error(
    certs: CertSet, foreign_certs: CertSet
) -> None:
    mtls = certs.mtls.model_copy(update={"key_file": foreign_certs.mtls.key_file})
    with pytest.raises(MTLSLoadError):
        mtls.ssl_context()


# ── Health checks ────────────────────────────────────────────────────


def _static_row(mtls: MTLSClientConfig | None, url: str) -> db.StaticDeployment:
    return db.StaticDeployment(
        uid=1,
        name="sophia/static/m",
        cluster_name="sophia",
        model_name="m",
        api_url=f"{url}/v1",
        api_key=None,
        mtls=mtls.model_dump() if mtls else None,
        upstream_model_name="m",
        router_params={},
        health_check={"url": f"{url}/health", "debounce": 1},
        health="healthy",
        prometheus_metrics_path="/metrics",
        prometheus_scrape_interval_sec=30,
    )


def _health_observer() -> HealthObserver:
    return HealthObserver("health", MagicMock(), MagicMock())


async def test_health_check_presents_client_cert(
    certs: CertSet, foreign_certs: CertSet, upstream: str
) -> None:
    observer = _health_observer()
    params = HealthCheckParams(url=f"{upstream}/health", attempts_per_check=1)
    # Trusts the upstream, but presents a client cert from another CA
    wrong_identity = certs.mtls.model_copy(
        update={
            "cert_file": foreign_certs.mtls.cert_file,
            "key_file": foreign_certs.mtls.key_file,
        }
    )
    for mtls, expected in [
        (certs.mtls, HealthCheckResult.healthy),
        (wrong_identity, HealthCheckResult.unhealthy),
        (foreign_certs.mtls, HealthCheckResult.unhealthy),
        (None, HealthCheckResult.unhealthy),
    ]:
        client = observer._client_for(_static_row(mtls, upstream))
        outcome = await perform_health_check(client, params)
        assert outcome.result == expected, (mtls, outcome)


def test_health_clients_are_shared_per_cert_set(certs: CertSet, upstream: str) -> None:
    observer = _health_observer()
    a = observer._client_for(_static_row(certs.mtls, upstream))
    b = observer._client_for(_static_row(certs.mtls, upstream))
    assert a is b
    assert a is not observer.health_client
    assert observer._client_for(_static_row(None, upstream)) is observer.health_client


async def test_unloadable_cert_set_reports_unhealthy_detail(certs: CertSet) -> None:
    observer = _health_observer()
    observer.hb = MagicMock()
    broken = certs.mtls.model_copy(update={"ca_file": "/nonexistent/ca.crt"})
    _, _, result, detail = await observer._check(  # type: ignore[misc]
        _static_row(broken, "https://upstream.invalid")
    )
    assert result == HealthCheckResult.unhealthy
    assert detail is not None and "Could not load mTLS certificate set" in detail


# ── Data plane ───────────────────────────────────────────────────────


def _router_config(mtls: MTLSClientConfig | None, url: str) -> RouterConfig:
    (deployment,) = RouterConfigObserver._build_deployments(
        [], [_static_row(mtls, url)], {}
    )
    return RouterConfig(
        version=1,
        models=[
            ModelConfig(
                name="m",
                aliases=[],
                allowed_groups=[],
                allowed_domains=[],
                supported_endpoints=["chat/completions"],
                usage_limits={},  # type: ignore[arg-type]
                overload={},  # type: ignore[arg-type]
                deployments=[deployment],
            )
        ],
    )


def test_router_config_carries_mtls(certs: CertSet) -> None:
    cfg = _router_config(certs.mtls, "https://x")
    assert cfg.models[0].deployments[0].backends[0].mtls == certs.mtls
    # Round trip through the Redis JSON blob
    assert RouterConfig.model_validate_json(cfg.model_dump_json()) == cfg


async def test_backend_client_presents_client_cert(
    pilot_mtls_settings: None,
    monkeypatch: pytest.MonkeyPatch,
    certs: CertSet,
    upstream: str,
) -> None:
    monkeypatch.setattr(bcm, "CLOSE_GRACE_SEC", 0)
    manager = bcm.BackendClientManager(Settings())
    try:
        await manager.on_config_swap(_router_config(None, upstream))
        plain = manager.get("static_deployment/1")
        assert plain is not None
        with pytest.raises(httpx.ConnectError):
            await plain.get("models")

        # Adding mtls to the deployment swaps in a client that presents it
        await manager.on_config_swap(_router_config(certs.mtls, upstream))
        client = manager.get("static_deployment/1")
        assert client is not None and client is not plain
        resp = await client.get("models")
        assert resp.status_code == 200
    finally:
        await manager.close_all()


async def test_unloadable_cert_set_fails_only_that_backend(
    pilot_mtls_settings: None, certs: CertSet, upstream: str
) -> None:
    broken = certs.mtls.model_copy(update={"cert_file": "/nonexistent/client.crt"})
    cfg = _router_config(broken, upstream)
    manager = bcm.BackendClientManager(Settings())
    try:
        with pytest.raises(RuntimeError, match="static_deployment/1"):
            await manager.on_config_swap(cfg)
        assert manager.get("static_deployment/1") is None
    finally:
        await manager.close_all()


# ── Prometheus discovery ─────────────────────────────────────────────


async def test_discovery_separates_targets_by_cert_set(
    monkeypatch: pytest.MonkeyPatch, certs: CertSet
) -> None:
    def deployment(name: str, mtls: MTLSClientConfig | None) -> DeploymentConfig:
        return DeploymentConfig(
            kind="static",
            name=name,
            cluster_name="c",
            router_params={},  # type: ignore[arg-type]
            prometheus_metrics_path="/metrics",
            prometheus_scrape_interval_sec=30,
            backends=[
                BackendConfig(
                    id=f"static_deployment/{name}",
                    model_url=f"https://{name}.example/v1",
                    backend_model_name="m",
                    api_key=None,
                    mtls=mtls,
                )
            ],
        )

    cfg = RouterConfig(
        models=[
            ModelConfig(
                name="m",
                aliases=[],
                allowed_groups=[],
                allowed_domains=[],
                supported_endpoints=[],
                usage_limits={},  # type: ignore[arg-type]
                overload={},  # type: ignore[arg-type]
                deployments=[deployment("plain", None), deployment("mtls", certs.mtls)],
            )
        ]
    )
    monkeypatch.setattr(RouterConfig, "load", AsyncMock(return_value=cfg))

    async def deployments(mtls_cert_file: str | None) -> list[str]:
        targets = await discovery.prometheus_service_discovery(
            MagicMock(), mtls_cert_file
        )
        return [t.labels["deployment"] for t in targets]

    assert await deployments(None) == ["plain"]
    assert await deployments(certs.mtls.cert_file) == ["mtls"]
    assert await deployments("/run/secrets/upstream/other/client.crt") == []


# ── Plan-time validation ─────────────────────────────────────────────


def _with_static_mtls(mtls: MTLSClientConfig) -> list:  # type: ignore[type-arg]
    resources = _load("baseline")
    for i, r in enumerate(resources):
        if r.kind == "StaticDeployment":
            spec = r.spec.model_copy(update={"mtls": mtls})
            resources[i] = r.model_copy(update={"spec": spec})
    return resources


def test_plan_accepts_loadable_cert_set(certs: CertSet) -> None:
    validate_resources(_with_static_mtls(certs.mtls))


def test_plan_rejects_unloadable_cert_set(certs: CertSet) -> None:
    broken = certs.mtls.model_copy(update={"ca_file": "/nonexistent/ca.crt"})
    with pytest.raises(InvalidSpecError, match="sophia/static/llama-3-8b"):
        validate_resources(_with_static_mtls(broken))


async def test_mtls_round_trips_through_database(
    db_session: AsyncSession, certs: CertSet
) -> None:
    resources = _with_static_mtls(certs.mtls)
    for r in resources:
        db.resource_registry[r.kind].create_from_spec(db_session, r.name, r.spec)
    await db_session.flush()

    row = await db.StaticDeployment.get_by_name(db_session, "sophia/static/llama-3-8b")
    assert MTLSClientConfig.model_validate(row.mtls) == certs.mtls
    # Re-planning the same spec must not report a spurious mtls change
    plan = await create_plan(resources, db_session)
    assert not plan.to_add and not plan.to_update and not plan.to_delete
