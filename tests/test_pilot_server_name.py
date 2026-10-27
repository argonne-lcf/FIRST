"""
Per-cluster pilot server names: each cluster's pilot server certificate names
`<cluster>.first-pilot.internal`, and every pilot client verifies that name
while dialing the pilot by IP (see docs/security/PLAN-CLUSTER-SERVER-NAME.md).
"""

import ssl
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import yaml
from cryptography import x509
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from pydantic import ValidationError
from typer.testing import CliRunner

from first_common.schema.pilot import (
    PILOT_SERVER_CN,
    PilotClientRole,
    pilot_server_name,
)
from first_common.schema.resources.plan_apply import ResourceManifest
from first_gateway.apiserver import backend_client_manager as bcm
from first_gateway.controllers.routes import discovery
from first_gateway.database.redis.router_config import (
    BackendConfig,
    DeploymentConfig,
    ModelConfig,
    RouterConfig,
)
from first_gateway.services.certmanager.cli import app as certmanager
from first_gateway.services.pilot_control import PilotControlClient, pilot_ssl_context
from first_gateway.settings import Settings
from first_pilot.nginx_manager import check_server_cert_name
from tests.fixtures.pki import PilotPKI

RESOURCE_SPECS = Path(__file__).parent / "resource_specs"


def _dns_sans(cert: x509.Certificate) -> list[str]:
    san = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName)
    return san.value.get_values_for_type(x509.DNSName)


# ── pilot-certmanager ────────────────────────────────────────────────


def test_pilot_server_issues_cluster_name(tmp_path: Path) -> None:
    runner = CliRunner()
    pki = ["--dir", str(tmp_path)]
    assert runner.invoke(certmanager, ["ca", "--name", "test", *pki]).exit_code == 0
    result = runner.invoke(certmanager, ["pilot-server", "tara", *pki])
    assert result.exit_code == 0, result.output

    cert = x509.load_pem_x509_certificate(
        (tmp_path / "pilot-server-tara.crt").read_bytes()
    )
    assert (tmp_path / "pilot-server-tara.key").stat().st_mode & 0o777 == 0o600
    assert _dns_sans(cert) == ["tara.first-pilot.internal"]
    [cn] = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
    assert cn.value == PILOT_SERVER_CN
    eku = cert.extensions.get_extension_for_class(x509.ExtendedKeyUsage)
    assert list(eku.value) == [ExtendedKeyUsageOID.SERVER_AUTH]


def test_pilot_server_rejects_non_dns_label(tmp_path: Path) -> None:
    runner = CliRunner()
    pki = ["--dir", str(tmp_path)]
    assert runner.invoke(certmanager, ["ca", "--name", "test", *pki]).exit_code == 0
    result = runner.invoke(certmanager, ["pilot-server", "Tara_1", *pki])
    assert result.exit_code == 1
    assert not list(tmp_path.glob("pilot-server-*"))


def test_standard_issues_one_server_cert_per_cluster(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        certmanager,
        [
            "standard",
            "--cluster",
            "tara",
            "--cluster",
            "sophia",
            "--dir",
            str(tmp_path),
        ],
    )
    assert result.exit_code == 0, result.output
    for cluster in ("tara", "sophia"):
        cert = x509.load_pem_x509_certificate(
            (tmp_path / f"pilot-server-{cluster}.crt").read_bytes()
        )
        assert _dns_sans(cert) == [pilot_server_name(cluster)]
    for role in PilotClientRole:
        assert (tmp_path / f"{role.value}.crt").exists()


def test_standard_requires_a_cluster(tmp_path: Path) -> None:
    result = CliRunner().invoke(certmanager, ["standard", "--dir", str(tmp_path)])
    assert result.exit_code != 0
    assert not (tmp_path / "ca.crt").exists()


# ── Client SSL contexts ──────────────────────────────────────────────


@pytest.mark.parametrize("dialed", ["10.0.0.1", None, "beta.first-pilot.internal"])
def test_context_verifies_cluster_name_whatever_the_caller_passes(
    pilot_mtls_settings: None, dialed: str | None
) -> None:
    ctx = pilot_ssl_context(Settings(), "alpha")
    assert ctx.check_hostname and ctx.verify_mode == ssl.CERT_REQUIRED
    obj = ctx.wrap_bio(ssl.MemoryBIO(), ssl.MemoryBIO(), server_hostname=dialed)
    assert obj.server_hostname == "alpha.first-pilot.internal"


def test_context_is_shared_per_cluster(pilot_mtls_settings: None) -> None:
    settings = Settings()
    alpha = pilot_ssl_context(settings, "alpha")
    assert pilot_ssl_context(Settings(), "alpha") is alpha
    assert pilot_ssl_context(settings, "beta") is not alpha


async def test_control_client_keeps_one_client_per_cluster(
    pilot_mtls_settings: None,
) -> None:
    client_state = MagicMock()
    client_state.settings = Settings()
    control = PilotControlClient(client_state)
    alpha = control._client_for("alpha")
    assert control._client_for("alpha") is alpha
    beta = control._client_for("beta")
    assert beta is not alpha

    await control.aclose()
    assert alpha.is_closed and beta.is_closed
    assert control._clients == {}


# ── Router clients ───────────────────────────────────────────────────


def _deployment(
    name: str, kind: str, cluster: str, metrics: str | None = None
) -> DeploymentConfig:
    return DeploymentConfig(
        kind=kind,  # type: ignore[arg-type]
        name=name,
        cluster_name=cluster,
        router_params={},  # type: ignore[arg-type]
        prometheus_metrics_path=metrics,
        prometheus_scrape_interval_sec=30,
        backends=[
            BackendConfig(
                id=f"{kind}/{name}",
                model_url=f"https://{name}.example/v1",
                backend_model_name="m",
                api_key=None,
            )
        ],
    )


def _router_config(*deployments: DeploymentConfig) -> RouterConfig:
    return RouterConfig(
        models=[
            ModelConfig(
                name="m",
                aliases=[],
                allowed_groups=[],
                allowed_domains=[],
                supported_endpoints=[],
                usage_limits={},  # type: ignore[arg-type]
                overload={},  # type: ignore[arg-type]
                deployments=list(deployments),
            )
        ]
    )


async def test_router_clients_use_their_clusters_context(
    pilot_mtls_settings: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    contexts: list[Any] = []
    transport = httpx.AsyncHTTPTransport

    def recording_transport(*, verify: Any, **kwargs: Any) -> Any:
        contexts.append(verify)
        return transport(verify=verify, **kwargs)

    monkeypatch.setattr(httpx, "AsyncHTTPTransport", recording_transport)
    settings = Settings()
    manager = bcm.BackendClientManager(settings)
    try:
        await manager.on_config_swap(
            _router_config(
                _deployment("a", "pilot", "alpha"),
                _deployment("b", "pilot", "beta"),
                _deployment("s", "static", "alpha"),
            )
        )
    finally:
        await manager.close_all()

    alpha, beta, static = contexts
    assert alpha is pilot_ssl_context(settings, "alpha")
    assert beta is pilot_ssl_context(settings, "beta")
    assert static is True


def test_router_client_recreated_when_cluster_changes(
    pilot_mtls_settings: None,
) -> None:
    manager = bcm.BackendClientManager(Settings())
    old = _deployment("a", "pilot", "alpha")
    manager._clients["pilot/a"] = MagicMock()
    manager._configs["pilot/a"] = (old.backends[0], old)
    moved = old.model_copy(update={"cluster_name": "beta"})
    assert manager._should_recreate("pilot/a", old.backends[0], moved)
    assert not manager._should_recreate("pilot/a", old.backends[0], old)


# ── Prometheus discovery ─────────────────────────────────────────────


async def test_discovery_lists_pilot_backends_per_cluster(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _router_config(
        _deployment("tara-1", "pilot", "tara", "/metrics"),
        _deployment("tara-2", "pilot", "tara", "/metrics"),
        _deployment("sophia-1", "pilot", "sophia", "/metrics"),
        _deployment("static-1", "static", "tara", "/metrics"),
    )
    monkeypatch.setattr(RouterConfig, "load", AsyncMock(return_value=cfg))

    async def deployments(pilot_cluster: str | None) -> list[str]:
        targets = await discovery.prometheus_service_discovery(
            MagicMock(), None, pilot_cluster
        )
        return [t.labels["deployment"] for t in targets]

    assert await deployments("tara") == ["tara-1", "tara-2"]
    assert await deployments("sophia") == ["sophia-1"]
    assert await deployments("polaris") == []
    # Without the parameter, no pilot backend: it would fail verification in
    # any job but its cluster's.
    assert await deployments(None) == ["static-1"]


# ── Cluster names ────────────────────────────────────────────────────


def _cluster_manifest(name: str, *, pilot: bool = True) -> dict[str, Any]:
    docs = yaml.safe_load_all((RESOURCE_SPECS / "baseline" / "spec.yaml").read_text())
    [cluster] = [d for d in docs if d["kind"] == "Cluster"]
    if not pilot:
        cluster["spec"].pop("pilot_system")
    return {**cluster, "name": name}


@pytest.mark.parametrize("name", ["Tara_1", "a/b", "tara.dev", "-tara", "x" * 64])
def test_pilot_cluster_name_must_be_dns_label(name: str) -> None:
    with pytest.raises(ValidationError):
        ResourceManifest.model_validate(_cluster_manifest(name))


@pytest.mark.parametrize("name", ["tara", "sophia", "test-cluster", "x" * 63])
def test_pilot_cluster_name_accepts_dns_label(name: str) -> None:
    ResourceManifest.model_validate(_cluster_manifest(name))


def test_static_only_cluster_name_is_unconstrained() -> None:
    ResourceManifest.model_validate(_cluster_manifest("Sophia_Static", pilot=False))


# ── Pilot startup ────────────────────────────────────────────────────


def test_pilot_accepts_certificate_naming_its_cluster(pilot_pki: PilotPKI) -> None:
    check_server_cert_name(pilot_pki.servers["beta"].crt.read_text(), "beta")


def test_pilot_refuses_certificate_of_another_cluster(pilot_pki: PilotPKI) -> None:
    with pytest.raises(RuntimeError, match="not issued for cluster 'alpha'"):
        check_server_cert_name(pilot_pki.servers["beta"].crt.read_text(), "alpha")
