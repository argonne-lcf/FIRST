from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).parents[1]


def _config(*parts: str) -> Any:
    return yaml.safe_load((ROOT.joinpath(*parts)).read_text())


def test_prometheus_serves_https_with_tls13_minimum() -> None:
    compose = _config("deploy", "compose.yaml")
    prometheus = compose["services"]["prometheus"]
    assert "--web.config.file=/etc/prometheus/web.yml" in prometheus["command"]
    assert "./prometheus-web.yml:/etc/prometheus/web.yml:ro" in prometheus["volumes"]

    web = _config("deploy", "prometheus-web.yml")
    assert web == {
        "tls_server_config": {
            "cert_file": "/run/secrets/prometheus_server.crt",
            "key_file": "/run/secrets/prometheus_server.key",
            "min_version": "TLS13",
        }
    }

    secrets = {secret["source"]: secret["target"] for secret in prometheus["secrets"]}
    assert secrets["prometheus_crt"] == "prometheus_server.crt"
    assert secrets["prometheus_key"] == "prometheus_server.key"
    assert compose["secrets"]["prometheus_crt"]["file"] == (
        "../pki/first-prometheus.crt"
    )
    assert compose["secrets"]["prometheus_key"]["file"] == (
        "../pki/first-prometheus.key"
    )


OVERLAYS = [
    "compose.yaml",
    "compose.dev.yaml",
    "compose.prod.yaml",
    "compose.analytics.yaml",
    "compose.dev-analytics.yaml",
    "compose.tunnel.yaml",
    "compose.v1-logs.yaml",
]


def test_prometheus_publishes_on_loopback_only() -> None:
    """Prometheus runs with the admin API, so nothing but loopback reaches it."""
    for name in OVERLAYS:
        service = (_config("deploy", name).get("services") or {}).get("prometheus")
        for port in (service or {}).get("ports", []):
            assert port.startswith("127.0.0.1:"), (name, port)


def test_prometheus_is_off_the_gateway_network() -> None:
    compose = _config("deploy", "compose.yaml")
    services = compose["services"]
    assert services["prometheus"]["networks"] == ["metrics"]
    assert "metrics" not in services["inference-gateway"]["networks"]
    assert "metrics" in services["controller-manager"]["networks"]


def test_analytics_proxy_requires_grafana_client_cert_and_allowlists_reads() -> None:
    conf = (ROOT / "deploy" / "prometheus-analytics-proxy.conf").read_text()
    assert "ssl_verify_client on;" in conf
    assert "ssl_client_certificate /run/secrets/analytics_ca.crt;" in conf
    assert "CN=first-analytics-grafana" in conf
    assert "proxy_ssl_verify on;" in conf
    assert "location / {\n            return 403;" in conf
    assert "admin" not in conf.split("location ~")[1].split("{")[0]

    analytics = _config("deploy", "compose.analytics.yaml")
    proxy = analytics["services"]["prometheus-analytics-proxy"]
    assert proxy["networks"] == ["metrics"]
    assert proxy["read_only"] is True
    assert proxy["cap_drop"] == ["ALL"]
    assert analytics["secrets"]["analytics_ca"]["file"] == "../pki/analytics-ca.crt"
    for name in ("compose.prod.yaml", "compose.dev-analytics.yaml"):
        services = _config("deploy", name)["services"]
        assert services["prometheus-analytics-proxy"]["ports"], name
