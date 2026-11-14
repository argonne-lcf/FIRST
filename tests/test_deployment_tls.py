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
