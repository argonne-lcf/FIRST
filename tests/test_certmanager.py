import ipaddress
from pathlib import Path

from cryptography import x509

from first_common.schema.pilot import PILOT_SERVER_CN
from first_gateway.services.certmanager import (
    PROMETHEUS_SERVER_CN,
    PROMETHEUS_SERVER_DNS_NAMES,
    PROMETHEUS_SERVER_IP_ADDRESSES,
    gen_ca_pem,
    generate_server_cert,
)
from first_gateway.services.certmanager.cli import standard


def _subject_alt_names(cert_pem: str) -> x509.SubjectAlternativeName:
    cert = x509.load_pem_x509_certificate(cert_pem.encode())
    return cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value


def test_server_certificate_supports_ip_sans() -> None:
    ca_cert, ca_key = gen_ca_pem(name="test-ca")
    cert, _ = generate_server_cert(
        cn=PILOT_SERVER_CN,
        san_dns=["tara.first-pilot.internal"],
        san_ip=["10.0.0.1"],
        ca_cert_pem=ca_cert,
        ca_key_pem=ca_key,
    )

    names = _subject_alt_names(cert)
    assert names.get_values_for_type(x509.DNSName) == ["tara.first-pilot.internal"]
    assert names.get_values_for_type(x509.IPAddress) == [
        ipaddress.ip_address("10.0.0.1")
    ]


def test_standard_pki_includes_prometheus_https_identity(tmp_path: Path) -> None:
    standard(
        directory=tmp_path,
        cluster=["tara"],
        prometheus_dns=["gateway.example"],
        prometheus_ip=["10.0.0.2"],
    )

    cert_path = tmp_path / f"{PROMETHEUS_SERVER_CN}.crt"
    assert (tmp_path / f"{PROMETHEUS_SERVER_CN}.key").is_file()
    assert (tmp_path / "pilot-server-tara.crt").is_file()
    names = _subject_alt_names(cert_path.read_text())
    assert names.get_values_for_type(x509.DNSName) == [
        *PROMETHEUS_SERVER_DNS_NAMES,
        "gateway.example",
    ]
    assert names.get_values_for_type(x509.IPAddress) == [
        ipaddress.ip_address(address)
        for address in [*PROMETHEUS_SERVER_IP_ADDRESSES, "10.0.0.2"]
    ]
