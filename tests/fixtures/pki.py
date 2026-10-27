"""
A throwaway pilot CA issuing the standard mTLS identity set (see
`pilot-certmanager standard`) for two pilot clusters, plus one client cert with
a CN the pilot does not recognize.
"""

import ssl
from dataclasses import dataclass
from pathlib import Path

import psutil
import pytest

from first_common.schema.pilot import (
    PILOT_SERVER_CN,
    PilotClientRole,
    pilot_server_name,
)
from first_gateway.services.certmanager import (
    gen_ca_pem,
    generate_client_cert,
    generate_server_cert,
)
from first_gateway.services.pilot_control import pilot_ssl_context
from first_gateway.settings import Settings

UNKNOWN_CLIENT_CN = "first-unknown"
# Pilot clusters with their own server certificate. Tests run pilots on the
# first unless they say otherwise.
PILOT_TEST_CLUSTERS = ("alpha", "beta")
PILOT_TEST_CLUSTER = PILOT_TEST_CLUSTERS[0]


@dataclass(frozen=True)
class Identity:
    crt: Path
    key: Path


@dataclass(frozen=True)
class PilotPKI:
    ca_crt: Path
    servers: dict[str, Identity]
    clients: dict[PilotClientRole, Identity]
    unknown_client: Identity

    @property
    def server(self) -> Identity:
        """The server identity of PILOT_TEST_CLUSTER."""
        return self.servers[PILOT_TEST_CLUSTER]

    def runtime_config(
        self, cluster: str = PILOT_TEST_CLUSTER
    ) -> dict[str, str | list[str]]:
        """The file-only fields of a pre-staged PilotRuntimeConfig."""
        server = self.servers[cluster]
        return {
            "ca_crt": self.ca_crt.read_text(),
            "server_crt": server.crt.read_text(),
            "server_key": server.key.read_text(),
            "network_interfaces": [loopback_interface()],
        }

    def client_context(
        self, identity: Identity, cluster: str = PILOT_TEST_CLUSTER
    ) -> ssl.SSLContext:
        """A client SSLContext for calling a pilot on `cluster` as `identity`."""
        settings = Settings.model_construct(
            pilot_ca_crt_file=self.ca_crt,
            pilot_client_crt_file=identity.crt,
            pilot_client_key_file=identity.key,
        )
        return pilot_ssl_context(settings, cluster)


def loopback_interface() -> str:
    """The loopback interface name: "lo" on Linux, "lo0" on macOS."""
    for ifname, addrs in psutil.net_if_addrs().items():
        if any(a.address == "127.0.0.1" for a in addrs):
            return ifname
    raise RuntimeError("no interface holds 127.0.0.1")


@pytest.fixture(scope="session")
def pilot_pki(tmp_path_factory: pytest.TempPathFactory) -> PilotPKI:
    directory = tmp_path_factory.mktemp("pki")
    ca_crt, ca_key = gen_ca_pem(name="test-ca")
    (directory / "ca.crt").write_text(ca_crt)

    def write(base: str, crt: str, key: str) -> Identity:
        identity = Identity(directory / f"{base}.crt", directory / f"{base}.key")
        identity.crt.write_text(crt)
        identity.key.write_text(key)
        return identity

    def issue_server(cluster: str) -> Identity:
        crt, key = generate_server_cert(
            cn=PILOT_SERVER_CN,
            san_dns=[pilot_server_name(cluster)],
            ca_cert_pem=ca_crt,
            ca_key_pem=ca_key,
        )
        return write(f"pilot-server-{cluster}", crt, key)

    def issue_client(cn: str) -> Identity:
        crt, key = generate_client_cert(cn=cn, ca_cert_pem=ca_crt, ca_key_pem=ca_key)
        return write(cn, crt, key)

    return PilotPKI(
        ca_crt=directory / "ca.crt",
        servers={cluster: issue_server(cluster) for cluster in PILOT_TEST_CLUSTERS},
        clients={role: issue_client(role.value) for role in PilotClientRole},
        unknown_client=issue_client(UNKNOWN_CLIENT_CN),
    )


@pytest.fixture
def pilot_mtls_settings(pilot_pki: PilotPKI, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point Settings() at the throwaway CA and the control identity."""
    control = pilot_pki.clients[PilotClientRole.control]
    monkeypatch.setenv("FIRST_PILOT_CA_CRT_FILE", str(pilot_pki.ca_crt))
    monkeypatch.setenv("FIRST_PILOT_CLIENT_CRT_FILE", str(control.crt))
    monkeypatch.setenv("FIRST_PILOT_CLIENT_KEY_FILE", str(control.key))
