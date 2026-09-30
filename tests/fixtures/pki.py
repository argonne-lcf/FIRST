"""
A throwaway pilot CA issuing the standard mTLS identity set (see
`pilot-certmanager standard`), plus one client cert with a CN the pilot does
not recognize.
"""

import ssl
from dataclasses import dataclass
from pathlib import Path

import pytest

from first_common.schema.pilot import PILOT_SERVER_CN, PilotClientRole
from first_gateway.services.certmanager import (
    gen_ca_pem,
    generate_client_cert,
    generate_server_cert,
)

UNKNOWN_CLIENT_CN = "first-unknown"


@dataclass(frozen=True)
class Identity:
    crt: Path
    key: Path


@dataclass(frozen=True)
class PilotPKI:
    ca_crt: Path
    server: Identity
    clients: dict[PilotClientRole, Identity]
    unknown_client: Identity

    def runtime_config(self) -> dict[str, str]:
        """The cert fields of a pre-staged PilotRuntimeConfig."""
        return {
            "ca_crt": self.ca_crt.read_text(),
            "server_crt": self.server.crt.read_text(),
            "server_key": self.server.key.read_text(),
        }

    def client_context(self, identity: Identity) -> ssl.SSLContext:
        """A client SSLContext for calling a pilot as ``identity``."""
        ctx = ssl.create_default_context(cafile=self.ca_crt)
        ctx.check_hostname = False  # pilots are reached by IP
        ctx.load_cert_chain(identity.crt, identity.key)
        return ctx


@pytest.fixture(scope="session")
def pilot_pki(tmp_path_factory: pytest.TempPathFactory) -> PilotPKI:
    directory = tmp_path_factory.mktemp("pki")
    ca_crt, ca_key = gen_ca_pem(name="test-ca")
    (directory / "ca.crt").write_text(ca_crt)

    def issue(cn: str, *, server: bool = False) -> Identity:
        issue_fn = generate_server_cert if server else generate_client_cert
        crt, key = issue_fn(cn=cn, ca_cert_pem=ca_crt, ca_key_pem=ca_key)
        identity = Identity(directory / f"{cn}.crt", directory / f"{cn}.key")
        identity.crt.write_text(crt)
        identity.key.write_text(key)
        return identity

    return PilotPKI(
        ca_crt=directory / "ca.crt",
        server=issue(PILOT_SERVER_CN, server=True),
        clients={role: issue(role.value) for role in PilotClientRole},
        unknown_client=issue(UNKNOWN_CLIENT_CN),
    )


@pytest.fixture
def pilot_mtls_settings(pilot_pki: PilotPKI, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point Settings() at the throwaway CA and the control identity."""
    control = pilot_pki.clients[PilotClientRole.control]
    monkeypatch.setenv("FIRST_PILOT_CA_CRT_FILE", str(pilot_pki.ca_crt))
    monkeypatch.setenv("FIRST_PILOT_CLIENT_CRT_FILE", str(control.crt))
    monkeypatch.setenv("FIRST_PILOT_CLIENT_KEY_FILE", str(control.key))
