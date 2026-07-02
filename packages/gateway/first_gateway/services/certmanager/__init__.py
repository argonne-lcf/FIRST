import secrets
import subprocess
import tempfile
from pathlib import Path
from shutil import which


class OpenSSLError(RuntimeError):
    """openssl is missing or a subprocess invocation failed."""


def _run(*args: str, stdin: str | None = None) -> str:
    if which("openssl") is None:
        raise OpenSSLError("openssl is not installed or not on PATH")
    proc = subprocess.run(
        ["openssl", *args],
        input=stdin,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    if proc.returncode:
        raise OpenSSLError(f"openssl {args[0]} failed: {proc.stderr.strip()}")
    return proc.stdout


def gen_key_pem() -> str:
    """Generate a new EC P-256 private key and return it as PEM."""
    return _run(
        "genpkey",
        "-algorithm",
        "EC",
        "-pkeyopt",
        "ec_paramgen_curve:P-256",
    )


def gen_ca_pem(*, name: str, days: int = 3650) -> tuple[str, str]:
    """Generate a self-signed Root CA. Returns ``(cert_pem, key_pem)``."""
    key_pem = gen_key_pem()
    with tempfile.TemporaryDirectory() as td:
        key_path = Path(td) / "ca.key"
        key_path.write_text(key_pem)
        cert_pem = _run(
            "req",
            "-x509",
            "-new",
            "-key",
            str(key_path),
            "-sha256",
            "-days",
            str(days),
            "-subj",
            f"/CN={name}",
            "-addext",
            "basicConstraints=critical,CA:TRUE,pathlen:0",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
            "-addext",
            "subjectKeyIdentifier=hash",
        )
    return cert_pem, key_pem


def _issue_leaf_pem(
    *,
    cn: str,
    ca_cert_pem: str,
    ca_key_pem: str,
    days: int,
    eku: str,
) -> tuple[str, str]:
    """Issue a leaf cert signed by the given CA. Returns ``(cert_pem, key_pem)``."""
    key_pem = gen_key_pem()
    # Random 159-bit positive serial — keeps each issued cert unique without
    # needing a persistent serial counter on disk.
    serial_hex = f"0x{secrets.randbits(159):x}"
    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        key_path = d / "leaf.key"
        ca_cert_path = d / "ca.crt"
        ca_key_path = d / "ca.key"
        key_path.write_text(key_pem)
        ca_cert_path.write_text(ca_cert_pem)
        ca_key_path.write_text(ca_key_pem)

        csr_pem = _run(
            "req",
            "-new",
            "-key",
            str(key_path),
            "-subj",
            f"/CN={cn}",
            "-addext",
            "basicConstraints=critical,CA:FALSE",
            "-addext",
            "keyUsage=critical,digitalSignature",
            "-addext",
            f"extendedKeyUsage={eku}",
        )
        cert_pem = _run(
            "x509",
            "-req",
            "-CA",
            str(ca_cert_path),
            "-CAkey",
            str(ca_key_path),
            "-set_serial",
            serial_hex,
            "-days",
            str(days),
            "-sha256",
            "-copy_extensions",
            "copy",
            stdin=csr_pem,
        )
    return cert_pem, key_pem


def generate_client_cert(
    *,
    cn: str,
    ca_cert_pem: str,
    ca_key_pem: str,
    days: int = 730,
) -> tuple[str, str]:
    """Issue a clientAuth leaf cert. Returns ``(cert_pem, key_pem)``."""
    return _issue_leaf_pem(
        cn=cn,
        ca_cert_pem=ca_cert_pem,
        ca_key_pem=ca_key_pem,
        days=days,
        eku="clientAuth",
    )


def generate_server_cert(
    *,
    cn: str,
    ca_cert_pem: str,
    ca_key_pem: str,
    days: int = 730,
) -> tuple[str, str]:
    """Issue a serverAuth leaf cert. Returns ``(cert_pem, key_pem)``."""
    return _issue_leaf_pem(
        cn=cn,
        ca_cert_pem=ca_cert_pem,
        ca_key_pem=ca_key_pem,
        days=days,
        eku="serverAuth",
    )
