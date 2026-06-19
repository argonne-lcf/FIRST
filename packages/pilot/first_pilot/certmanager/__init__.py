"""Manage self-signed certificates for mTLS using openssl.

Workflow:
    mtls.py ca --name "FIRST CA"     # one self-signed Root CA (default 10 years)
    mtls.py server first_pilot       # server cert signed by that CA (default 2 years)
    mtls.py client first_gateway     # client cert signed by that CA (default 2 years)

Re-running `server` / `client` with the same name re-issues (rotates) that cert
against the existing CA. Requires OpenSSL 3.x.
"""

import re
import subprocess
from pathlib import Path
from shutil import which
from typing import Annotated

import typer

app = typer.Typer(add_completion=False, help=__doc__, no_args_is_help=True)
DirOpt = Annotated[Path, typer.Option("--dir", help="PKI directory.")]


def run(*args: str) -> None:
    """Run an openssl subcommand, surfacing stderr on failure."""
    if which("openssl") is None:
        typer.secho(
            "Please install openssl, which is needed to use this tool.",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(1)

    proc = subprocess.run(["openssl", *args], capture_output=True, text=True)
    if proc.returncode:
        typer.secho(proc.stderr.strip(), fg=typer.colors.RED, err=True)
        raise typer.Exit(1)


def gen_key(path: Path) -> None:
    run(
        "genpkey",
        "-algorithm",
        "EC",
        "-pkeyopt",
        "ec_paramgen_curve:P-256",
        "-out",
        str(path),
    )


def slug(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9.-]+", "_", text).strip("_")


def issue(
    *,
    kind: str,
    cn: str,
    directory: Path,
    days: int,
    eku: str,
) -> None:
    """Generate a leaf key + CSR and sign it with the CA."""
    ca_key, ca_crt = directory / "ca.key", directory / "ca.crt"
    if not (ca_key.exists() and ca_crt.exists()):
        typer.secho(
            f"No CA in '{directory}'. Run `ca` first.", fg=typer.colors.RED, err=True
        )
        raise typer.Exit(1)

    base = slug(cn)
    key, csr, crt = (directory / f"{base}.{ext}" for ext in ("key", "csr", "crt"))
    key_usage = "digitalSignature"
    gen_key(key)

    req = [
        "req",
        "-new",
        "-key",
        str(key),
        "-out",
        str(csr),
        "-subj",
        f"/CN={cn}",
        "-addext",
        "basicConstraints=critical,CA:FALSE",
        "-addext",
        f"keyUsage=critical,{key_usage}",
        "-addext",
        f"extendedKeyUsage={eku}",
    ]
    run(*req)

    run(
        "x509",
        "-req",
        "-in",
        str(csr),
        "-CA",
        str(ca_crt),
        "-CAkey",
        str(ca_key),
        "-CAcreateserial",
        "-days",
        str(days),
        "-sha256",
        "-copy_extensions",
        "copy",
        "-out",
        str(crt),
    )
    csr.unlink(missing_ok=True)
    typer.secho(
        f"\u2713 {kind} cert: {crt}  (key: {key}, {days} days)", fg=typer.colors.GREEN
    )


@app.command()
def ca(
    name: Annotated[str, typer.Option(help="CA common name (CN).")],
    directory: DirOpt = Path("pki"),
    days: Annotated[int, typer.Option(help="Validity in days.")] = 3650,
) -> None:
    """Create a CA key and self-signed Root CA certificate (default 10 years)."""
    directory.mkdir(parents=True, exist_ok=True)
    ca_key, ca_crt = directory / "ca.key", directory / "ca.crt"
    gen_key(ca_key)
    run(
        "req",
        "-x509",
        "-new",
        "-key",
        str(ca_key),
        "-sha256",
        "-days",
        str(days),
        "-out",
        str(ca_crt),
        "-subj",
        f"/CN={name}",
        "-addext",
        "basicConstraints=critical,CA:TRUE,pathlen:0",
        "-addext",
        "keyUsage=critical,keyCertSign,cRLSign",
        "-addext",
        "subjectKeyIdentifier=hash",
    )
    typer.secho(
        f"\u2713 Root CA: {ca_crt}  (key: {ca_key}, {days} days)", fg=typer.colors.GREEN
    )


@app.command()
def server(
    cn: Annotated[
        str, typer.Argument(help="Server hostname / identity, e.g. api.internal.")
    ],
    directory: DirOpt = Path("pki"),
    days: Annotated[int, typer.Option(help="Validity in days.")] = 730,
) -> None:
    """Issue a server certificate (serverAuth) signed by the CA (default 2 years)."""
    issue(kind="Server", cn=cn, directory=directory, days=days, eku="serverAuth")


@app.command()
def client(
    cn: Annotated[
        str, typer.Argument(help="Client identity, e.g. alice or a service name.")
    ],
    directory: DirOpt = Path("pki"),
    days: Annotated[int, typer.Option(help="Validity in days.")] = 730,
) -> None:
    """Issue a client certificate (clientAuth) signed by the CA (default 2 years)."""
    issue(kind="Client", cn=cn, directory=directory, days=days, eku="clientAuth")


if __name__ == "__main__":
    app()
