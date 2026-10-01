"""Manage self-signed certificates for mTLS on disk.

Workflow:
    pilot-certmanager standard --dir pki    # CA (if absent) + pilot server + all client roles
    pilot-certmanager ca --name "FIRST CA"  # self-signed Root CA (default 10 years)
    pilot-certmanager server first-pilot    # server cert signed by the CA (default 2 years)
    pilot-certmanager client first-control  # client cert signed by the CA (default 2 years)

Certs are written as ``<dir>/<cn>.crt`` and ``<dir>/<cn>.key``. Re-running
``server`` / ``client`` / ``standard`` re-issues the leaf certs against the
existing CA. Requires OpenSSL 3.x.
"""

import re
from pathlib import Path
from typing import Annotated

import typer

from first_common.schema.pilot import PILOT_SERVER_CN, PilotClientRole

from . import (
    OpenSSLError,
    gen_ca_pem,
    generate_client_cert,
    generate_server_cert,
)

app = typer.Typer(add_completion=False, help=__doc__, no_args_is_help=True)
DirOpt = Annotated[Path, typer.Option("--dir", help="PKI directory.")]


def _slug(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9.-]+", "_", text).strip("_")


def _write(path: Path, data: str, *, mode: int) -> None:
    path.write_text(data)
    path.chmod(mode)


def _load_ca(directory: Path) -> tuple[str, str]:
    ca_key, ca_crt = directory / "ca.key", directory / "ca.crt"
    if not (ca_key.exists() and ca_crt.exists()):
        typer.secho(
            f"No CA in '{directory}'. Run `ca` first.",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(1)
    return ca_crt.read_text(), ca_key.read_text()


@app.command()
def ca(
    name: Annotated[str, typer.Option(help="CA common name (CN).")],
    directory: DirOpt = Path("pki"),
    days: Annotated[int, typer.Option(help="Validity in days.")] = 3650,
) -> None:
    """Create a CA key and self-signed Root CA certificate (default 10 years)."""
    directory.mkdir(parents=True, exist_ok=True)
    try:
        cert_pem, key_pem = gen_ca_pem(name=name, days=days)
    except OpenSSLError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(1)

    ca_key, ca_crt = directory / "ca.key", directory / "ca.crt"
    _write(ca_key, key_pem, mode=0o600)
    _write(ca_crt, cert_pem, mode=0o644)
    typer.secho(
        f"✓ Root CA: {ca_crt}  (key: {ca_key}, {days} days)",
        fg=typer.colors.GREEN,
    )


def _issue_to_disk(
    *,
    kind: str,
    cn: str,
    directory: Path,
    days: int,
) -> None:
    ca_cert_pem, ca_key_pem = _load_ca(directory)
    try:
        if kind == "Server":
            cert_pem, key_pem = generate_server_cert(
                cn=cn, ca_cert_pem=ca_cert_pem, ca_key_pem=ca_key_pem, days=days
            )
        else:
            cert_pem, key_pem = generate_client_cert(
                cn=cn, ca_cert_pem=ca_cert_pem, ca_key_pem=ca_key_pem, days=days
            )
    except OpenSSLError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(1)

    base = _slug(cn)
    key_path = directory / f"{base}.key"
    crt_path = directory / f"{base}.crt"
    _write(key_path, key_pem, mode=0o600)
    _write(crt_path, cert_pem, mode=0o644)
    typer.secho(
        f"✓ {kind} cert: {crt_path}  (key: {key_path}, {days} days)",
        fg=typer.colors.GREEN,
    )


@app.command()
def server(
    cn: Annotated[
        str, typer.Argument(help="Server hostname / identity, e.g. api.internal.")
    ],
    directory: DirOpt = Path("pki"),
    days: Annotated[int, typer.Option(help="Validity in days.")] = 365,
) -> None:
    """Issue a server certificate (serverAuth) signed by the CA (default 1 year)."""
    _issue_to_disk(kind="Server", cn=cn, directory=directory, days=days)


@app.command()
def client(
    cn: Annotated[
        str, typer.Argument(help="Client identity, e.g. alice or a service name.")
    ],
    directory: DirOpt = Path("pki"),
    days: Annotated[int, typer.Option(help="Validity in days.")] = 365,
) -> None:
    """Issue a client certificate (clientAuth) signed by the CA (default 1 year)."""
    _issue_to_disk(kind="Client", cn=cn, directory=directory, days=days)


@app.command()
def standard(
    directory: DirOpt = Path("pki"),
    ca_name: Annotated[
        str, typer.Option(help="CA common name, if a CA must be created.")
    ] = "FIRST Pilot CA",
    days: Annotated[int, typer.Option(help="Leaf validity in days.")] = 365,
) -> None:
    """Issue the pilot server cert and one client cert per role (CA if absent)."""
    if not (directory / "ca.crt").exists():
        ca(name=ca_name, directory=directory)
    _issue_to_disk(kind="Server", cn=PILOT_SERVER_CN, directory=directory, days=days)
    for role in PilotClientRole:
        _issue_to_disk(kind="Client", cn=role.value, directory=directory, days=days)


if __name__ == "__main__":
    app()
