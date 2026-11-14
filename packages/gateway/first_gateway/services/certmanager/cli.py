"""Manage internal CA certificates on disk.

Workflow:
    pilot-certmanager standard --cluster tara --dir pki  # CA (if absent) + all identities
    pilot-certmanager ca --name "FIRST CA"               # self-signed Root CA (default 10 years)
    pilot-certmanager pilot-server tara                  # pilot server cert for one cluster
    pilot-certmanager prometheus-server --dns gateway.example
    pilot-certmanager client first-control               # client cert signed by the CA

Client certs are written as ``<dir>/<cn>.{crt,key}``; pilot server certs as
``<dir>/pilot-server-<cluster>.{crt,key}`` and the Prometheus identity as
``<dir>/first-prometheus.{crt,key}``. Re-running a leaf command re-issues that
certificate against the existing CA. Requires OpenSSL 3.x.
"""

import re
from collections.abc import Sequence
from pathlib import Path
from typing import Annotated

import typer

from first_common.schema.pilot import (
    PILOT_CLUSTER_NAME_PATTERN,
    PILOT_SERVER_CN,
    PilotClientRole,
    pilot_server_name,
)

from . import (
    PROMETHEUS_SERVER_CN,
    PROMETHEUS_SERVER_DNS_NAMES,
    PROMETHEUS_SERVER_IP_ADDRESSES,
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
    san_dns: list[str] | None = None,
    san_ip: Sequence[str] = (),
    base: str | None = None,
) -> None:
    ca_cert_pem, ca_key_pem = _load_ca(directory)
    try:
        if san_dns is not None:
            cert_pem, key_pem = generate_server_cert(
                cn=cn,
                san_dns=san_dns,
                san_ip=san_ip,
                ca_cert_pem=ca_cert_pem,
                ca_key_pem=ca_key_pem,
                days=days,
            )
        else:
            cert_pem, key_pem = generate_client_cert(
                cn=cn, ca_cert_pem=ca_cert_pem, ca_key_pem=ca_key_pem, days=days
            )
    except OpenSSLError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(1)

    base = base or _slug(cn)
    key_path = directory / f"{base}.key"
    crt_path = directory / f"{base}.crt"
    _write(key_path, key_pem, mode=0o600)
    _write(crt_path, cert_pem, mode=0o644)
    sans = f", SAN {', '.join(san_dns)}" if san_dns else ""
    typer.secho(
        f"✓ {kind} cert: {crt_path}  (key: {key_path}, {days} days{sans})",
        fg=typer.colors.GREEN,
    )


def _issue_pilot_server(cluster: str, *, directory: Path, days: int) -> None:
    if not re.fullmatch(PILOT_CLUSTER_NAME_PATTERN, cluster):
        typer.secho(
            f"Cluster name {cluster!r} is not a DNS label.",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(1)
    _issue_to_disk(
        kind="Pilot server",
        cn=PILOT_SERVER_CN,
        directory=directory,
        days=days,
        san_dns=[pilot_server_name(cluster)],
        base=f"pilot-server-{cluster}",
    )


@app.command("pilot-server")
def pilot_server(
    cluster: Annotated[str, typer.Argument(help="Cluster name, e.g. tara.")],
    directory: DirOpt = Path("pki"),
    days: Annotated[int, typer.Option(help="Validity in days.")] = 365,
) -> None:
    """Issue a pilot server certificate for one cluster (default 1 year)."""
    _issue_pilot_server(cluster, directory=directory, days=days)


def _issue_prometheus_server(
    *,
    directory: Path,
    days: int,
    dns_names: Sequence[str] = (),
    ip_addresses: Sequence[str] = (),
) -> None:
    _issue_to_disk(
        kind="Prometheus server",
        cn=PROMETHEUS_SERVER_CN,
        directory=directory,
        days=days,
        san_dns=[*PROMETHEUS_SERVER_DNS_NAMES, *dns_names],
        san_ip=[*PROMETHEUS_SERVER_IP_ADDRESSES, *ip_addresses],
    )


@app.command("prometheus-server")
def prometheus_server(
    directory: DirOpt = Path("pki"),
    days: Annotated[int, typer.Option(help="Validity in days.")] = 365,
    dns_names: Annotated[
        list[str],
        typer.Option("--dns", help="Additional DNS SAN for analytics; repeatable."),
    ] = [],
    ip_addresses: Annotated[
        list[str],
        typer.Option("--ip", help="Additional IP SAN for analytics; repeatable."),
    ] = [],
) -> None:
    """Issue the Prometheus HTTPS server certificate (default 1 year)."""
    _issue_prometheus_server(
        directory=directory,
        days=days,
        dns_names=dns_names,
        ip_addresses=ip_addresses,
    )


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
    cluster: Annotated[
        list[str],
        typer.Option(
            "--cluster", help="Pilot cluster to issue a server cert for (repeat)."
        ),
    ],
    directory: DirOpt = Path("pki"),
    ca_name: Annotated[
        str, typer.Option(help="CA common name, if a CA must be created.")
    ] = "FIRST Pilot CA",
    days: Annotated[int, typer.Option(help="Leaf validity in days.")] = 365,
    prometheus_dns: Annotated[
        list[str],
        typer.Option(
            "--prometheus-dns", help="Additional Prometheus DNS SAN; repeatable."
        ),
    ] = [],
    prometheus_ip: Annotated[
        list[str],
        typer.Option(
            "--prometheus-ip", help="Additional Prometheus IP SAN; repeatable."
        ),
    ] = [],
) -> None:
    """Issue the standard pilot and Prometheus identities (CA if absent)."""
    if not (directory / "ca.crt").exists():
        ca(name=ca_name, directory=directory)
    for name in cluster:
        _issue_pilot_server(name, directory=directory, days=days)
    _issue_prometheus_server(
        directory=directory,
        days=days,
        dns_names=prometheus_dns,
        ip_addresses=prometheus_ip,
    )

    for role in PilotClientRole:
        _issue_to_disk(kind="Client", cn=role.value, directory=directory, days=days)


if __name__ == "__main__":
    app()
