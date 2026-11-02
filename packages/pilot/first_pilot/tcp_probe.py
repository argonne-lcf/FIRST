"""
Check that a node can open a TCP connection to its own HSN address.

Run on each node by the `tcp_self_connect` startup check. The probe listens on
the wildcard address, as a torch TCPStore rendezvous server does, connects to
the first configured interface's IPv4 address, and exchanges a short payload.
It prints one `Observation` as JSON.

Only a connect or accept timeout is reported as a result. Any other error (the
port is in use, the connection is refused, the payload is wrong) is raised:
it says nothing about whether the node can reach itself.
"""

import argparse
import socket
import time
from ipaddress import IPv4Address
from typing import Literal

from pydantic import BaseModel, Field

from .host_commands import host_identity, resolve_interface_ips

_PAYLOAD = b"FIRST IPv4 admission probe\n"


class Observation(BaseModel):
    host: str
    address: IPv4Address
    port: int
    result: Literal["pass", "connect_timeout", "accept_timeout"]
    elapsed_sec: float = Field(ge=0)


def probe(address: str, port: int, timeout_sec: float = 2.0) -> Observation:
    """Connect to `address:port` through a wildcard listener; close all sockets."""
    started = time.monotonic()
    with (
        socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener,
        socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client,
    ):
        listener.settimeout(timeout_sec)
        client.settimeout(timeout_sec)
        # No SO_REUSEADDR or fallback port: a collision is an error to report,
        # not evidence that the node cannot reach itself.
        listener.bind(("0.0.0.0", port))
        listener.listen(1)
        result = _exchange(listener, client, address, port, timeout_sec)
    return Observation(
        host=host_identity(socket.gethostname()),
        address=IPv4Address(address),
        port=port,
        result=result,
        elapsed_sec=time.monotonic() - started,
    )


def _exchange(
    listener: socket.socket,
    client: socket.socket,
    address: str,
    port: int,
    timeout_sec: float,
) -> Literal["pass", "connect_timeout", "accept_timeout"]:
    try:
        client.connect((address, port))
    except TimeoutError:
        return "connect_timeout"
    try:
        connection, _peer = listener.accept()
    except TimeoutError:
        return "accept_timeout"
    with connection:
        connection.settimeout(timeout_sec)
        client.sendall(_PAYLOAD)
        _receive(connection, _PAYLOAD)
        connection.sendall(_PAYLOAD)
        _receive(client, _PAYLOAD)
        # The client must close first, so TIME_WAIT lands on its ephemeral port.
        # TIME_WAIT on the probe port would make the confirming probe's bind fail.
        client.shutdown(socket.SHUT_WR)
        connection.recv(1)  # wait for the client's FIN before closing
    return "pass"


def _receive(connection: socket.socket, expected: bytes) -> None:
    received = b""
    while len(received) < len(expected):
        chunk = connection.recv(len(expected) - len(received))
        if not chunk:
            break
        received += chunk
    if received != expected:
        raise RuntimeError("TCP probe payload differs")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--interfaces", required=True, nargs="+")
    args = parser.parse_args(argv)
    # Same interface choice as the pilot server; raises if none is usable.
    address = resolve_interface_ips(args.interfaces)[0]
    print(probe(address, args.port).model_dump_json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
