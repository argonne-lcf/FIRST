"""One bounded, GPU-free IPv4 self-connect observation on a configured interface.

The listener deliberately binds the wildcard address, like TCPStore. Only a
connect/accept timeout while that exact socket is still listening is returned
as a TIMEOUT observation. Configuration, binding and payload errors propagate;
the caller decides whether a repeated observation establishes a node fault.
"""

import argparse
import ipaddress
import json
import math
import os
import socket
import time

from .host_commands import host_identity

_PAYLOAD = b"FIRST IPv4 admission probe\n"


def _receive(connection: socket.socket, expected: bytes) -> None:
    received = b""
    while len(received) < len(expected):
        chunk = connection.recv(len(expected) - len(received))
        if not chunk:
            raise RuntimeError("TCP probe payload ended early")
        received += chunk
    if received != expected:
        raise RuntimeError("TCP probe payload differs")


def _observation(
    listener: socket.socket,
    address: str,
    port: int,
    result: str,
    phase: str,
    accepted: bool,
) -> dict[str, object]:
    inode = os.fstat(listener.fileno()).st_ino
    if (
        listener.getsockopt(socket.SOL_SOCKET, socket.SO_ACCEPTCONN) != 1
        or listener.getsockname() != ("0.0.0.0", port)
        or inode <= 0
    ):
        raise RuntimeError("TCP probe listener identity is not established")
    host = host_identity(socket.gethostname())
    if not host:
        raise RuntimeError("TCP probe hostname is unavailable")
    return {
        "host": host,
        "address": address,
        "port": port,
        "listening": True,
        "listener_inode": inode,
        "result": result,
        "phase": phase,
        "accepted": accepted,
        "pid": os.getpid(),
    }


def probe(address: str, port: int, timeout_sec: float = 2.0) -> dict[str, object]:
    """Close every socket before returning PASS or a verified TIMEOUT snapshot."""
    address = str(ipaddress.IPv4Address(address))
    if ipaddress.IPv4Address(address).is_unspecified:
        raise ValueError("TCP probe requires an interface IPv4 address")
    if type(port) is not int or not 1 <= port <= 65535:
        raise ValueError("TCP probe port must be between 1 and 65535")
    if not math.isfinite(timeout_sec) or timeout_sec <= 0:
        raise ValueError("TCP probe timeout must be positive and finite")

    started = time.monotonic()
    with (
        socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener,
        socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client,
    ):
        listener.settimeout(timeout_sec)
        client.settimeout(timeout_sec)
        # No reuse options or alternate ports: collisions are infrastructure
        # errors, not evidence that this node cannot connect to itself.
        listener.bind(("0.0.0.0", port))
        listener.listen(1)
        try:
            client.connect((address, port))
        except TimeoutError:
            result = _observation(listener, address, port, "TIMEOUT", "connect", False)
        else:
            try:
                connection, peer = listener.accept()
            except TimeoutError:
                result = _observation(
                    listener, address, port, "TIMEOUT", "accept", False
                )
            else:
                with connection:
                    connection.settimeout(timeout_sec)
                    if peer != client.getsockname() or connection.getsockname() != (
                        address,
                        port,
                    ):
                        raise RuntimeError("TCP probe accepted connection differs")
                    client.sendall(_PAYLOAD)
                    _receive(connection, _PAYLOAD)
                    connection.sendall(_PAYLOAD)
                    _receive(client, _PAYLOAD)
                    # Close the client first, leaving TIME_WAIT on its ephemeral
                    # port rather than preventing a confirmation on this port.
                    client.shutdown(socket.SHUT_WR)
                    if connection.recv(1):
                        raise RuntimeError("TCP probe received trailing payload")
                    connection.shutdown(socket.SHUT_WR)
                    if client.recv(1):
                        raise RuntimeError("TCP probe received trailing response")
                result = _observation(listener, address, port, "PASS", "complete", True)
    result["elapsed_sec"] = time.monotonic() - started
    return result


def main(argv: list[str] | None = None) -> int:
    from .host_commands import resolve_interface_ips

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--interfaces", required=True, nargs="+")
    args = parser.parse_args(argv)
    # Reuse the pilot's interface order and IPv4 selection, never DNS or a
    # management/localhost fallback. Unit tests may explicitly select loopback.
    addresses = resolve_interface_ips(args.interfaces)
    if not addresses:
        raise RuntimeError("No configured TCP probe interface has an IPv4 address")
    print(json.dumps(probe(addresses[0], args.port), allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
