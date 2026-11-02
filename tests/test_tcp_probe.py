"""CPU sockets qualify the probe, not Tara's actual HSN or GPU readiness."""

import errno
import json
import os
import socket
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from first_pilot import tcp_probe
from first_pilot.host_commands import host_identity

_SOCKET = socket.socket


def unused_port() -> int:
    with _SOCKET(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("0.0.0.0", 0))
        return int(listener.getsockname()[1])


@contextmanager
def sockets(
    configure: Callable[[MagicMock, int], None] | None = None,
) -> Iterator[tuple[list[socket.socket], list[MagicMock]]]:
    """Observe real descriptors; customize only the operation under test."""
    raw_sockets: list[socket.socket] = []
    wrappers: list[MagicMock] = []

    def track(raw: socket.socket) -> MagicMock:
        wrapped = MagicMock(spec=_SOCKET, wraps=raw)
        wrapped.__enter__.return_value = wrapped
        wrapped.__exit__.side_effect = lambda *_args: raw.close()
        raw_sockets.append(raw)
        wrappers.append(wrapped)

        def accept() -> tuple[MagicMock, tuple[str, int]]:
            connection, peer = raw.accept()
            return track(connection), peer

        wrapped.accept.side_effect = accept
        if configure is not None:
            configure(wrapped, len(wrappers) - 1)
        return wrapped

    def create(
        family: int, kind: int, proto: int = 0, fileno: int | None = None
    ) -> socket.socket | MagicMock:
        assert family == socket.AF_INET and kind == socket.SOCK_STREAM
        if fileno is not None:
            # socket.accept constructs its returned native descriptor through
            # this same module; the accept wrapper tracks it exactly once.
            return _SOCKET(family, kind, proto, fileno=fileno)
        return track(_SOCKET(family, kind))

    with patch("first_pilot.tcp_probe.socket.socket", side_effect=create):
        yield raw_sockets, wrappers
    assert all(raw.fileno() == -1 for raw in raw_sockets)


def test_real_ipv4_exchange_closes_sockets_and_reuses_fixed_port() -> None:
    port = unused_port()
    with sockets() as (raw, wrapped):
        for _ in range(2):
            result = tcp_probe.probe("127.0.0.1", port)
            assert result == {
                "host": host_identity(socket.gethostname()),
                "address": "127.0.0.1",
                "port": port,
                "listening": True,
                "listener_inode": result["listener_inode"],
                "result": "PASS",
                "phase": "complete",
                "accepted": True,
                "pid": os.getpid(),
                "elapsed_sec": result["elapsed_sec"],
            }
            assert isinstance(result["listener_inode"], int)
            assert result["listener_inode"] > 0
            assert isinstance(result["elapsed_sec"], float)
            assert result["elapsed_sec"] >= 0
            assert all(item.fileno() == -1 for item in raw)
        assert len(raw) == 6
        assert all(not item.setsockopt.called for item in wrapped)


@pytest.mark.parametrize("phase", ["connect", "accept"])
def test_only_connect_accept_timeout_returns_observed_listener(phase: str) -> None:
    port = unused_port()

    def configure(wrapped: MagicMock, index: int) -> None:
        if phase == "connect" and index == 1:
            wrapped.connect.side_effect = TimeoutError("controlled connect timeout")
        if phase == "accept" and index == 0:
            wrapped.accept.side_effect = TimeoutError("controlled accept timeout")

    with sockets(configure) as (raw, wrapped):
        result = tcp_probe.probe("127.0.0.1", port, timeout_sec=0.2)
        assert result["result"] == "TIMEOUT" and result["phase"] == phase
        assert result["accepted"] is False and result["listening"] is True
        assert isinstance(result["listener_inode"], int)
        assert result["listener_inode"] > 0
        wrapped[0].getsockopt.assert_called_once_with(
            socket.SOL_SOCKET, socket.SO_ACCEPTCONN
        )
        assert all(item.fileno() == -1 for item in raw)


def test_timeout_without_still_listening_evidence_is_infrastructure_error() -> None:
    port = unused_port()

    def configure(wrapped: MagicMock, index: int) -> None:
        if index == 0:
            wrapped.getsockopt.return_value = 0
        if index == 1:
            wrapped.connect.side_effect = TimeoutError

    with sockets(configure), pytest.raises(RuntimeError, match="listener identity"):
        tcp_probe.probe("127.0.0.1", port)


def test_port_collision_propagates_and_does_not_select_another_port() -> None:
    with _SOCKET(socket.AF_INET, socket.SOCK_STREAM) as occupied:
        occupied.bind(("0.0.0.0", 0))
        occupied.listen(1)
        port = int(occupied.getsockname()[1])
        with sockets() as (_, wrapped), pytest.raises(OSError) as error:
            tcp_probe.probe("127.0.0.1", port)
        assert error.value.errno == errno.EADDRINUSE
        wrapped[0].bind.assert_called_once_with(("0.0.0.0", port))


@pytest.mark.parametrize("operation", ["listen", "connect", "recv"])
def test_other_socket_failures_propagate(operation: str) -> None:
    port = unused_port()

    def configure(wrapped: MagicMock, index: int) -> None:
        if operation == "listen" and index == 0:
            wrapped.listen.side_effect = PermissionError("controlled listen failure")
        elif operation == "connect" and index == 1:
            wrapped.connect.side_effect = ConnectionRefusedError("controlled refusal")
        elif operation == "recv" and index == 2:
            wrapped.recv.side_effect = TimeoutError("payload timeout is not a fault")

    with sockets(configure), pytest.raises(OSError):
        tcp_probe.probe("127.0.0.1", port)


@pytest.mark.parametrize("difference", ["peer", "local", "payload"])
def test_accepted_identity_or_payload_mismatch_is_not_a_node_fault(
    difference: str,
) -> None:
    port = unused_port()

    def configure(wrapped: MagicMock, index: int) -> None:
        if difference == "peer" and index == 0:
            original = wrapped.accept.side_effect

            def wrong_peer() -> tuple[MagicMock, tuple[str, int]]:
                connection, _peer = original()
                return connection, ("127.0.0.2", 1)

            wrapped.accept.side_effect = wrong_peer
        elif difference == "local" and index == 2:
            wrapped.getsockname.return_value = ("127.0.0.2", port)
        elif difference == "payload" and index == 2:
            wrapped.recv.side_effect = lambda size: b"x" * size

    with sockets(configure), pytest.raises(RuntimeError, match="differs"):
        tcp_probe.probe("127.0.0.1", port)


@pytest.mark.parametrize(
    ("address", "port", "timeout"),
    [
        ("::1", 20005, 2.0),
        ("0.0.0.0", 20005, 2.0),
        ("not-an-ip", 20005, 2.0),
        ("127.0.0.1", 0, 2.0),
        ("127.0.0.1", 65536, 2.0),
        ("127.0.0.1", True, 2.0),
        ("127.0.0.1", 20005, 0.0),
        ("127.0.0.1", 20005, float("nan")),
        ("127.0.0.1", 20005, float("inf")),
    ],
)
def test_invalid_configuration_opens_no_sockets(
    address: str, port: int, timeout: float
) -> None:
    with patch("first_pilot.tcp_probe.socket.socket") as create:
        with pytest.raises(ValueError):
            tcp_probe.probe(address, port, timeout)
        create.assert_not_called()


def test_cli_selects_first_configured_interface_and_emits_after_closing(
    capsys: pytest.CaptureFixture[str],
) -> None:
    port = unused_port()
    dumps = json.dumps
    with (
        sockets() as (raw, _wrapped),
        patch(
            "first_pilot.host_commands.resolve_interface_ips",
            return_value=["127.0.0.1", "127.0.0.2"],
        ) as resolve,
    ):

        def after_close(value: object, *, allow_nan: bool) -> str:
            assert len(raw) == 3 and all(item.fileno() == -1 for item in raw)
            return dumps(value, allow_nan=allow_nan)

        with patch("first_pilot.tcp_probe.json.dumps", side_effect=after_close):
            assert (
                tcp_probe.main(["--port", str(port), "--interfaces", "hsn1", "hsn0"])
                == 0
            )
    resolve.assert_called_once_with(["hsn1", "hsn0"])
    captured = capsys.readouterr()
    assert not captured.err
    [line] = captured.out.splitlines()
    assert json.loads(line)["address"] == "127.0.0.1"


def test_cli_interface_failure_does_not_fall_back(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with (
        patch(
            "first_pilot.host_commands.resolve_interface_ips",
            side_effect=RuntimeError("No configured interface"),
        ),
        patch.object(tcp_probe, "probe") as run,
        pytest.raises(RuntimeError, match="configured interface"),
    ):
        tcp_probe.main(["--port", "20005", "--interfaces", "hsn0"])
    run.assert_not_called()
    assert not capsys.readouterr().out
