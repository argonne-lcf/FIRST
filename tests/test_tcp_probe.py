"""The TCP self-connect probe, on loopback. This tests the probe, not an HSN."""

import errno
import socket
from unittest.mock import patch

import pytest

from first_pilot import tcp_probe
from first_pilot.host_commands import host_identity
from first_pilot.tcp_probe import Observation


def unused_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("0.0.0.0", 0))
        return int(listener.getsockname()[1])


def test_pass_twice_on_the_same_port() -> None:
    # Each probe binds the same port without SO_REUSEADDR, so this also checks
    # that the one before it left nothing on the port: no TIME_WAIT, and no
    # accepted socket still closing (a race, hence the repeats).
    port = unused_port()
    for _ in range(50):
        result = tcp_probe.probe("127.0.0.1", port)
        assert result.result == "pass"
        assert result.host == host_identity(socket.gethostname())
        assert str(result.address) == "127.0.0.1" and result.port == port


@pytest.mark.parametrize(
    ("method", "expected"),
    [("connect", "connect_timeout"), ("accept", "accept_timeout")],
)
def test_timeout_is_a_result(method: str, expected: str) -> None:
    with patch.object(socket.socket, method, side_effect=TimeoutError):
        result = tcp_probe.probe("127.0.0.1", unused_port(), timeout_sec=0.2)
    assert result.result == expected


def test_port_in_use_raises() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as occupied:
        occupied.bind(("0.0.0.0", 0))
        occupied.listen(1)
        with pytest.raises(OSError) as error:
            tcp_probe.probe("127.0.0.1", occupied.getsockname()[1])
    assert error.value.errno == errno.EADDRINUSE


def test_refused_connection_raises() -> None:
    with (
        patch.object(socket.socket, "connect", side_effect=ConnectionRefusedError),
        pytest.raises(ConnectionRefusedError),
    ):
        tcp_probe.probe("127.0.0.1", unused_port())


def test_wrong_payload_raises() -> None:
    with (
        patch.object(socket.socket, "recv", side_effect=lambda size: b"x" * size),
        pytest.raises(RuntimeError, match="payload differs"),
    ):
        tcp_probe.probe("127.0.0.1", unused_port())


def test_cli_probes_the_first_configured_interface(
    capsys: pytest.CaptureFixture[str],
) -> None:
    port = unused_port()
    with patch(
        "first_pilot.tcp_probe.resolve_interface_ips",
        return_value=["127.0.0.1", "127.0.0.2"],
    ) as resolve:
        assert (
            tcp_probe.main(["--port", str(port), "--interfaces", "hsn1", "hsn0"]) == 0
        )
    resolve.assert_called_once_with(["hsn1", "hsn0"])
    [line] = capsys.readouterr().out.splitlines()
    result = Observation.model_validate_json(line)
    assert str(result.address) == "127.0.0.1" and result.result == "pass"


def test_cli_without_a_usable_interface_raises(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with (
        patch(
            "first_pilot.tcp_probe.resolve_interface_ips",
            side_effect=RuntimeError("no usable interface"),
        ),
        patch.object(tcp_probe, "probe") as probe,
        pytest.raises(RuntimeError, match="no usable interface"),
    ):
        tcp_probe.main(["--port", "20005", "--interfaces", "hsn0"])
    probe.assert_not_called()
    assert not capsys.readouterr().out
