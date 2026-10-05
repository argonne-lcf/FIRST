import socket
from collections.abc import Iterator
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from first_pilot.control_api import resolve_interface_ips


def _addr(family: int, address: str) -> SimpleNamespace:
    return SimpleNamespace(family=family, address=address)


ADDRS = {
    "hsn0": [_addr(socket.AF_INET, "10.0.0.10")],
    "hsn1": [
        _addr(socket.AF_INET6, "fe80::1"),
        _addr(socket.AF_INET, "10.0.1.10"),
    ],
    "hsn2": [_addr(socket.AF_INET, "10.0.2.10")],
    "hsn3": [_addr(socket.AF_INET6, "fe80::3")],
}
STATS = {
    "hsn0": SimpleNamespace(isup=True),
    "hsn1": SimpleNamespace(isup=True),
    "hsn2": SimpleNamespace(isup=False),
    "hsn3": SimpleNamespace(isup=True),
}


@pytest.fixture(autouse=True)
def interfaces() -> Iterator[None]:
    with (
        patch("first_pilot.control_api.psutil.net_if_addrs", return_value=ADDRS),
        patch("first_pilot.control_api.psutil.net_if_stats", return_value=STATS),
    ):
        yield


def test_resolves_in_configured_order() -> None:
    assert resolve_interface_ips(["hsn1", "hsn0"]) == ["10.0.1.10", "10.0.0.10"]


def test_healthy_hsn0_is_advertised_first() -> None:
    ips = resolve_interface_ips(["hsn0", "hsn1", "hsn2", "hsn3"])
    assert ips == ["10.0.0.10", "10.0.1.10"]


@pytest.mark.parametrize("bad", ["missing0", "hsn2", "hsn3"])
def test_skips_missing_down_or_ipv6_only_interfaces(bad: str) -> None:
    assert resolve_interface_ips([bad, "hsn1"]) == ["10.0.1.10"]


def test_fails_when_no_interface_resolves() -> None:
    with pytest.raises(RuntimeError, match="None of the configured"):
        resolve_interface_ips(["missing0", "hsn2", "hsn3"])
