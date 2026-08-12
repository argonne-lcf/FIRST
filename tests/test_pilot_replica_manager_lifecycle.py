"""Authoritative ReplicaManager release and bounded stop-all tests."""

import subprocess
import sys
import threading
import time
from typing import cast
from unittest.mock import MagicMock

import pytest

from first_common.errors import NotFound
from first_common.schema.types import GpuClaim
from first_pilot.replica import Replica, ReplicaTeardownError
from first_pilot.replica_manager import ReplicaManager


def _mock_replica(name: str, gpu: str, port: int) -> tuple[Replica, MagicMock]:
    mock = MagicMock(spec=Replica)
    mock.name = name
    mock.resources = [GpuClaim(hostname="node", gpu_ids=[gpu])]
    mock.port = port
    return cast(Replica, mock), mock


def _manager(*replicas: Replica) -> ReplicaManager:
    manager = ReplicaManager.__new__(ReplicaManager)
    manager._lock = threading.Lock()
    manager._replicas = {replica.name: replica for replica in replicas}
    manager._claimed = set()
    manager._used_ports = set()
    for replica in replicas:
        manager._claimed.update(manager._flatten(replica.resources))
        manager._used_ports.add(replica.port)
    return manager


def test_failed_stop_retains_address_claims_and_port_for_retry() -> None:
    replica, mock = _mock_replica("replica", "0", 18123)
    mock.stop.side_effect = [ReplicaTeardownError("group survived"), None]
    manager = _manager(replica)

    with pytest.raises(ReplicaTeardownError, match="group survived"):
        manager.stop_replica("replica")

    assert manager.get_replica("replica") is replica
    assert manager._claimed == {("node", "0")}
    assert manager._used_ports == {18123}

    manager.stop_replica("replica")
    with pytest.raises(NotFound):
        manager.get_replica("replica")
    assert manager._claimed == set()
    assert manager._used_ports == set()


class _SerializedReplica:
    def __init__(self, name: str, gpu: str, port: int) -> None:
        self.name = name
        self.resources = [GpuClaim(hostname="node", gpu_ids=[gpu])]
        self.port = port
        self._stop_lock = threading.Lock()
        self._attempt_lock = threading.Lock()
        self._attempts = 0
        self.first_inside = threading.Event()
        self.second_attempted = threading.Event()
        self.second_inside = threading.Event()
        self.allow_first = threading.Event()
        self.allow_second = threading.Event()

    def stop(self) -> None:
        with self._attempt_lock:
            self._attempts += 1
            attempt = self._attempts
            if attempt == 2:
                self.second_attempted.set()

        with self._stop_lock:
            if attempt == 1:
                self.first_inside.set()
                assert self.allow_first.wait(timeout=2)
            else:
                self.second_inside.set()
                assert self.allow_second.wait(timeout=2)


def test_late_concurrent_stop_cannot_release_replacement_identity() -> None:
    old_impl = _SerializedReplica("replica", "0", 18123)
    old = cast(Replica, old_impl)
    manager = _manager(old)
    errors: list[BaseException] = []

    def stop() -> None:
        try:
            manager.stop_replica("replica")
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    first = threading.Thread(target=stop)
    second = threading.Thread(target=stop)
    first.start()
    assert old_impl.first_inside.wait(timeout=2)
    second.start()
    assert old_impl.second_attempted.wait(timeout=2)

    old_impl.allow_first.set()
    first.join(timeout=2)
    assert not first.is_alive()
    assert old_impl.second_inside.wait(timeout=2)

    replacement_impl = _SerializedReplica("replica", "0", 18123)
    replacement = cast(Replica, replacement_impl)
    with manager._lock:
        manager._replicas[replacement.name] = replacement
        manager._claimed.update(manager._flatten(replacement.resources))
        manager._used_ports.add(replacement.port)

    old_impl.allow_second.set()
    second.join(timeout=2)
    assert not second.is_alive()
    assert errors == []
    assert manager.get_replica("replica") is replacement
    assert manager._claimed == {("node", "0")}
    assert manager._used_ports == {18123}


def test_stop_all_is_bounded_and_releases_only_completed_replicas() -> None:
    success, success_mock = _mock_replica("success", "0", 18123)
    failure, failure_mock = _mock_replica("failure", "1", 18124)
    blocked, blocked_mock = _mock_replica("blocked", "2", 18125)
    failure_mock.stop.side_effect = ReplicaTeardownError("survived SIGKILL")

    release_blocked = threading.Event()
    blocked_done = threading.Event()

    def block() -> None:
        release_blocked.wait(timeout=2)
        blocked_done.set()

    blocked_mock.stop.side_effect = block
    manager = _manager(success, failure, blocked)
    manager._STOP_JOIN_TIMEOUT = 0.05

    started = time.monotonic()
    with pytest.raises(RuntimeError, match="did not stop authoritatively") as exc:
        manager.stop_all()
    elapsed = time.monotonic() - started

    assert elapsed < 0.5
    assert "failure: survived SIGKILL" in str(exc.value)
    assert "blocked: teardown timed out" in str(exc.value)
    with pytest.raises(NotFound):
        manager.get_replica("success")
    assert manager.get_replica("failure") is failure
    assert manager.get_replica("blocked") is blocked
    assert manager._claimed == {("node", "1"), ("node", "2")}
    assert manager._used_ports == {18124, 18125}
    success_mock.stop.assert_called_once_with()

    # A timed-out daemon worker can still complete later while the manager is
    # live. Its success releases only its own exact mapping and reservation.
    release_blocked.set()
    assert blocked_done.wait(timeout=2)
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        try:
            manager.get_replica("blocked")
        except NotFound:
            break
        time.sleep(0.01)
    else:  # pragma: no cover - diagnostic failure branch
        pytest.fail("late successful stop did not release the blocked replica")

    assert manager.get_replica("failure") is failure
    assert manager._claimed == {("node", "1")}
    assert manager._used_ports == {18124}


def test_permanently_blocked_stop_does_not_hold_interpreter_exit() -> None:
    script = """
import threading

from first_common.schema.types import GpuClaim
from first_pilot.replica_manager import ReplicaManager


class PermanentlyBlockedReplica:
    name = "blocked"
    resources = [GpuClaim(hostname="node", gpu_ids=["0"])]
    port = 18123

    def stop(self):
        threading.Event().wait()


replica = PermanentlyBlockedReplica()
manager = ReplicaManager.__new__(ReplicaManager)
manager._lock = threading.Lock()
manager._replicas = {replica.name: replica}
manager._claimed = {("node", "0")}
manager._used_ports = {replica.port}
manager._STOP_JOIN_TIMEOUT = 0.02

try:
    manager.stop_all()
except RuntimeError:
    pass
else:
    raise AssertionError("permanently blocked teardown reported success")

print("bounded stop returned")
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "bounded stop returned"
