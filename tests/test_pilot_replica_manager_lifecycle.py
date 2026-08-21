"""Authoritative ReplicaManager release and bounded stop-all tests."""

import subprocess
import sys
import threading
import time
from typing import cast
from unittest.mock import MagicMock

import pytest

from first_common.errors import NotFound, ReplicaTeardownError
from first_common.schema.types import GpuClaim
from first_pilot.replica import Replica
from first_pilot.replica_manager import ReplicaManager


def _mock_replica(name: str, gpu: str) -> tuple[Replica, MagicMock]:
    mock = MagicMock(spec=Replica)
    mock.name = name
    mock.resources = [GpuClaim(hostname="node", gpu_ids=[gpu])]
    return cast(Replica, mock), mock


def _manager(*replicas: Replica) -> ReplicaManager:
    manager = ReplicaManager.__new__(ReplicaManager)
    manager._lock = threading.Lock()
    manager._replicas = {replica.name: replica for replica in replicas}
    manager._claimed = set()
    for replica in replicas:
        manager._claimed.update(manager._flatten(replica.resources))
    return manager


def test_failed_stop_retains_claims_for_retry() -> None:
    replica, mock = _mock_replica("replica", "0")
    mock.stop.side_effect = [ReplicaTeardownError("group survived"), None]
    manager = _manager(replica)

    with pytest.raises(ReplicaTeardownError, match="group survived"):
        manager.stop_replica("replica")

    assert manager.get_replica("replica") is replica
    assert manager._claimed == {("node", "0")}

    manager.stop_replica("replica")
    with pytest.raises(NotFound):
        manager.get_replica("replica")
    assert manager._claimed == set()


def test_late_duplicate_release_does_not_clear_another_replicas_claim() -> None:
    replica_a, _ = _mock_replica("replica-a", "0")
    manager = _manager(replica_a)

    manager.stop_replica("replica-a")
    assert manager._claimed == set()

    replica_b, _ = _mock_replica("replica-b", "0")
    with manager._lock:
        manager._replicas[replica_b.name] = replica_b
        manager._claimed.update(manager._flatten(replica_b.resources))

        # A late duplicate completion for A must not clear GPU 0, which B now
        # owns under a different unique replica name.
        assert not manager._release_locked("replica-a", replica_a.resources)

    assert manager.get_replica("replica-b") is replica_b
    assert manager._claimed == {("node", "0")}


def test_stop_all_is_bounded_and_releases_only_completed_replicas() -> None:
    success, success_mock = _mock_replica("success", "0")
    failure, failure_mock = _mock_replica("failure", "1")
    blocked, blocked_mock = _mock_replica("blocked", "2")
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


def test_permanently_blocked_stop_does_not_hold_interpreter_exit() -> None:
    script = """
import threading

from first_common.schema.types import GpuClaim
from first_pilot.replica_manager import ReplicaManager


class PermanentlyBlockedReplica:
    name = "blocked"
    resources = [GpuClaim(hostname="node", gpu_ids=["0"])]

    def stop(self):
        threading.Event().wait()


replica = PermanentlyBlockedReplica()
manager = ReplicaManager.__new__(ReplicaManager)
manager._lock = threading.Lock()
manager._replicas = {replica.name: replica}
manager._claimed = {("node", "0")}
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
