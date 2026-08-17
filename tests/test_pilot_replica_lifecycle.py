"""Focused tests for cooperative replica quiesce and bounded fallback."""

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest
from pydantic import ValidationError

from first_common.schema.types import (
    GpuClaim,
    HealthCheckParams,
    PilotLaunchSpec,
    ReplicaState,
)
from first_gateway.services.pilot_control import STOP_TIMEOUT
from first_pilot.replica import Replica, ReplicaTeardownError
from first_pilot.replica_manager import ReplicaManager

_SERVE_UNTIL_TERM = """
trap 'exit 0' TERM INT
while true; do
    sleep 0.1
done
"""


def _launch_spec(**overrides: Any) -> PilotLaunchSpec:
    values: dict[str, Any] = {
        "served_model_name": "test-model",
        "gpus_per_node": 2,
        "num_nodes": 1,
        "venv_path": "/immutable/venv",
        "weights_path": "/immutable/weights",
        "weights_cache_path": "/private/cache",
        "env": {"OFFLINE_ONLY": "1"},
        "serve_script_template": _SERVE_UNTIL_TERM,
        "max_startup_sec": 30,
        "health_check": HealthCheckParams(url=""),
    }
    values.update(overrides)
    return PilotLaunchSpec.model_validate(values)


def _replica(tmp_path: Path, spec: PilotLaunchSpec) -> Replica:
    workdir = tmp_path / "replica"
    workdir.mkdir()
    return Replica(
        name="deployment/replica/one",
        uds=str(tmp_path / "replica.sock"),
        resources=[GpuClaim(hostname="node-a", gpu_ids=["0", "1"])],
        launch_spec=spec,
        workdir=workdir,
    )


def _teardown_complete(replica: Replica) -> bool:
    """Read without narrowing the mutable attribute across a stop() call."""
    return replica._teardown_complete


def _state(replica: Replica) -> ReplicaState:
    """Read mutable state without narrowing it across a stop() call."""
    return replica.state


def test_pre_stop_template_validation_and_unhealthy_deadline() -> None:
    with pytest.raises(ValidationError, match="unknown variables"):
        _launch_spec(pre_stop_script_template="echo {{ not_in_context }}")
    with pytest.raises(ValidationError, match="less than or equal to 25"):
        _launch_spec(pre_stop_script_template="true", pre_stop_timeout_sec=26)
    with pytest.raises(ValidationError, match="unknown variables"):
        _launch_spec(post_stop_script_template="echo {{ not_in_context }}")
    with pytest.raises(ValidationError, match="less than or equal to 50"):
        _launch_spec(post_stop_script_template="true", post_stop_timeout_sec=51)

    explicit = Replica.__new__(Replica)
    explicit.launch_spec = _launch_spec(max_unhealthy_sec=7)
    assert explicit._unhealthy_timeout_sec() == 7

    fallback = Replica.__new__(Replica)
    fallback.launch_spec = _launch_spec()
    assert fallback._unhealthy_timeout_sec() == 30


def test_stop_rpc_and_join_budgets_cover_sequential_hooks() -> None:
    assert STOP_TIMEOUT.read == 120.0
    assert ReplicaManager._STOP_JOIN_TIMEOUT == 120.0


def test_process_group_probe_is_fail_closed_on_permission_error() -> None:
    with patch("first_pilot.replica.os.killpg", side_effect=PermissionError):
        assert Replica._process_group_alive(12345)
    with patch("first_pilot.replica.os.killpg", side_effect=ProcessLookupError):
        assert not Replica._process_group_alive(12345)


def test_cooperative_pre_stop_renders_exact_allocation_and_is_idempotent(
    tmp_path: Path,
) -> None:
    spec = _launch_spec(
        pre_stop_script_template="""
printf '%s\n' '{{ replica_name }}|{{ gpus_by_host["node-a"] | join(",") }}|{{ env["OFFLINE_ONLY"] }}' >> quiesced
""",
        pre_stop_timeout_sec=2,
    )
    replica = _replica(tmp_path, spec)
    try:
        replica.stop(timeout=0.2)
        replica.stop(timeout=0.2)
    finally:
        replica.stop(timeout=0.2)

    assert replica.state == ReplicaState.terminated
    assert replica.state_message == (
        "Model replica terminated after cooperative pre-stop hook."
    )
    assert (replica.workdir / "quiesced").read_text().splitlines() == [
        "deployment/replica/one|0,1|1"
    ]
    lifecycle_log = replica.log_path.read_text()
    assert lifecycle_log.count("pre-stop hook started") == 1
    assert lifecycle_log.count("pre-stop hook completed") == 1
    assert not replica._group_alive()


def test_post_stop_runs_only_after_model_absence_and_latches_success(
    tmp_path: Path,
) -> None:
    spec = _launch_spec(
        serve_script_template="""
printf '%s\n' "$$" > model-pgid
trap 'exit 0' TERM INT
while true; do sleep 0.1; done
""",
        post_stop_script_template="""
model_pgid=$(cat model-pgid)
if kill -0 -- "-$model_pgid" 2>/dev/null; then
    exit 70
fi
printf '%s\n' verified >> post-stop-proof
""",
        post_stop_timeout_sec=2,
    )
    replica = _replica(tmp_path, spec)
    try:
        replica.stop(timeout=0.2)
        replica.stop(timeout=0.2)
    finally:
        if not _teardown_complete(replica):
            replica.stop(timeout=0.2)

    assert replica.state == ReplicaState.terminated
    assert replica.state_message == (
        "Model replica terminated after verified post-stop hook."
    )
    assert (replica.workdir / "post-stop-proof").read_text().splitlines() == [
        "verified"
    ]
    assert replica.log_path.read_text().count("post-stop hook completed") == 1


def test_nonzero_post_stop_is_fail_closed_and_retryable(tmp_path: Path) -> None:
    replica = _replica(
        tmp_path,
        _launch_spec(
            post_stop_script_template="""
if [ ! -e permit-post-stop ]; then
    exit 7
fi
exit 0
""",
            post_stop_timeout_sec=2,
        ),
    )
    try:
        with pytest.raises(ReplicaTeardownError, match="post-stop"):
            replica.stop(timeout=0.2)
        assert not _teardown_complete(replica)
        assert not replica._log_fh.closed
        assert replica._post_stop_attempts == 1

        (replica.workdir / "permit-post-stop").touch()
        replica.stop(timeout=0.2)
        assert _teardown_complete(replica)
        assert replica._post_stop_attempts == 2
        assert replica.state_message == (
            "Model replica terminated after verified post-stop hook."
        )
    finally:
        if not _teardown_complete(replica):
            (replica.workdir / "permit-post-stop").touch()
            replica.stop(timeout=0.2)


def test_timed_out_post_stop_cleans_group_but_requires_new_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(Replica, "_HOOK_TERM_GRACE", 0.05)
    monkeypatch.setattr(Replica, "_HOOK_KILL_GRACE", 0.2)
    monkeypatch.setattr(Replica, "_GROUP_POLL_INTERVAL", 0.01)
    replica = _replica(
        tmp_path,
        _launch_spec(
            post_stop_script_template="""
printf '%s\n' "$$" > post-hook-pgid
trap '' TERM
while true; do sleep 1; done
""",
            post_stop_timeout_sec=0.3,
        ),
    )
    try:
        with pytest.raises(ReplicaTeardownError, match="post-stop"):
            replica.stop(timeout=0.2)
        hook_pgid = int((replica.workdir / "post-hook-pgid").read_text())
        assert not Replica._process_group_alive(hook_pgid)
        assert not _teardown_complete(replica)

        replica._post_stop_script_path.write_text("exit 0\n")  # type: ignore[union-attr]
        replica.stop(timeout=0.2)
        assert replica._post_stop_attempts == 2
    finally:
        if not _teardown_complete(replica):
            replica._post_stop_script_path.write_text("exit 0\n")  # type: ignore[union-attr]
            replica.stop(timeout=0.2)


def test_post_stop_descendant_is_removed_before_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(Replica, "_HOOK_TERM_GRACE", 0.05)
    monkeypatch.setattr(Replica, "_HOOK_KILL_GRACE", 0.2)
    monkeypatch.setattr(Replica, "_GROUP_POLL_INTERVAL", 0.01)
    replica = _replica(
        tmp_path,
        _launch_spec(
            post_stop_script_template="""
if [ ! -e retry-post-stop ]; then
    printf '%s\n' "$$" > post-descendant-pgid
    (trap '' TERM HUP; while true; do sleep 1; done) &
    exit 0
fi
old_pgid=$(cat post-descendant-pgid)
if kill -0 -- "-$old_pgid" 2>/dev/null; then
    exit 71
fi
exit 0
""",
            post_stop_timeout_sec=2,
        ),
    )
    try:
        with pytest.raises(ReplicaTeardownError, match="post-stop"):
            replica.stop(timeout=0.2)
        old_pgid = int((replica.workdir / "post-descendant-pgid").read_text())
        assert not Replica._process_group_alive(old_pgid)

        (replica.workdir / "retry-post-stop").touch()
        replica.stop(timeout=0.2)
        assert _teardown_complete(replica)
        assert replica._post_stop_attempts == 2
    finally:
        if not _teardown_complete(replica):
            (replica.workdir / "retry-post-stop").touch()
            replica.stop(timeout=0.2)


def test_nonzero_pre_stop_uses_process_group_fallback(tmp_path: Path) -> None:
    replica = _replica(
        tmp_path,
        _launch_spec(pre_stop_script_template="exit 7", pre_stop_timeout_sec=2),
    )
    try:
        replica.stop(timeout=0.2)
    finally:
        replica.stop(timeout=0.2)

    assert replica.state == ReplicaState.terminated
    assert "process-group fallback" in replica.state_message
    assert "pre-stop hook exited 7; using fallback" in replica.log_path.read_text()
    assert not replica._group_alive()


def test_timed_out_pre_stop_kills_hook_tree_then_model_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(Replica, "_TERM_GRACE", 0.05)
    monkeypatch.setattr(Replica, "_KILL_GRACE", 0.2)
    monkeypatch.setattr(Replica, "_HOOK_TERM_GRACE", 0.05)
    monkeypatch.setattr(Replica, "_HOOK_KILL_GRACE", 0.2)
    monkeypatch.setattr(Replica, "_GROUP_POLL_INTERVAL", 0.01)

    replica = _replica(
        tmp_path,
        _launch_spec(
            pre_stop_script_template="""
trap '' TERM
while true; do
    sleep 1
done
""",
            pre_stop_timeout_sec=0.05,
        ),
    )
    try:
        replica.stop(timeout=0.2)
    finally:
        replica.stop(timeout=0.2)

    assert replica.state == ReplicaState.terminated
    assert "process-group fallback" in replica.state_message
    assert "pre-stop hook timed out; using fallback" in replica.log_path.read_text()
    assert not replica._group_alive()


def test_hook_leader_exit_cannot_leave_term_ignoring_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(Replica, "_HOOK_TERM_GRACE", 0.05)
    monkeypatch.setattr(Replica, "_HOOK_KILL_GRACE", 0.2)
    monkeypatch.setattr(Replica, "_GROUP_POLL_INTERVAL", 0.01)

    replica = _replica(
        tmp_path,
        _launch_spec(
            pre_stop_script_template="""
printf '%s\n' "$$" > hook-pgid
(
    trap '' TERM HUP
    while true; do
        sleep 1
    done
) &
exit 0
""",
            pre_stop_timeout_sec=2,
        ),
    )
    try:
        replica.stop(timeout=0.2)
    finally:
        replica.stop(timeout=0.2)

    hook_pgid = int((replica.workdir / "hook-pgid").read_text())
    assert "process-group fallback" in replica.state_message
    assert "left descendant processes; using fallback" in replica.log_path.read_text()
    assert not Replica._process_group_alive(hook_pgid)
    assert not replica._group_alive()


def test_model_group_survival_is_not_success_and_stop_is_retryable(
    tmp_path: Path,
) -> None:
    replica = _replica(tmp_path, _launch_spec())
    try:
        with patch.object(replica, "_terminate_process_group", return_value=False):
            with pytest.raises(ReplicaTeardownError, match="model process group"):
                replica.stop(timeout=0.2)

        assert _state(replica) == ReplicaState.terminating
        assert not _teardown_complete(replica)
        assert not replica._log_fh.closed
        assert replica.proc.poll() is None

        # A later controller retry uses the same Replica/PGID and can complete
        # authoritatively; the cooperative hook is never rerun.
        replica.stop(timeout=0.2)
        assert _teardown_complete(replica)
        assert _state(replica) == ReplicaState.terminated
        assert replica._log_fh.closed
    finally:
        if not _teardown_complete(replica):
            replica.stop(timeout=0.2)


def test_model_group_surviving_sigkill_returns_failure(tmp_path: Path) -> None:
    replica = _replica(tmp_path, _launch_spec())
    try:
        with (
            patch.object(replica, "_group_alive", return_value=True),
            patch.object(
                replica,
                "_wait_for_group_exit",
                side_effect=[False, False],
            ),
            patch("first_pilot.replica.os.killpg") as killpg,
        ):
            assert not replica._terminate_process_group()

        assert killpg.call_args_list == [
            call(replica._pgid, 15),
            call(replica._pgid, 9),
        ]
    finally:
        replica.stop(timeout=0.2)


def test_permission_denied_hook_kill_is_retained_as_alive() -> None:
    replica = Replica.__new__(Replica)
    replica._GROUP_POLL_INTERVAL = 0.001
    replica._HOOK_TERM_GRACE = 0.001
    replica._HOOK_KILL_GRACE = 0.001
    replica._hook_cleanup_attempts = 0
    replica._hook_survivors = None
    hook = MagicMock()
    hook.pid = 12345

    with (
        patch.object(replica, "_process_group_alive", return_value=True),
        patch.object(replica, "_wait_for_process_group_exit", return_value=False),
        patch("first_pilot.replica.os.killpg", side_effect=PermissionError) as killpg,
    ):
        assert not replica._terminate_hook_group(hook)

    assert killpg.call_args_list == [call(12345, 15), call(12345, 9)]


def test_retry_refuses_signal_when_process_group_identity_changed() -> None:
    replica = Replica.__new__(Replica)
    leader = MagicMock()
    leader.pid = 12345
    leader.poll.return_value = 1

    with (
        patch.object(
            replica,
            "_snapshot_process_group",
            return_value={45678: 222},
        ),
        patch("first_pilot.replica.os.getpgid") as getpgid,
        patch("first_pilot.replica.os.getsid") as getsid,
    ):
        assert not replica._retry_owns_process_group(
            12345,
            leader,
            {45678: 111},
            label="model",
        )

    getpgid.assert_not_called()
    getsid.assert_not_called()
