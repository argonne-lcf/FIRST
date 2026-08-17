import logging
import os
import shlex
import signal
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from httpx import Client, HTTPTransport
from jinja2 import Environment, StrictUndefined

from first_common.health import perform_health_check_sync
from first_common.schema.types import (
    GpuClaim,
    HealthCheckResult,
    PilotLaunchSpec,
    ReplicaState,
    ScriptTemplateContext,
)

logger = logging.getLogger(__name__)


class ReplicaTeardownError(RuntimeError):
    """The bounded teardown attempt ended with a process group still present."""


def tail_file(
    path: Path,
    num_lines: int = 200,
    max_bytes: int = 1024 * 1024,
) -> str:
    """
    Return the last `num_lines` of `path`, scanning at most `max_bytes` from the
    end. Missing files return an empty string.
    """
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            start = max(0, size - max_bytes)
            f.seek(start)
            data = f.read()
    except FileNotFoundError:
        return ""

    text = data.decode("utf-8", errors="replace")
    lines = text.splitlines(keepends=True)
    # If we truncated, drop the partial first line
    if start > 0 and lines:
        lines = lines[1:]
    return "".join(lines[-num_lines:])


class Replica:
    """
    Handle to a model replica subprocess and its health-monitor daemon thread.
    """

    _HEALTH_INTERVAL = 2.0
    _TERM_GRACE = 8.0
    _KILL_GRACE = 5.0
    _HOOK_TERM_GRACE = 1.0
    _HOOK_KILL_GRACE = 1.0
    _GROUP_POLL_INTERVAL = 0.2

    def __init__(
        self,
        name: str,
        uds: str,
        resources: list[GpuClaim],
        launch_spec: PilotLaunchSpec,
        workdir: Path,
    ) -> None:
        self.name = name
        self.uds = uds
        self.resources = resources
        self.launch_spec = launch_spec
        self.workdir = workdir

        self.log_path = workdir / "out.log"

        script_path = self.workdir / "serve.sh"
        script_path.write_text(
            self._render_script(self.launch_spec.serve_script_template)
        )
        script_path.chmod(0o755)

        self._pre_stop_script_path: Path | None = None
        if self.launch_spec.pre_stop_script_template is not None:
            self._pre_stop_script_path = self.workdir / "pre-stop.sh"
            self._pre_stop_script_path.write_text(
                self._render_script(self.launch_spec.pre_stop_script_template)
            )
            self._pre_stop_script_path.chmod(0o700)

        self._post_stop_script_path: Path | None = None
        if self.launch_spec.post_stop_script_template is not None:
            self._post_stop_script_path = self.workdir / "post-stop.sh"
            self._post_stop_script_path.write_text(
                self._render_script(self.launch_spec.post_stop_script_template)
            )
            self._post_stop_script_path.chmod(0o700)

        self._log_fh = open(self.log_path, "ab")

        self._env = os.environ.copy()
        self._env.update(self.launch_spec.env)

        logger.info(
            "starting replica %s on uds %s (workdir=%s)",
            self.name,
            self.uds,
            workdir,
        )
        try:
            self.proc = subprocess.Popen(
                ["/bin/bash", str(script_path)],
                cwd=str(self.workdir),
                stdout=self._log_fh,
                stderr=subprocess.STDOUT,
                env=self._env,
                start_new_session=True,
            )
        except Exception:
            logger.exception("failed to Popen model replica %s", self.name)
            self._close_log_handles()
            raise

        # start_new_session=True makes the child its own session/group leader
        self._pgid = self.proc.pid

        self.state = ReplicaState.launching
        self.state_message = "Model startup script has begun."
        self.started_at = datetime.now(timezone.utc)
        self._startup_deadline = time.monotonic() + self.launch_spec.max_startup_sec

        self.consecutive_health_ok = 0
        self.consecutive_health_fail = 0
        self._unhealthy_since: float | None = None
        self._health_client = Client(transport=HTTPTransport(uds=self.uds))

        self._health_params = self.launch_spec.health_check
        self._health_debounce = max(5, self._health_params.debounce)
        if self._health_params.url:
            path = urlparse(self._health_params.url).path
            url = f"http://localhost/{path.lstrip('/')}"
            self._health_params.url = url
            logger.info(f"Replica will monitor health at {url}")
        else:
            logger.info("Replica health check disabled")

        self._teardown_lock = threading.Lock()
        self._stop_lock = threading.Lock()
        self._teardown_started: bool = False
        self._teardown_complete: bool = False
        self._pre_stop_attempted: bool = False
        self._pre_stop_proc: subprocess.Popen[bytes] | None = None
        self._pre_stop_succeeded: bool | None = None
        self._post_stop_proc: subprocess.Popen[bytes] | None = None
        self._post_stop_succeeded: bool | None = None
        self._post_stop_attempts = 0
        self._model_teardown_attempts = 0
        self._hook_cleanup_attempts = 0
        self._post_hook_cleanup_attempts = 0
        self._model_survivors: dict[int, int] | None = None
        self._hook_survivors: dict[int, int] | None = None
        self._post_hook_survivors: dict[int, int] | None = None
        self._monitor_exit = threading.Event()
        self._monitor = threading.Thread(
            target=self._monitor_loop,
            name=f"replica-monitor-{self.name}",
            daemon=True,
        )
        self._monitor.start()

    def _render_script(self, template: str) -> str:
        spec = self.launch_spec

        gpus_by_host: dict[str, list[str]] = {}
        for claim in self.resources:
            gpus_by_host.setdefault(claim.hostname, []).extend(claim.gpu_ids)

        context: ScriptTemplateContext = {
            "replica_name": self.name,
            "served_model_name": spec.served_model_name,
            "uds": self.uds,
            "gpus_per_node": spec.gpus_per_node,
            "num_nodes": spec.num_nodes,
            "gpus_by_host": gpus_by_host,
            "venv_path": str(spec.venv_path),
            "weights_path": str(spec.weights_path),
            "weights_cache_path": str(spec.weights_cache_path),
            "env": spec.env,
            "quote": shlex.quote,
        }

        env = Environment(undefined=StrictUndefined)
        return env.from_string(template).render(**context)

    def _check_health(self) -> HealthCheckResult:
        if not self._health_params.url:
            # No health endpoint -> trust the process: alive == healthy.
            return HealthCheckResult.healthy

        return perform_health_check_sync(self._health_client, self._health_params)

    def _record_health(self, health: HealthCheckResult) -> None:
        if health == HealthCheckResult.healthy:
            self.consecutive_health_ok += 1
            self.consecutive_health_fail = 0
        else:
            self.consecutive_health_ok = 0
            self.consecutive_health_fail += 1

    def _unhealthy_for_too_long(self) -> bool:
        if self._unhealthy_since is None:
            return False
        elapsed = time.monotonic() - self._unhealthy_since
        return elapsed > self._unhealthy_timeout_sec()

    def _unhealthy_timeout_sec(self) -> int:
        timeout = self.launch_spec.max_unhealthy_sec
        return self.launch_spec.max_startup_sec if timeout is None else timeout

    def _monitor_loop(self) -> None:
        while not self._monitor_exit.wait(timeout=self._HEALTH_INTERVAL):
            try:
                self._run_monitor_check()
            except ReplicaTeardownError:
                # The teardown path already recorded exact surviving groups in
                # state_message and the lifecycle log.  Keep the Replica
                # addressable so a later controller retry can finish cleanup.
                logger.exception("teardown incomplete for replica %s", self.name)
                if self.state != ReplicaState.terminating:
                    self.state = ReplicaState.error
                return
            except Exception:
                logger.exception(
                    "uncaught exception in monitor thread for %s", self.name
                )
                self.state = ReplicaState.error
                try:
                    self._shutdown()
                except ReplicaTeardownError:
                    logger.exception(
                        "fallback teardown incomplete for replica %s", self.name
                    )
                return

    def _run_monitor_check(self) -> None:
        rc = self.proc.poll()
        if rc is not None:
            self._handle_process_exit(rc)
            return

        health = self._check_health()
        self._record_health(health)
        self._advance_state(health)

    def _handle_process_exit(self, rc: int) -> None:
        expected_stop = self.state in (
            ReplicaState.terminating,
            ReplicaState.terminated,
        )
        if not expected_stop:
            log = self.get_logs(num_lines=10)
            msg = (
                f"Model replica {self.name} exited unexpectedly with code {rc}:\n{log}"
            )
            logger.error(msg)
            self.state = ReplicaState.error
            self.state_message = msg

        # The leader is gone but may have left GPU-pinned children behind:
        self._shutdown()
        if expected_stop:
            self.state = ReplicaState.terminated
            self.state_message = "Model replica has terminated."

    def _advance_state(self, health: HealthCheckResult) -> None:
        healthy = health == HealthCheckResult.healthy

        if self.state == ReplicaState.launching:
            if healthy:
                elapsed = (datetime.now(timezone.utc) - self.started_at).total_seconds()
                logger.info(
                    msg := f"Replica {self.name} ready after {elapsed:.1f} seconds"
                )
                self.state_message = msg
                self.state = ReplicaState.ready
            elif time.monotonic() > self._startup_deadline:
                log = self.get_logs(num_lines=10)
                msg = f"Replica {self.name} did not become healthy within spec max_startup_sec; tearing down:\n{log}"
                logger.error(msg)
                self.state_message = msg
                self.state = ReplicaState.start_timeout
                self._shutdown()

        elif self.state == ReplicaState.ready:
            if not healthy and self.consecutive_health_fail >= self._health_debounce:
                logger.warning(msg := f"replica {self.name} became unhealthy")
                self.state_message = msg
                self.state = ReplicaState.unhealthy
                self._unhealthy_since = time.monotonic()

        elif self.state == ReplicaState.unhealthy:
            if healthy and self.consecutive_health_ok >= self._health_debounce:
                logger.info(msg := f"replica {self.name} recovered")
                self.state_message = msg
                self.state = ReplicaState.ready
                self._unhealthy_since = None
            elif self._unhealthy_for_too_long():
                log = self.get_logs(num_lines=10)
                timeout = self._unhealthy_timeout_sec()
                msg = (
                    f"replica {self.name} unhealthy for over {timeout}s; "
                    f"tearing down:\n{log}"
                )
                logger.error(msg)
                self.state_message = msg
                self.state = ReplicaState.error
                self._shutdown()

    def stop(self, timeout: float = 10.0) -> None:
        """
        Cooperatively quiesce (when configured), terminate the process group,
        wait for the monitor to exit, then record the terminal state.

        Calls are serialized and idempotent after authoritative completion. A
        failed attempt leaves the Replica addressable and retryable; success is
        reported only after both the hook and model process groups are absent.
        """
        with self._stop_lock:
            if self._is_teardown_complete():
                return

            logger.info("stopping replica %s", self.name)
            self.state = ReplicaState.terminating
            failure: ReplicaTeardownError | None = None
            try:
                self._shutdown(cooperative=True)
            except ReplicaTeardownError as exc:
                failure = exc

            # Join the monitor so nothing writes self.state after this point.
            if (
                self._monitor.is_alive()
                and threading.current_thread() is not self._monitor
            ):
                self._monitor.join(timeout=timeout + 5)

            # A monitor that was already inside a check may have completed a
            # second serialized teardown attempt while we joined it.
            if not self._is_teardown_complete():
                if failure is None:
                    failure = ReplicaTeardownError(self.state_message)
                raise failure

            self.state = ReplicaState.terminated
            if self._pre_stop_succeeded is True and self._post_stop_succeeded is True:
                self.state_message = (
                    "Model replica terminated after cooperative pre-stop and "
                    "verified post-stop hooks."
                )
            elif self._post_stop_succeeded is True:
                self.state_message = (
                    "Model replica terminated after verified post-stop hook."
                )
            elif self._pre_stop_succeeded is True:
                self.state_message = (
                    "Model replica terminated after cooperative pre-stop hook."
                )
            elif self._pre_stop_succeeded is False:
                self.state_message = (
                    "Model replica terminated with process-group fallback after "
                    "pre-stop hook failure."
                )
            else:
                self.state_message = "Model replica has terminated."

    def _is_teardown_complete(self) -> bool:
        """Read mutable completion state without exposing a stale snapshot."""
        return self._teardown_complete

    def _shutdown(self, *, cooperative: bool = False) -> None:
        """
        Run one bounded teardown attempt.

        Completion is latched only after both private process groups are absent;
        once latched, their numeric PGIDs are never probed or signalled again.
        Failed attempts retain the original Popen handles for a safe retry and
        cannot be confused with a subsequently started Replica.
        """
        self._monitor_exit.set()
        with self._teardown_lock:
            if self._teardown_complete:
                return

            first_attempt = not self._teardown_started
            self._teardown_started = True

            try:
                if cooperative and first_attempt and not self._pre_stop_attempted:
                    (
                        self._pre_stop_succeeded,
                        hook_absent,
                    ) = self._run_pre_stop_hook()
                else:
                    hook_absent = self._ensure_hook_group_absent()
            except Exception:
                logger.exception("pre-stop cleanup failed for replica %s", self.name)
                self._pre_stop_succeeded = False
                hook_absent = False

            # The model fallback is independent of hook outcome: even a hook
            # cleanup error must not prevent the GPU-bearing group from being
            # driven through its own bounded TERM/KILL sequence.
            try:
                model_absent = self._terminate_process_group()
            except Exception:
                logger.exception("model cleanup failed for replica %s", self.name)
                model_absent = False
            post_stop_ready = True
            if model_absent:
                try:
                    (
                        self._post_stop_succeeded,
                        post_stop_group_absent,
                    ) = self._run_post_stop_hook()
                    post_stop_ready = (
                        self._post_stop_succeeded is not False
                        and post_stop_group_absent
                    )
                except Exception:
                    logger.exception(
                        "post-stop verification failed for replica %s", self.name
                    )
                    self._post_stop_succeeded = False
                    post_stop_ready = False

            if hook_absent and model_absent and post_stop_ready:
                self._teardown_complete = True
                self._close_log_handles()
                return

            survivors = []
            if not hook_absent:
                survivors.append("pre-stop hook process group")
            if not model_absent:
                survivors.append("model process group")
            if not post_stop_ready:
                survivors.append("post-stop hook or verification")
            message = (
                f"Replica {self.name} teardown incomplete; still present: "
                + ", ".join(survivors)
            )
            logger.error(message)
            self.state_message = message
            self._write_lifecycle_log(message)
            raise ReplicaTeardownError(message)

    def _run_pre_stop_hook(self) -> tuple[bool | None, bool]:
        self._pre_stop_attempted = True
        script_path = self._pre_stop_script_path
        if script_path is None:
            return None, True

        timeout = self.launch_spec.pre_stop_timeout_sec
        logger.info(
            "running pre-stop hook for replica %s (timeout=%.1fs)",
            self.name,
            timeout,
        )
        self._write_lifecycle_log(f"pre-stop hook started (timeout={timeout:.1f}s)")
        try:
            hook = subprocess.Popen(
                ["/bin/bash", str(script_path)],
                cwd=str(self.workdir),
                stdout=self._log_fh,
                stderr=subprocess.STDOUT,
                env=self._env,
                start_new_session=True,
            )
        except Exception:
            logger.exception("failed to start pre-stop hook for replica %s", self.name)
            self._write_lifecycle_log("pre-stop hook failed to start; using fallback")
            return False, True

        self._pre_stop_proc = hook

        try:
            rc = hook.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            logger.error(
                "pre-stop hook for replica %s exceeded %.1fs; using fallback",
                self.name,
                timeout,
            )
            self._write_lifecycle_log("pre-stop hook timed out; using fallback")
            return False, self._terminate_hook_group(hook)

        if rc != 0:
            logger.error(
                "pre-stop hook for replica %s exited %d; using fallback",
                self.name,
                rc,
            )
            self._write_lifecycle_log(f"pre-stop hook exited {rc}; using fallback")
            # The leader is reaped, but it may have orphaned descendants in its
            # private session. Absence of the whole hook process group is the
            # cleanup condition for every hook outcome.
            return False, self._terminate_hook_group(hook)

        if self._process_group_alive(hook.pid):
            logger.error(
                "pre-stop hook for replica %s left descendant processes; "
                "using fallback",
                self.name,
            )
            self._write_lifecycle_log(
                "pre-stop hook left descendant processes; using fallback"
            )
            return False, self._terminate_hook_group(hook)

        logger.info("pre-stop hook completed for replica %s", self.name)
        self._write_lifecycle_log("pre-stop hook completed")
        return True, True

    def _run_post_stop_hook(self) -> tuple[bool | None, bool]:
        """Run cleanup verification after authoritative model-group absence.

        The hook runs in its own private session.  Every non-success outcome is
        retained as a stop failure even when fallback cleanup removes the hook
        group, so a later controller attempt must rerun and obtain an explicit
        successful verification result.
        """
        script_path = self._post_stop_script_path
        if script_path is None:
            return None, True
        if self._post_stop_succeeded is True:
            return True, self._ensure_post_stop_hook_group_absent()

        # A prior failed attempt may still own descendants.  Never start a new
        # verifier until that exact private process group is absent.
        if not self._ensure_post_stop_hook_group_absent():
            return False, False

        self._post_stop_attempts += 1
        timeout = self.launch_spec.post_stop_timeout_sec
        logger.info(
            "running post-stop hook for replica %s (attempt=%d, timeout=%.1fs)",
            self.name,
            self._post_stop_attempts,
            timeout,
        )
        self._write_lifecycle_log(
            "post-stop hook started "
            f"(attempt={self._post_stop_attempts}, timeout={timeout:.1f}s)"
        )
        try:
            hook = subprocess.Popen(
                ["/bin/bash", str(script_path)],
                cwd=str(self.workdir),
                stdout=self._log_fh,
                stderr=subprocess.STDOUT,
                env=self._env,
                start_new_session=True,
            )
        except Exception:
            logger.exception("failed to start post-stop hook for replica %s", self.name)
            self._write_lifecycle_log("post-stop hook failed to start")
            return False, True

        self._post_stop_proc = hook
        try:
            rc = hook.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            logger.error(
                "post-stop hook for replica %s exceeded %.1fs",
                self.name,
                timeout,
            )
            self._write_lifecycle_log("post-stop hook timed out")
            return False, self._terminate_post_stop_hook_group(hook)

        if rc != 0:
            logger.error(
                "post-stop hook for replica %s exited %d",
                self.name,
                rc,
            )
            self._write_lifecycle_log(f"post-stop hook exited {rc}")
            return False, self._terminate_post_stop_hook_group(hook)

        if self._process_group_alive(hook.pid):
            logger.error(
                "post-stop hook for replica %s left descendant processes",
                self.name,
            )
            self._write_lifecycle_log("post-stop hook left descendant processes")
            return False, self._terminate_post_stop_hook_group(hook)

        logger.info("post-stop hook completed for replica %s", self.name)
        self._write_lifecycle_log("post-stop hook completed")
        return True, True

    def _ensure_hook_group_absent(self) -> bool:
        hook = self._pre_stop_proc
        if hook is None or not self._process_group_alive(hook.pid):
            return True
        return self._terminate_hook_group(hook)

    def _terminate_hook_group(self, hook: subprocess.Popen[bytes]) -> bool:
        """Bound cleanup of a pre-stop hook group; return authoritative absence."""
        retry = self._hook_cleanup_attempts > 0
        self._hook_cleanup_attempts += 1
        hook.poll()
        if not self._process_group_alive(hook.pid):
            return True
        if retry and not self._retry_owns_process_group(
            hook.pid,
            hook,
            self._hook_survivors,
            label="pre-stop hook",
        ):
            return False
        try:
            os.killpg(hook.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        except PermissionError:
            logger.error("permission denied signalling pre-stop hook group SIGTERM")
        if self._wait_for_process_group_exit(hook.pid, hook, self._HOOK_TERM_GRACE):
            return True
        try:
            os.killpg(hook.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except PermissionError:
            logger.error("permission denied signalling pre-stop hook group SIGKILL")
        absent = self._wait_for_process_group_exit(
            hook.pid, hook, self._HOOK_KILL_GRACE
        )
        if not absent:
            logger.error("pre-stop hook process group survived SIGKILL")
            self._hook_survivors = self._snapshot_process_group(hook.pid)
        return absent

    def _ensure_post_stop_hook_group_absent(self) -> bool:
        hook = self._post_stop_proc
        if hook is None or not self._process_group_alive(hook.pid):
            return True
        return self._terminate_post_stop_hook_group(hook)

    def _terminate_post_stop_hook_group(self, hook: subprocess.Popen[bytes]) -> bool:
        """Bound cleanup of a post-stop hook group; prove exact absence."""
        retry = self._post_hook_cleanup_attempts > 0
        self._post_hook_cleanup_attempts += 1
        hook.poll()
        if not self._process_group_alive(hook.pid):
            return True
        if retry and not self._retry_owns_process_group(
            hook.pid,
            hook,
            self._post_hook_survivors,
            label="post-stop hook",
        ):
            return False
        try:
            os.killpg(hook.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        except PermissionError:
            logger.error("permission denied signalling post-stop hook group SIGTERM")
        if self._wait_for_process_group_exit(hook.pid, hook, self._HOOK_TERM_GRACE):
            return True
        try:
            os.killpg(hook.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except PermissionError:
            logger.error("permission denied signalling post-stop hook group SIGKILL")
        absent = self._wait_for_process_group_exit(
            hook.pid, hook, self._HOOK_KILL_GRACE
        )
        if not absent:
            logger.error("post-stop hook process group survived SIGKILL")
            self._post_hook_survivors = self._snapshot_process_group(hook.pid)
        return absent

    def _write_lifecycle_log(self, message: str) -> None:
        try:
            self._log_fh.write(f"[FIRST lifecycle] {message}\n".encode())
            self._log_fh.flush()
        except (OSError, ValueError):
            logger.exception(
                "could not write lifecycle marker for replica %s", self.name
            )

    def _terminate_process_group(self) -> bool:
        """Bound TERM/KILL of the model group; return authoritative absence."""
        retry = self._model_teardown_attempts > 0
        self._model_teardown_attempts += 1
        if not self._group_alive():
            return True
        if retry and not self._retry_owns_process_group(
            self._pgid,
            self.proc,
            self._model_survivors,
            label="model",
        ):
            return False

        try:
            os.killpg(self._pgid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        except PermissionError:
            logger.error("permission denied signalling model group SIGTERM")

        if self._wait_for_group_exit(self._TERM_GRACE):
            return True

        logger.warning(
            "replica %s still alive %.0fs after SIGTERM; escalating to SIGKILL",
            self.name,
            self._TERM_GRACE,
        )
        try:
            os.killpg(self._pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except PermissionError:
            logger.error("permission denied signalling model group SIGKILL")
        absent = self._wait_for_group_exit(self._KILL_GRACE)
        if not absent:
            logger.error("replica %s process group survived SIGKILL", self.name)
            self._model_survivors = self._snapshot_process_group(self._pgid)
        return absent

    def _group_alive(self) -> bool:
        """True if the process group still has at least one member."""
        return self._process_group_alive(self._pgid)

    @staticmethod
    def _process_group_alive(pgid: int) -> bool:
        """True if process group ``pgid`` still has at least one member."""
        try:
            os.killpg(pgid, 0)  # signal 0 == existence probe
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            # EPERM means the kernel found the group but denied the probe.  It
            # is not evidence of absence; cleanup must remain fail-closed.
            return True
        except OSError:
            logger.exception("process-group existence probe failed for pgid=%d", pgid)
            return True

    @staticmethod
    def _snapshot_process_group(pgid: int) -> dict[int, int] | None:
        """Return Linux ``pid -> starttime`` identities for one private session.

        ``None`` means the procfs ownership check is unavailable.  An empty
        mapping means no readable member currently has both pgrp and session
        equal to the launch-time PGID.
        """
        try:
            entries = list(Path("/proc").iterdir())
        except OSError:
            return None

        members: dict[int, int] = {}
        for entry in entries:
            if not entry.name.isdecimal():
                continue
            try:
                raw = (entry / "stat").read_text()
                # comm (field 2) is parenthesized and may contain spaces or ')'.
                fields = raw[raw.rfind(")") + 2 :].split()
                pgrp = int(fields[2])  # field 5
                session = int(fields[3])  # field 6
                starttime = int(fields[19])  # field 22
            except (OSError, ValueError, IndexError):
                continue
            if pgrp == pgid and session == pgid:
                members[int(entry.name)] = starttime
        return members

    def _retry_owns_process_group(
        self,
        pgid: int,
        leader: subprocess.Popen[bytes],
        witnesses: dict[int, int] | None,
        *,
        label: str,
    ) -> bool:
        """Refuse a retry signal unless the numeric PGID is still ours.

        PGIDs can be reused after a group disappears. A live original leader is
        an ownership witness. Once that leader is gone, at least one surviving
        ``(pid, /proc starttime)`` captured by the prior failed attempt must
        still match. Ambiguity is retained as teardown failure rather than
        risking a signal to an unrelated, reused group.
        """
        if leader.poll() is None:
            try:
                if os.getpgid(leader.pid) == pgid and os.getsid(leader.pid) == pgid:
                    return True
            except (ProcessLookupError, PermissionError):
                pass

        current = self._snapshot_process_group(pgid)
        if (
            current is not None
            and witnesses
            and any(
                current.get(pid) == starttime for pid, starttime in witnesses.items()
            )
        ):
            witnesses.update(current)
            return True

        logger.error(
            "refusing retry signal for %s pgid=%d: original process-group "
            "ownership cannot be proven",
            label,
            pgid,
        )
        return False

    def _wait_for_group_exit(self, timeout: float) -> bool:
        """
        Poll until the group has no members, or `timeout` elapses. Returns True
        if it drained.
        """
        deadline = time.monotonic() + timeout
        while True:
            self.proc.poll()  # reap the leader so a zombie can't keep the group "alive"
            if not self._group_alive():
                return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(self._GROUP_POLL_INTERVAL)

    def _wait_for_process_group_exit(
        self, pgid: int, leader: subprocess.Popen[bytes], timeout: float
    ) -> bool:
        """Bound wait for an auxiliary process group, reaping its leader."""
        deadline = time.monotonic() + timeout
        while True:
            leader.poll()
            if not self._process_group_alive(pgid):
                return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(self._GROUP_POLL_INTERVAL)

    def _close_log_handles(self) -> None:
        try:
            self._log_fh.close()
        except OSError:
            pass

    def get_logs(self, num_lines: int = 200) -> str:
        return tail_file(self.log_path, num_lines=num_lines)
