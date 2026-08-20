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

from first_common.errors import ReplicaTeardownError
from first_common.health import perform_health_check_sync
from first_common.schema.types import (
    GpuClaim,
    HealthCheckResult,
    PilotLaunchSpec,
    ReplicaState,
    ScriptTemplateContext,
)

logger = logging.getLogger(__name__)


def _pgid_alive(pgid: int) -> bool:
    """True if process group ``pgid`` still has at least one member."""
    try:
        os.killpg(pgid, 0)  # signal 0 == existence probe
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        # EPERM means the kernel found the group but denied the probe.  It is
        # not evidence of absence; cleanup must remain fail-closed.
        return True


class _ManagedGroup:
    """One private session (session == pgid) driven to authoritative absence.

    Wraps a leader ``Popen`` started with ``start_new_session=True``, so its pid
    is the pgid of a session it alone owns. `ensure_absent` runs a bounded
    TERM->KILL sequence and returns whether the whole group is provably gone.
    Every call is identical, so a controller retry just repeats it.
    """

    def __init__(
        self,
        leader: subprocess.Popen[bytes],
        *,
        label: str,
        term_grace: float,
        kill_grace: float,
        poll_interval: float,
    ) -> None:
        self._leader = leader
        self._pgid = leader.pid
        self._label = label
        self._term_grace = term_grace
        self._kill_grace = kill_grace
        self._poll_interval = poll_interval

    @property
    def pgid(self) -> int:
        return self._pgid

    def alive(self) -> bool:
        self._leader.poll()  # reap the leader so a zombie can't look "alive"
        return _pgid_alive(self._pgid)

    def ensure_absent(self) -> bool:
        """Drive the group to proven absence with a bounded TERM->KILL pass."""
        if not self.alive():
            return True

        self._signal(signal.SIGTERM)
        if self._wait_for_exit(self._term_grace):
            return True

        logger.warning(
            "%s group %d still alive %.0fs after SIGTERM; escalating to SIGKILL",
            self._label,
            self._pgid,
            self._term_grace,
        )
        self._signal(signal.SIGKILL)
        absent = self._wait_for_exit(self._kill_grace)
        if not absent:
            # A group can outlast SIGKILL only while a member is wedged in
            # uninterruptible (D-state) sleep; the kernel delivers the pending
            # kill once it unwedges. Re-signalling cannot help, so we only
            # report the survivor and let the controller retry later.
            logger.error("%s process group survived SIGKILL", self._label)
        return absent

    def _signal(self, sig: int) -> None:
        try:
            os.killpg(self._pgid, sig)
        except ProcessLookupError:
            pass
        except PermissionError:
            logger.error(
                "permission denied signalling %s group %s",
                self._label,
                signal.Signals(sig).name,
            )

    def _wait_for_exit(self, timeout: float) -> bool:
        """Poll until the group drains or ``timeout`` elapses."""
        deadline = time.monotonic() + timeout
        while True:
            if not self.alive():
                return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(self._poll_interval)


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

    Teardown model
    --------------
    A replica owns up to three private process groups: an optional pre-stop
    hook, the model itself, and an optional post-stop verifier. Each is wrapped
    in a :class:`_ManagedGroup` that drives it to *proven* absence.

    * ``stop()`` is the *only* path that tears a replica down. The health
      monitor merely observes and records state (``error``, ``start_timeout``);
      the controller always calls ``stop()`` to free resources, even for a
      replica that already parked in a failure state.
    * ``stop()`` joins the monitor first, so teardown runs single-threaded. It
      runs one bounded attempt and either latches success (``_teardown_complete``)
      or raises :class:`ReplicaTeardownError`. Retries are the controller calling
      ``stop()`` again — the object stays addressable in between.
    * Stage order is fixed: quiesce (pre-stop) -> kill the model group *no matter
      what the hook did* -> verify (post-stop) *only once the model is gone*.
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

        self.state = ReplicaState.launching
        self.state_message = "Model startup script has begun."
        self.started_at = datetime.now(timezone.utc)
        self._startup_deadline = time.monotonic() + self.launch_spec.max_startup_sec

        self.consecutive_health_ok = 0
        self.consecutive_health_fail = 0
        self._unhealthy_since: float | None = None

        self._health_params = self.launch_spec.health_check.model_copy()
        # Pilot-side policy floor, not a schema rule: the gateway legitimately
        # uses smaller debounce values, but at our 2s poll interval anything
        # below 5 makes replica ready/unhealthy flapping too twitchy.
        self._health_debounce = max(5, self._health_params.debounce)
        self._health_client = Client(transport=HTTPTransport(uds=self.uds))

        if self._health_params.url:
            path = urlparse(self._health_params.url).path
            url = f"http://localhost/{path.lstrip('/')}"
            self._health_params.url = url
            logger.info(f"Replica will monitor health at {url}")
        else:
            logger.info("Replica health check disabled")

        self._stop_lock = threading.Lock()
        self._teardown_complete: bool = False
        self._pre_stop_ran: bool = False  # cooperative hook runs at most once, ever
        self._pre_stop_ok: bool = False  # ...and exited 0 with no descendants
        self._post_stop_ok: bool = False  # one post-stop run verified cleanly
        self._post_stop_attempts = 0

        # The three private groups this replica drives to proven absence. The
        # model group exists for the replica's whole life; the hook groups are
        # created lazily when their scripts launch.
        self._model_group = _ManagedGroup(
            self.proc,
            label=f"replica {self.name} model",
            term_grace=self._TERM_GRACE,
            kill_grace=self._KILL_GRACE,
            poll_interval=self._GROUP_POLL_INTERVAL,
        )
        self._pre_stop_group: _ManagedGroup | None = None
        self._post_stop_group: _ManagedGroup | None = None

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
        return elapsed > self._unhealthy_timeout_sec

    @property
    def _unhealthy_timeout_sec(self) -> int:
        return self.launch_spec.max_unhealthy_sec or self.launch_spec.max_startup_sec

    def _monitor_loop(self) -> None:
        """Observe the replica and record its state; never tear it down.

        The monitor only reports what it sees. Teardown -- freeing the GPUs and
        sweeping the process group -- is always driven by the controller calling
        :meth:`stop`, even for a replica that has parked in ``error`` or
        ``start_timeout``. When the monitor decides the replica is doomed (or
        has exited) it records the terminal state and returns; there is nothing
        further to observe.
        """
        while not self._monitor_exit.wait(timeout=self._HEALTH_INTERVAL):
            try:
                if self._run_monitor_check():
                    return
            except Exception:
                logger.exception(
                    "uncaught exception in monitor thread for %s", self.name
                )
                self.state = ReplicaState.error
                return

    def _run_monitor_check(self) -> bool:
        """Run one observation. Return True when the monitor should stop."""
        rc = self.proc.poll()
        if rc is not None:
            self._handle_process_exit(rc)
            return True

        health = self._check_health()
        self._record_health(health)
        return self._advance_state(health)

    def _handle_process_exit(self, rc: int) -> None:
        # A stop() in flight owns the terminal state; don't race it.
        if self.state in (ReplicaState.terminating, ReplicaState.terminated):
            return

        log = self.get_logs(num_lines=10)
        msg = f"Model replica {self.name} exited unexpectedly with code {rc}:\n{log}"
        logger.error(msg)
        self.state = ReplicaState.error
        self.state_message = msg

    def _advance_state(self, health: HealthCheckResult) -> bool:
        """Advance the state machine. Return True when the monitor should stop."""
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
                msg = (
                    f"Replica {self.name} did not become healthy within spec "
                    f"max_startup_sec; awaiting teardown:\n{log}"
                )
                logger.error(msg)
                self.state_message = msg
                self.state = ReplicaState.start_timeout
                return True

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
                timeout = self._unhealthy_timeout_sec
                msg = (
                    f"replica {self.name} unhealthy for over {timeout}s; "
                    f"awaiting teardown:\n{log}"
                )
                logger.error(msg)
                self.state_message = msg
                self.state = ReplicaState.error
                return True

        return False

    def stop(self, timeout: float = 10.0) -> None:
        """
        Cooperatively quiesce (when configured), terminate the process group,
        and record the terminal state. This is the only path that tears a
        replica down; the health monitor never does.

        Calls are serialized by ``_stop_lock`` and idempotent after success. A
        failed attempt raises :class:`ReplicaTeardownError` and leaves the
        Replica addressable so the controller can retry by calling ``stop()``
        again. See the class docstring for the teardown model.
        """
        with self._stop_lock:
            if self._teardown_complete:
                return

            logger.info("stopping replica %s", self.name)
            self.state = ReplicaState.terminating

            # Stop the observer first so nothing writes self.state while we tear
            # down. The monitor never calls stop(), so this cannot self-join.
            self._monitor_exit.set()
            self._monitor.join(timeout=timeout + 5)
            if self._monitor.is_alive():
                logger.warning(
                    "monitor thread for replica %s still alive after %.0fs join; "
                    "proceeding with teardown anyway",
                    self.name,
                    timeout + 5,
                )

            # Re-assert terminating in case monitor changed it before joining
            self.state = ReplicaState.terminating

            survivors = self._teardown()
            if survivors:
                message = (
                    f"Replica {self.name} teardown incomplete; still present: "
                    + ", ".join(survivors)
                )
                logger.error(message)
                self.state_message = message
                self._write_lifecycle_log(message)
                raise ReplicaTeardownError(message)

            self._teardown_complete = True
            self._close_log_handles()
            self._health_client.close()
            self.state = ReplicaState.terminated
            self.state_message = self._terminated_message()

    def _terminated_message(self) -> str:
        """Describe a completed teardown from the recorded hook results."""
        if self._pre_stop_ok and self._post_stop_ok:
            return (
                "Model replica terminated after cooperative pre-stop and "
                "verified post-stop hooks."
            )
        if self._post_stop_ok:
            return "Model replica terminated after verified post-stop hook."
        if self._pre_stop_ok:
            return "Model replica terminated after cooperative pre-stop hook."
        if self._pre_stop_ran:  # ran (implies configured) but did not succeed
            return (
                "Model replica terminated with process-group fallback after "
                "pre-stop hook failure."
            )
        return "Model replica has terminated."

    def _teardown(self) -> list[str]:
        """Run one bounded teardown attempt, top to bottom; return survivors.

        Stage order is fixed: quiesce (pre-stop) -> kill the model group no
        matter what the hook did -> verify (post-stop) only once the model is
        gone. A retry re-runs only what is unsettled: the cooperative pre-stop
        hook runs at most once ever, the model kill is idempotent, and the
        post-stop hook re-runs until one attempt verifies cleanly. Serialized
        by ``stop()``'s ``_stop_lock``, so no internal locking is needed.
        """
        survivors: list[str] = []

        # 1. Pre-stop quiesce. Exception-guarded so a fault in the hook
        #    machinery can never skip the model kill below. A failed hook is
        #    tolerated -- the kill below is the fallback -- but its private
        #    process group must still be proven absent.
        try:
            if self._pre_stop_script_path is not None and not self._pre_stop_ran:
                self._pre_stop_ran = True
                self._pre_stop_ok = self._run_hook(
                    "pre-stop",
                    self._pre_stop_script_path,
                    self.launch_spec.pre_stop_timeout_sec,
                )
                if not self._pre_stop_ok:
                    self._write_lifecycle_log(
                        "pre-stop hook failed; using process-group fallback"
                    )
            pre_stop_gone = (
                self._pre_stop_group is None or self._pre_stop_group.ensure_absent()
            )
        except Exception:
            logger.exception("pre-stop cleanup failed for replica %s", self.name)
            pre_stop_gone = False
        if not pre_stop_gone:
            survivors.append("pre-stop hook process group")

        # 2. The GPU-bearing model group, driven to proven absence.
        if not self._model_group.ensure_absent():
            survivors.append("model process group")
            return survivors  # post-stop is meaningless while the model lives

        # 3. Post-stop verification: one clean hook run (exit 0, no
        #    descendants) after the model is gone. Never start a new run while
        #    a previous run's process group survives.
        if self._post_stop_script_path is not None and not self._post_stop_ok:
            try:
                prior_gone = (
                    self._post_stop_group is None
                    or self._post_stop_group.ensure_absent()
                )
                if prior_gone:
                    self._post_stop_attempts += 1
                    self._post_stop_ok = self._run_hook(
                        "post-stop",
                        self._post_stop_script_path,
                        self.launch_spec.post_stop_timeout_sec,
                    )
                    if not self._post_stop_ok and self._post_stop_group is not None:
                        self._post_stop_group.ensure_absent()  # sweep leftovers now
            except Exception:
                logger.exception(
                    "post-stop verification failed for replica %s", self.name
                )
            if not self._post_stop_ok:
                survivors.append("post-stop hook or verification")

        return survivors

    def _run_hook(self, label: str, script_path: Path, timeout: float) -> bool:
        """Run a hook script in its own session and wait for it to finish.

        The group is stored on the Replica immediately after launch, before any
        wait or validation that can raise. ``True`` means the hook exited 0
        within ``timeout`` and left no descendants in its process group.
        """
        logger.info(
            "running %s hook for replica %s (timeout=%.1fs)", label, self.name, timeout
        )
        self._write_lifecycle_log(f"{label} hook started (timeout={timeout:.1f}s)")
        try:
            proc = subprocess.Popen(
                ["/bin/bash", str(script_path)],
                cwd=str(self.workdir),
                stdout=self._log_fh,
                stderr=subprocess.STDOUT,
                env=self._env,
                start_new_session=True,
            )
        except Exception:
            logger.exception("failed to start %s hook for replica %s", label, self.name)
            self._write_lifecycle_log(f"{label} hook failed to start")
            return False

        group = _ManagedGroup(
            proc,
            label=f"replica {self.name} {label} hook",
            term_grace=self._HOOK_TERM_GRACE,
            kill_grace=self._HOOK_KILL_GRACE,
            poll_interval=self._GROUP_POLL_INTERVAL,
        )
        if label == "pre-stop":
            self._pre_stop_group = group
        else:
            self._post_stop_group = group

        try:
            rc = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            logger.error(
                "%s hook for replica %s exceeded %.1fs", label, self.name, timeout
            )
            self._write_lifecycle_log(f"{label} hook timed out")
            return False

        if rc != 0:
            logger.error("%s hook for replica %s exited %d", label, self.name, rc)
            self._write_lifecycle_log(f"{label} hook exited {rc}")
            return False

        if group.alive():
            # The leader exited 0 but orphaned descendants in its private
            # session; the whole group must be gone for the hook to count.
            logger.error(
                "%s hook for replica %s left descendant processes", label, self.name
            )
            self._write_lifecycle_log(f"{label} hook left descendant processes")
            return False

        logger.info("%s hook completed for replica %s", label, self.name)
        self._write_lifecycle_log(f"{label} hook completed")
        return True

    def _write_lifecycle_log(self, message: str) -> None:
        try:
            self._log_fh.write(f"[FIRST lifecycle] {message}\n".encode())
            self._log_fh.flush()
        except OSError:
            logger.exception(
                "could not write lifecycle marker for replica %s", self.name
            )

    def _close_log_handles(self) -> None:
        try:
            self._log_fh.close()
        except OSError:
            pass

    def get_logs(self, num_lines: int = 200) -> str:
        return tail_file(self.log_path, num_lines=num_lines)
