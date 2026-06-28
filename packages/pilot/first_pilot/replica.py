import logging
import os
import shlex
import signal
import subprocess
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from jinja2 import Environment, StrictUndefined

from first_common.schema.types import (
    GpuClaim,
    HealthEndpointStatus,
    PilotLaunchSpec,
    ReplicaPhase,
    ScriptTemplateContext,
)

logger = logging.getLogger(__name__)


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

    _HEALTH_INTERVAL = 0.4
    _HEALTH_DEBOUNCE = 10
    _TERM_GRACE = 8.0
    _KILL_GRACE = 5.0
    _GROUP_POLL_INTERVAL = 0.2

    def __init__(
        self,
        name: str,
        port: int,
        resources: list[GpuClaim],
        launch_spec: PilotLaunchSpec,
        workdir: Path,
    ) -> None:
        self.name = name
        self.port = port
        self.resources = resources
        self.launch_spec = launch_spec
        self.workdir = workdir

        self.log_path = workdir / f"{self.name}.log"

        script_path = self.workdir / "serve.sh"
        script_path.write_text(self._render_script())
        script_path.chmod(0o755)

        self._log_fh = open(self.log_path, "ab")

        env = os.environ.copy()
        env.update(self.launch_spec.env)

        logger.info(
            "starting replica %s on port %d (workdir=%s)",
            self.name,
            self.port,
            workdir,
        )
        try:
            self.proc = subprocess.Popen(
                ["/bin/bash", str(script_path)],
                cwd=str(self.workdir),
                stdout=self._log_fh,
                stderr=subprocess.STDOUT,
                env=env,
                start_new_session=True,
            )
        except Exception:
            logger.exception("failed to Popen model replica %s", self.name)
            self._close_log_handles()
            raise

        # start_new_session=True makes the child its own session/group leader
        self._pgid = self.proc.pid

        self.phase = ReplicaPhase.launching
        self.status_info = "Model startup script has begun."
        self.started_at = datetime.now(timezone.utc)
        self._startup_deadline = time.monotonic() + self.launch_spec.max_startup_sec

        self.consecutive_health_ok = 0
        self.consecutive_health_fail = 0
        self._unhealthy_since: float | None = None

        self._teardown_lock = threading.Lock()
        self._torn_down = False
        self._monitor_exit = threading.Event()
        self._monitor = threading.Thread(
            target=self._monitor_loop,
            name=f"replica-monitor-{self.name}",
            daemon=True,
        )
        self._monitor.start()

    def _render_script(self) -> str:
        spec = self.launch_spec

        gpus_by_host: dict[str, list[str]] = {}
        for claim in self.resources:
            gpus_by_host.setdefault(claim.hostname, []).extend(claim.gpu_ids)

        context: ScriptTemplateContext = {
            "replica_name": self.name,
            "served_model_name": spec.served_model_name,
            "port": self.port,
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
        return env.from_string(spec.serve_script_template).render(**context)

    def _check_health(self) -> HealthEndpointStatus:
        health_path = self.launch_spec.health_path
        if not health_path:
            # No health endpoint -> trust the process: alive == healthy.
            return HealthEndpointStatus.healthy

        url = f"http://127.0.0.1:{self.port}{health_path}"
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if 200 <= resp.status < 300:
                    return HealthEndpointStatus.healthy
                return HealthEndpointStatus.unhealthy
        except (urllib.error.URLError, OSError, TimeoutError):
            return HealthEndpointStatus.unknown

    def _record_health(self, health: HealthEndpointStatus) -> None:
        if health == HealthEndpointStatus.healthy:
            self.consecutive_health_ok += 1
            self.consecutive_health_fail = 0
        else:
            self.consecutive_health_ok = 0
            self.consecutive_health_fail += 1

    def _unhealthy_for_too_long(self) -> bool:
        if self._unhealthy_since is None:
            return False
        elapsed = time.monotonic() - self._unhealthy_since
        return elapsed > self.launch_spec.max_startup_sec

    def _monitor_loop(self) -> None:
        while not self._monitor_exit.wait(timeout=self._HEALTH_INTERVAL):
            try:
                self._run_monitor_check()
            except Exception:
                logger.exception(
                    "uncaught exception in monitor thread for %s", self.name
                )
                self.phase = ReplicaPhase.error
                self._shutdown()
                return

    def _run_monitor_check(self) -> None:
        rc = self.proc.poll()
        if rc is not None:
            self._handle_process_exit(rc)
            return

        health = self._check_health()
        self._record_health(health)
        self._advance_phase(health)

    def _handle_process_exit(self, rc: int) -> None:
        if self.phase in (ReplicaPhase.terminating, ReplicaPhase.terminated):
            self.phase = ReplicaPhase.terminated
            self.status_info = "Model replica has terminated."
        else:
            log = self.get_logs(num_lines=10)
            msg = (
                f"Model replica {self.name} exited unexpectedly with code {rc}:\n{log}"
            )
            logger.error(msg)
            self.phase = ReplicaPhase.error
            self.status_info = msg

        # The leader is gone but may have left GPU-pinned children behind:
        self._shutdown()

    def _advance_phase(self, health: HealthEndpointStatus) -> None:
        healthy = health == HealthEndpointStatus.healthy

        if self.phase == ReplicaPhase.launching:
            if healthy:
                elapsed = (datetime.now(timezone.utc) - self.started_at).total_seconds()
                logger.info(
                    msg := f"Replica {self.name} ready after {elapsed:.1f} seconds"
                )
                self.status_info = msg
                self.phase = ReplicaPhase.ready
            elif time.monotonic() > self._startup_deadline:
                log = self.get_logs(num_lines=10)
                msg = f"Replica {self.name} did not become healthy within spec max_startup_sec; tearing down:\n{log}"
                logger.error(msg)
                self.status_info = msg
                self.phase = ReplicaPhase.start_timeout
                self._shutdown()

        elif self.phase == ReplicaPhase.ready:
            if not healthy and self.consecutive_health_fail >= self._HEALTH_DEBOUNCE:
                logger.warning(msg := f"replica {self.name} became unhealthy")
                self.status_info = msg
                self.phase = ReplicaPhase.unhealthy
                self._unhealthy_since = time.monotonic()

        elif self.phase == ReplicaPhase.unhealthy:
            if healthy and self.consecutive_health_ok >= self._HEALTH_DEBOUNCE:
                logger.info(msg := f"replica {self.name} recovered")
                self.status_info = msg
                self.phase = ReplicaPhase.ready
                self._unhealthy_since = None
            elif self._unhealthy_for_too_long():
                log = self.get_logs(num_lines=10)
                msg = f"replica {self.name} unhealthy for over max_startup_sec; tearing down:\n{log}"
                logger.error(msg)
                self.status_info = msg
                self.phase = ReplicaPhase.error
                self._shutdown()

    def stop(self, timeout: float = 10.0) -> None:
        """
        Terminate the process group, wait for the monitor to exit, then record
        the terminal phase.
        """
        logger.info("stopping replica %s", self.name)
        self.phase = ReplicaPhase.terminating
        self._shutdown()

        # Join the monitor so nothing writes self.phase after this point
        if self._monitor.is_alive() and threading.current_thread() is not self._monitor:
            self._monitor.join(timeout=timeout + 5)

        self.phase = ReplicaPhase.terminated

    def _shutdown(self) -> None:
        """
        Idempotent teardown: kill the group, close logs, stop the monitor.  Safe
        to call concurrently from the monitor thread and stop().
        """
        with self._teardown_lock:
            if not self._torn_down:
                self._torn_down = True
                self._terminate_process_group()
                self._close_log_handles()
        self._monitor_exit.set()

    def _terminate_process_group(self) -> None:
        if not self._signal_group(signal.SIGTERM):
            return  # group already empty

        if self._wait_for_group_exit(self._TERM_GRACE):
            return

        logger.warning(
            "replica %s still alive %.0fs after SIGTERM; escalating to SIGKILL",
            self.name,
            self._TERM_GRACE,
        )
        if not self._signal_group(signal.SIGKILL):
            return
        if not self._wait_for_group_exit(self._KILL_GRACE):
            logger.error("replica %s process group survived SIGKILL", self.name)

    def _signal_group(self, sig: int) -> bool:
        """Send `sig` to the whole process group. Returns False if empty."""
        try:
            os.killpg(self._pgid, sig)
            return True
        except ProcessLookupError:
            return False

    def _group_alive(self) -> bool:
        """True if the process group still has at least one member."""
        try:
            os.killpg(self._pgid, 0)  # signal 0 == existence probe
            return True
        except ProcessLookupError:
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

    def _close_log_handles(self) -> None:
        try:
            self._log_fh.close()
        except OSError:
            pass

    def get_logs(self, num_lines: int = 200) -> str:
        return tail_file(self.log_path, num_lines=num_lines)
