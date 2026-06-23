"""
A SchedulerAdapter that runs the first-pilot process locally as a child
subprocess, plus the shell-script shims (`nvidia-smi`, `ssh`) it needs to
believe it is on a GPU node.
"""

import asyncio
import logging
import os
import signal
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Any, Self

import yaml

from first_common.schema.base_scheduler import (
    JobPhase,
    JobStatusInfo,
    JobSubmitPayload,
    JobSubmitResult,
    SchedulerAdapter,
)

logger = logging.getLogger(__name__)

# We avoid the bash script PilotSubmitter renders (`uvx first-pilot@...`)
# because it would hit PyPI. Instead we exec the entrypoint from the same
# interpreter that's running the tests.
_PILOT_BOOTSTRAP = "from first_pilot.control_api import entrypoint; entrypoint()"


@dataclass
class _TrackedJob:
    payload: JobSubmitPayload
    proc: subprocess.Popen[bytes]
    log_fh: IO[bytes]
    readyfile: Path
    submitted_at: datetime

    def phase(self) -> JobPhase:
        if self.proc.poll() is not None:
            return JobPhase.gone
        if self.readyfile.exists():
            return JobPhase.running
        return JobPhase.queued

    def terminate(self) -> None:
        if self.proc.poll() is None:
            try:
                self.proc.terminate()  # SIGTERM → uvicorn → lifespan teardown → nginx
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
                except (OSError, ProcessLookupError):
                    pass
                self.proc.wait()
        try:
            self.log_fh.close()
        except OSError:
            pass


class LocalSchedulerAdapter(SchedulerAdapter):
    """Subprocess + filesystem SchedulerAdapter for integration tests."""

    def __init__(self, extra_env: dict[str, str] | None = None) -> None:
        self.extra_env = extra_env or {}
        self._jobs: dict[str, _TrackedJob] = {}

    @classmethod
    async def build(cls, _client_state: Any, _config: dict[str, Any]) -> Self:
        return cls()

    async def submit_job(self, job: JobSubmitPayload) -> JobSubmitResult:
        # PilotSubmitter writes `<name>.config.yaml` next to `<name>.sh`.
        config_path = job.script_path.with_name(job.script_path.stem + ".config.yaml")
        cfg = yaml.safe_load(config_path.read_text())
        readyfile = (
            Path(cfg["workdir"]) / "readyfiles" / f"{cfg['job_name']}.ready.json"
        )

        job.log_path.parent.mkdir(parents=True, exist_ok=True)
        log_fh = open(job.log_path, "ab")

        env = os.environ.copy()
        env.update(self.extra_env)
        env["PILOT_CONFIG_FILE"] = str(config_path)

        proc = subprocess.Popen(
            [sys.executable, "-c", _PILOT_BOOTSTRAP],
            env=env,
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

        self._jobs[job.name] = _TrackedJob(
            payload=job,
            proc=proc,
            log_fh=log_fh,
            readyfile=readyfile,
            submitted_at=datetime.now(timezone.utc),
        )
        return JobSubmitResult(job_name=job.name, scheduler_id=str(proc.pid))

    async def get_job_statuses(self) -> list[JobStatusInfo]:
        return [
            JobStatusInfo(
                id=str(t.proc.pid),
                name=name,
                state=t.phase(),
                created_at=t.submitted_at,
                started_at=t.submitted_at,
                walltime_minutes=t.payload.walltime_min,
            )
            for name, t in self._jobs.items()
        ]

    async def terminate_job(self, job_id: str) -> None:
        for tracked in self._jobs.values():
            if str(tracked.proc.pid) == job_id:
                await asyncio.to_thread(tracked.terminate)
                return

    async def put_file(self, content: str, path: Path, mode: int) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        path.chmod(mode)

    async def list_files(self, directory: Path) -> list[str]:
        if not directory.exists():
            return []
        return [p.name for p in directory.iterdir() if p.is_file()]

    async def read_file(self, path: Path) -> str:
        return path.read_text()

    def close(self) -> None:
        for tracked in self._jobs.values():
            tracked.terminate()


_FAKE_NVIDIA_SMI = """\
#!/bin/sh
# Test fixture: emits a fixed 2-GPU CSV, ignoring all flags.
cat <<EOF
0, MockGPU, 81920, 0
1, MockGPU, 81920, 0
EOF
"""

_FAKE_SSH = """\
#!/bin/sh
# Test fixture: strips the destination hostname and execs the remainder
# locally, so `ssh host nvidia-smi ...` runs our fake nvidia-smi.
shift
exec "$@"
"""


def make_mock_pilot_env(bin_dir: Path) -> dict[str, str]:
    """Write fake nvidia-smi + ssh into bin_dir; return env vars to inject."""
    bin_dir.mkdir(parents=True, exist_ok=True)
    for name, body in (("nvidia-smi", _FAKE_NVIDIA_SMI), ("ssh", _FAKE_SSH)):
        script = bin_dir / name
        script.write_text(body)
        script.chmod(0o755)
    return {"PATH": f"{bin_dir}:{os.environ['PATH']}"}
