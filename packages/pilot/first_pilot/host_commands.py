"""Run one command on every node of the pilot's allocation."""

import shlex
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from first_common.schema.pilot import PilotRuntimeConfig
from first_common.schema.types import SSHDiscovery

from .replica_manager import _PALS_LABEL, _host_matches, _require_executable


def run_on_hosts(
    command: list[str], hostnames: list[str], config: PilotRuntimeConfig
) -> dict[str, str]:
    """
    Return each host's stdout, reaching the hosts the same way GPU discovery
    does. Raises RuntimeError if the command cannot be run on every host.
    """
    discovery = config.gpu_discovery
    timeout_sec = discovery.timeout_sec

    if len(hostnames) == 1:
        return {hostnames[0]: _run(command, timeout_sec)}

    if isinstance(discovery, SSHDiscovery):
        with ThreadPoolExecutor(max_workers=len(hostnames)) as pool:
            outputs = pool.map(
                lambda host: _run(["ssh", host, shlex.join(command)], timeout_sec),
                hostnames,
            )
            return dict(zip(hostnames, outputs))

    return _run_pals(command, hostnames, discovery.launcher_path, timeout_sec)


def _run(command: list[str], timeout_sec: float) -> str:
    try:
        result = subprocess.run(
            command, capture_output=True, text=True, timeout=timeout_sec
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"{command[0]} timed out after {timeout_sec:g}s") from exc
    if result.returncode != 0:
        diagnostics = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(
            f"{command[0]} exited {result.returncode}: {diagnostics[-2000:]}"
        )
    return result.stdout


def _run_pals(
    command: list[str], hostnames: list[str], launcher_path: Path, timeout_sec: float
) -> dict[str, str]:
    """Launch one labeled rank per host and split the output by host label."""
    pals = _require_executable(launcher_path, label="PALS launcher")
    # PALS does not reliably search PATH for the launched application.
    executable = shutil.which(command[0])
    if executable is None:
        raise RuntimeError(f"required {command[0]} is unavailable on the pilot PATH")

    stdout = _run(
        [
            pals,
            "--pmi=pmix",
            "--genvnone",
            "--no-transfer",
            "--line-buffer",
            "--label",
            "--abort-on-failure",
            "--timeout",
            # Let the launcher report and exit before our own deadline.
            str(max(1, int(timeout_sec) - 5)),
            "-n",
            str(len(hostnames)),
            "--ppn",
            "1",
            "--cpu-bind=none",
            str(Path(executable).resolve()),
            *command[1:],
        ],
        timeout_sec,
    )

    lines: dict[str, list[str]] = {host: [] for host in hostnames}
    for line in stdout.splitlines():
        if not line.strip():
            continue
        match = _PALS_LABEL.fullmatch(line)
        host = match and next(
            (h for h in hostnames if _host_matches(match.group("host"), h)), None
        )
        if not match or not host:
            raise RuntimeError(f"PALS returned a row from an unknown host: {line!r}")
        lines[host].append(match.group("row"))
    return {host: "\n".join(rows) for host, rows in lines.items()}
