"""
Run one command on every node of the pilot's allocation.

This is the pilot's only way of reaching its nodes: GPU inventory and startup
node checks both go through `run_on_hosts`.
"""

import os
import re
import shlex
import shutil
import socket
import subprocess
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from first_common.schema.types import GpuDiscovery, SSHDiscovery

_PALS_LABEL = re.compile(r"^(?P<host>\S+)\s+(?P<rank>\d+):\s?(?P<row>.*)$")


def run_on_hosts(
    command: list[str],
    hostnames: list[str],
    discovery: GpuDiscovery,
) -> dict[str, str]:
    """
    Return each host's stdout. A command for the local host alone runs locally;
    otherwise `discovery` selects SSH or the PALS launcher. Raises RuntimeError
    unless the command completed successfully on every host.
    """
    timeout_sec = discovery.timeout_sec

    if not hostnames or len(deduplicate_hosts(hostnames)) != len(hostnames):
        raise RuntimeError(
            "host-command inventory is empty or contains duplicate hosts"
        )

    if not isinstance(discovery, SSHDiscovery):
        executable = shutil.which(command[0])
        if executable is None:
            raise RuntimeError(
                f"required {command[0]} is unavailable on the pilot PATH"
            )
        command = [str(Path(executable).resolve()), *command[1:]]

    marker = f"FIRST_HOST_COMMAND_{uuid.uuid4().hex}"
    native_command = _completed_command(command, marker)

    if len(hostnames) == 1 and host_matches(socket.gethostname(), hostnames[0]):
        outputs = {hostnames[0]: _run(native_command, timeout_sec)}

    elif isinstance(discovery, SSHDiscovery):
        with ThreadPoolExecutor(max_workers=len(hostnames)) as pool:
            results = pool.map(
                lambda host: _run(
                    ["ssh", host, shlex.join(native_command)], timeout_sec
                ),
                hostnames,
            )
            outputs = dict(zip(hostnames, results))
    else:
        outputs = _run_pals(
            native_command, hostnames, discovery.launcher_path, timeout_sec
        )

    return {
        host: _completed_output(output, marker, host)
        for host, output in outputs.items()
    }


def _completed_command(command: list[str], marker: str) -> list[str]:
    # No output is evidence of success only if this per-rank trailer arrives.
    # "$@" preserves arguments without evaluating configuration as shell code.
    script = (
        '"$@"; result=$?; '
        "physical_host=$(/bin/hostname -s) || exit 125; "
        f'printf \'\\n{marker}:%s:%s\\n\' "$physical_host" "$result"; '
        'exit "$result"'
    )
    return ["/bin/sh", "-c", script, "first-host-command", *command]


def _completed_output(output: str, marker: str, host: str) -> str:
    rows = output.splitlines()
    matching = [row for row in rows if row.startswith(marker + ":")]
    if len(matching) != 1 or not rows or rows[-1] != matching[0]:
        raise RuntimeError(f"host {host!r} has no unique final command completion")
    parts = matching[0].split(":")
    if len(parts) != 3 or parts[2] != "0" or not host_matches(parts[1], host):
        raise RuntimeError(f"host {host!r} has an invalid command completion")
    rows.pop()
    # The wrapper adds one separating newline, not a command-output row.
    if rows and not rows[-1]:
        rows.pop()
    return "\n".join(rows)


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
    if len(result.stdout) > 1024 * 1024:
        raise RuntimeError(f"{command[0]} returned excessive host-command output")
    return result.stdout


def _run_pals(
    command: list[str], hostnames: list[str], launcher_path: Path, timeout_sec: float
) -> dict[str, str]:
    """Launch one labeled rank per host and split the output by rank."""
    pals = _require_executable(launcher_path, label="PALS launcher")
    # PALS does not reliably search PATH for the launched application. Resolve
    # it on the allocation head and give every rank a canonical absolute path.
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
            # Leave time for the launcher to report and exit before our outer
            # subprocess deadline expires.
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

    rows: dict[str, list[str]] = {host: [] for host in hostnames}
    for line in stdout.splitlines():
        if not line.strip():
            continue
        match = _PALS_LABEL.fullmatch(line)
        if match is None:
            raise RuntimeError(f"PALS returned an unlabeled row: {line!r}")
        rank = int(match.group("rank"))
        if rank >= len(hostnames):
            raise RuntimeError(f"PALS returned unexpected rank {rank}")
        # Rank order must equal scheduler host order: replica launches rely on it.
        if not host_matches(match.group("host"), hostnames[rank]):
            raise RuntimeError(
                f"PALS rank {rank} reported host {match.group('host')!r}; "
                f"expected {hostnames[rank]!r}"
            )
        rows[hostnames[rank]].append(match.group("row"))
    return {host: "\n".join(lines) for host, lines in rows.items()}


def _require_executable(path: Path, *, label: str) -> str:
    """Return a configured executable path or fail with a useful error."""
    if not path.is_file() or not os.access(path, os.X_OK):
        raise RuntimeError(f"required {label} is unavailable: {path}")
    return str(path)


def host_identity(hostname: str) -> str:
    """The short, lowercase hostname: how two spellings of a host are compared."""
    return hostname.strip().rstrip(".").lower().split(".", maxsplit=1)[0]


def host_matches(label: str, expected: str) -> bool:
    return bool(host_identity(label)) and host_identity(label) == host_identity(
        expected
    )


def deduplicate_hosts(hostnames: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for hostname in hostnames:
        identity = host_identity(hostname)
        if not identity:
            raise RuntimeError("scheduler node inventory contains an empty hostname")
        if identity not in seen:
            seen.add(identity)
            result.append(hostname)
    return result
