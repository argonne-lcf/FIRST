"""
disk_observations: how full is each block device this container can see?

The container sees a host filesystem only through something mounted from it.
deploy/compose.yaml mounts an empty, read-only directory from each host
filesystem worth watching under /run/disk-watch/<label>; the other mounts
(secret files, /etc/hosts, volumes) show the same devices again.

Mounts are read from /proc/self/mountinfo and grouped by device, so each
device is reported once, by name (``/dev/sda2``), not by whichever file
happens to be mounted from it. Only sources under /dev/ count: this skips the
container's own overlay root (it lives on the Docker data root, which a watch
mount covers) and pseudo filesystems (tmpfs, shm, proc, sysfs, ...).
"""

import logging
import re
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import NamedTuple, Protocol

from first_common.schema.resources.runtime import Severity
from first_gateway.controllers.workers.health_alerter.types import Observation

logger = logging.getLogger(__name__)

MOUNTINFO = Path("/proc/self/mountinfo")
DISK_WATCH_DIR = "/run/disk-watch"


class _Usage(Protocol):
    @property
    def used(self) -> int: ...
    @property
    def free(self) -> int: ...


class DeviceMount(NamedTuple):
    device: str
    mountpoint: str


def _unescape(field: str) -> str:
    """mountinfo octal-escapes space, tab, newline and backslash."""
    return re.sub(r"\\([0-7]{3})", lambda m: chr(int(m.group(1), 8)), field)


def parse_mountinfo(text: str) -> list[DeviceMount]:
    """Mounts whose source is a block device, in mountinfo order.

    Line format (proc(5)): ``id parent maj:min root mountpoint opts
    [optional...] - fstype source superopts``.
    """
    mounts: list[DeviceMount] = []
    for line in text.splitlines():
        left, sep, right = line.partition(" - ")
        lf, rf = left.split(), right.split()
        if not sep or len(lf) < 5 or len(rf) < 2:
            continue
        source = _unescape(rf[1])
        if source.startswith("/dev/"):
            mounts.append(DeviceMount(source, _unescape(lf[4])))
    return mounts


def group_by_device(mounts: list[DeviceMount]) -> dict[str, list[str]]:
    """Device -> its mountpoints, watch mounts first."""
    by_device: dict[str, list[str]] = {}
    for m in mounts:
        by_device.setdefault(m.device, []).append(m.mountpoint)
    for paths in by_device.values():
        paths.sort(key=lambda p: not _is_watch(p))
    return by_device


def _is_watch(path: str) -> bool:
    return path.startswith(DISK_WATCH_DIR + "/")


def df_use_percent(used: int, avail: int) -> int | None:
    """``df``'s Use%: used / (used + available to non-root), rounded up.

    Blocks reserved for root count as neither, so a disk reads 100% when
    ordinary users can no longer write, not when it is completely full.
    """
    if used + avail <= 0:
        return None
    return -(-used * 100 // (used + avail))


def disk_severity(use: int) -> Severity | None:
    if use > 90:
        return "crit"
    if use > 80:
        return "warn"
    if use > 70:
        return "info"
    return None


def disk_observations(
    mountinfo: Path = MOUNTINFO,
    disk_usage: Callable[[str], _Usage] = shutil.disk_usage,
) -> list[Observation]:
    """One observation per block device over 70% full.

    Returns nothing where there is no mount table (not Linux).
    """
    try:
        text = mountinfo.read_text()
    except OSError:
        logger.debug("No mount table at %s; skipping disk check", mountinfo)
        return []

    obs: list[Observation] = []
    for device, paths in group_by_device(parse_mountinfo(text)).items():
        try:
            usage = disk_usage(paths[0])
        except OSError:
            logger.warning("Could not stat %s (%s)", paths[0], device, exc_info=True)
            continue
        use = df_use_percent(usage.used, usage.free)
        sev = disk_severity(use) if use is not None else None
        if sev is None:
            continue
        labels = [p.removeprefix(DISK_WATCH_DIR + "/") for p in paths if _is_watch(p)]
        hint = f" ({', '.join(labels)})" if labels else ""
        obs.append(
            Observation(
                key=f"disk:{device}",
                status=sev,
                summary=f"{device} {use}% full{hint}",
                display_name=f"Disk {device}",
                severity=sev,
            )
        )
    return obs
