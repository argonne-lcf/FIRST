"""Tests for the health alerter's disk check (health_alerter/disk.py)."""

from collections.abc import Callable
from pathlib import Path
from shutil import _ntuple_diskusage as DiskUsage

import pytest

from first_gateway.controllers.workers.health_alerter.disk import (
    DeviceMount,
    df_use_percent,
    disk_observations,
    disk_severity,
    group_by_device,
    parse_mountinfo,
)

# Captured from `docker run -v vol:/run/disk-watch/docker-data:ro
# -v dir:/run/disk-watch/checkout:ro -v file:/run/secrets/x.crt:ro` on Docker
# Desktop (macOS): the Docker data root is btrfs on /dev/vdb1, macOS paths
# arrive over virtiofs. Overlay options trimmed.
DOCKER_DESKTOP = """\
698 594 0:161 / / rw,relatime - overlay overlay rw,lowerdir=/var/lib/docker/overlay2/l/U53P,upperdir=/var/lib/docker/overlay2/24d6/diff
700 698 0:173 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw
701 698 0:174 / /dev rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
702 701 0:175 / /dev/pts rw,nosuid,noexec,relatime - devpts devpts rw,gid=5,mode=620,ptmxmode=666
703 698 0:176 / /sys ro,nosuid,nodev,noexec,relatime - sysfs sysfs ro
704 703 0:32 / /sys/fs/cgroup ro,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw,nsdelegate
705 701 0:171 / /dev/mqueue rw,nosuid,nodev,noexec,relatime - mqueue mqueue rw
706 701 0:177 / /dev/shm rw,nosuid,nodev,noexec,relatime - tmpfs shm rw,size=8211456k
707 698 0:38 /docker/containers/eb69/resolv.conf /etc/resolv.conf rw,noatime - btrfs /dev/vdb1 rw,ssd,subvolid=256,subvol=/docker
708 698 0:38 /docker/containers/eb69/hostname /etc/hostname rw,noatime - btrfs /dev/vdb1 rw,ssd,subvolid=256,subvol=/docker
709 698 0:38 /docker/containers/eb69/hosts /etc/hosts rw,noatime - btrfs /dev/vdb1 rw,ssd,subvolid=256,subvol=/docker
710 698 0:35 /private/tmp/scratchpad/file.txt /run/secrets/x.crt ro,relatime - virtiofs mac rw
711 698 0:38 /docker/volumes/diskwatch-test/_data /run/disk-watch/docker-data ro,noatime master:69 - btrfs /dev/vdb1 rw,ssd,subvolid=256,subvol=/docker
712 698 0:35 /private/tmp/scratchpad/watch /run/disk-watch/checkout ro,relatime - virtiofs mac rw
597 700 0:173 /bus /proc/bus ro,nosuid,nodev,noexec,relatime - proc proc rw
603 700 0:178 / /proc/asound ro,relatime - tmpfs tmpfs ro,size=4k,nr_inodes=1
604 700 0:174 /null /proc/interrupts rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
"""

# The same container shape on a VM: Docker's data root on /dev/sda2, the
# checkout (secret files, pki/upstream) on an LVM volume.
VM = """\
1 0 0:50 / / rw,relatime - overlay overlay rw,lowerdir=/var/lib/docker/overlay2/l/A
2 1 0:51 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw
3 1 0:52 / /dev/shm rw,nosuid,nodev,noexec,relatime - tmpfs shm rw,size=65536k
4 1 8:2 /var/lib/docker/containers/c1/hosts /etc/hosts rw,relatime - ext4 /dev/sda2 rw
5 1 253:3 /inference-gateway/pki/first-control.key /run/secrets/pilot_client.key ro,relatime - xfs /dev/mapper/vg01-webportal rw
6 1 253:3 /inference-gateway/pki/upstream /run/secrets/upstream ro,relatime - xfs /dev/mapper/vg01-webportal rw
7 1 0:60 / /run/log-shipping ro,relatime - tmpfs tmpfs rw,size=1024k,mode=1777
8 1 8:2 /var/lib/docker/volumes/first_disk-watch/_data /run/disk-watch/docker-data ro,relatime master:1 - ext4 /dev/sda2 rw
9 1 253:3 /inference-gateway/deploy/disk-watch /run/disk-watch/checkout ro,relatime - xfs /dev/mapper/vg01-webportal rw
10 1 8:2 /tmp/a\\040b /mnt/a\\040b rw,relatime - ext4 /dev/sda2 rw
"""


def test_parse_keeps_only_block_devices() -> None:
    mounts = parse_mountinfo(DOCKER_DESKTOP)
    assert {m.device for m in mounts} == {"/dev/vdb1"}
    assert "/" not in {m.mountpoint for m in mounts}  # overlay root
    assert "/run/secrets/x.crt" not in {m.mountpoint for m in mounts}  # virtiofs


def test_parse_unescapes_and_tolerates_junk() -> None:
    mounts = parse_mountinfo(VM + "garbage\n\n1 2 3 - ext4\n")
    assert DeviceMount("/dev/sda2", "/mnt/a b") in mounts
    assert len(mounts) == 6


def test_group_by_device_puts_watch_mount_first() -> None:
    groups = group_by_device(parse_mountinfo(VM))
    assert list(groups) == ["/dev/sda2", "/dev/mapper/vg01-webportal"]
    assert groups["/dev/sda2"][0] == "/run/disk-watch/docker-data"
    assert groups["/dev/mapper/vg01-webportal"][0] == "/run/disk-watch/checkout"


@pytest.mark.parametrize(
    ("used", "avail", "expected"),
    [
        (88, 12, 88),
        (881, 119, 89),  # rounded up, like df
        (0, 100, 0),
        (100, 0, 100),
        (0, 0, None),
    ],
)
def test_df_use_percent(used: int, avail: int, expected: int | None) -> None:
    assert df_use_percent(used, avail) == expected


def test_df_use_percent_ignores_reserved_blocks() -> None:
    # 1000 blocks, 50 reserved for root: 900 used leaves 50 for users. df says
    # 95% (900 / 950), not 90% (900 / 1000).
    assert df_use_percent(900, 50) == 95


@pytest.mark.parametrize(
    ("use", "severity"),
    [(70, None), (71, "info"), (80, "info"), (81, "warn"), (90, "warn"), (91, "crit")],
)
def test_disk_severity(use: int, severity: str | None) -> None:
    assert disk_severity(use) == severity


def _usage(
    pct_by_path: dict[str, int],
) -> tuple[Callable[[str], DiskUsage], list[str]]:
    stat_calls: list[str] = []

    def disk_usage(path: str) -> DiskUsage:
        stat_calls.append(path)
        pct = pct_by_path[path]
        return DiskUsage(total=100, used=pct, free=100 - pct)

    return disk_usage, stat_calls


def test_disk_observations_one_per_device(tmp_path: Path) -> None:
    mountinfo = tmp_path / "mountinfo"
    mountinfo.write_text(VM)
    disk_usage, stat_calls = _usage(
        {"/run/disk-watch/docker-data": 88, "/run/disk-watch/checkout": 95}
    )
    obs = disk_observations(mountinfo, disk_usage)

    assert stat_calls == ["/run/disk-watch/docker-data", "/run/disk-watch/checkout"]
    assert [(o.key, o.display_name, o.summary, o.severity, o.status) for o in obs] == [
        (
            "disk:/dev/sda2",
            "Disk /dev/sda2",
            "/dev/sda2 88% full (docker-data)",
            "warn",
            "warn",
        ),
        (
            "disk:/dev/mapper/vg01-webportal",
            "Disk /dev/mapper/vg01-webportal",
            "/dev/mapper/vg01-webportal 95% full (checkout)",
            "crit",
            "crit",
        ),
    ]
    # Never names the secret files mounted from the same device.
    assert not any("secrets" in o.summary for o in obs)


def test_disk_observations_below_threshold_and_without_watch(tmp_path: Path) -> None:
    mountinfo = tmp_path / "mountinfo"
    mountinfo.write_text(
        "4 1 8:2 /c/hosts /etc/hosts rw - ext4 /dev/sda2 rw\n"
        "5 1 8:3 /x /data rw - ext4 /dev/sdb1 rw\n"
    )
    disk_usage, _ = _usage({"/etc/hosts": 75, "/data": 50})
    obs = disk_observations(mountinfo, disk_usage)
    assert [(o.key, o.summary, o.severity) for o in obs] == [
        ("disk:/dev/sda2", "/dev/sda2 75% full", "info")
    ]


def test_disk_observations_skips_unstattable_mount(tmp_path: Path) -> None:
    mountinfo = tmp_path / "mountinfo"
    mountinfo.write_text(VM)

    def disk_usage(path: str) -> DiskUsage:
        if path == "/run/disk-watch/docker-data":
            raise PermissionError(path)
        return DiskUsage(total=100, used=99, free=1)

    obs = disk_observations(mountinfo, disk_usage)
    assert [o.key for o in obs] == ["disk:/dev/mapper/vg01-webportal"]


def test_disk_observations_without_mount_table(tmp_path: Path) -> None:
    def disk_usage(path: str) -> DiskUsage:
        raise AssertionError("must not stat without a mount table")

    assert disk_observations(tmp_path / "missing", disk_usage) == []
