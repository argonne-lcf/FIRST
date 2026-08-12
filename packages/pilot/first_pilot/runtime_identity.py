"""Execution-time identity checks for externally frozen pilot assets."""

from __future__ import annotations

import argparse
import grp
import hashlib
import os
import pwd
import re
import stat
import sys
from pathlib import Path

from first_common.schema.pilot import PilotRuntimeConfig

_SERVICE_USER = "openinference_svc"
_SERVICE_GROUP = "inference_service"
_PYTHON_UID = 0
_PYTHON_GID = 0
_HEX40 = re.compile(r"[0-9a-f]{40}")
_HEX64 = re.compile(r"[0-9a-f]{64}")
_MANIFEST_ROW = re.compile(r"([0-9a-f]{64})  ([A-Za-z0-9_./+-]+)")
_SOURCE_KEYS = frozenset(
    {
        "schema_version",
        "first_commit",
        "first_source_date_epoch",
        "first_uv_lock_sha256",
        "deployment_commit",
        "site_policy_sha256",
        "source_wheel_lock_sha256",
        "uv_version",
        "uv_sha256",
        "uv_build_version",
        "uv_build_sha256",
        "python",
        "python_sha256",
        "runtime_packages",
        "pilot_entrypoint",
        "bundle",
    }
)
_MANIFEST_EXEMPT = frozenset({"RUNTIME.SHA256SUMS", ".published"})
_PYTHON_LAUNCHERS = frozenset({"bin/python", "bin/python3", "bin/python3.12"})


class PilotRuntimeIdentityError(RuntimeError):
    """The frozen pilot runtime cannot be proven identical."""


def _digest(path: Path) -> str:
    value = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1 << 20), b""):
                value.update(block)
    except OSError as exc:
        raise PilotRuntimeIdentityError(f"runtime file is unreadable: {path}") from exc
    return value.hexdigest()


def _service_ids() -> tuple[int, int]:
    try:
        return (
            pwd.getpwnam(_SERVICE_USER).pw_uid,
            grp.getgrnam(_SERVICE_GROUP).gr_gid,
        )
    except KeyError as exc:
        raise PilotRuntimeIdentityError(
            "production service identity does not resolve"
        ) from exc


def _parse_key_values(raw: bytes, *, expected: frozenset[str]) -> dict[str, str]:
    try:
        lines = raw.decode("ascii").splitlines()
    except UnicodeDecodeError as exc:
        raise PilotRuntimeIdentityError("runtime source identity is not ASCII") from exc
    values: dict[str, str] = {}
    for line in lines:
        key, separator, value = line.partition("=")
        if (
            not separator
            or re.fullmatch(r"[a-z0-9_]+", key) is None
            or not value
            or value != value.strip()
            or key in values
        ):
            raise PilotRuntimeIdentityError("runtime source identity framing differs")
        values[key] = value
    if set(values) != expected:
        raise PilotRuntimeIdentityError("runtime source identity field set differs")
    return values


def _parse_manifest(raw: bytes) -> dict[str, str]:
    try:
        lines = raw.decode("ascii").splitlines()
    except UnicodeDecodeError as exc:
        raise PilotRuntimeIdentityError("runtime manifest is not ASCII") from exc
    manifest: dict[str, str] = {}
    for line in lines:
        match = _MANIFEST_ROW.fullmatch(line)
        if match is None:
            raise PilotRuntimeIdentityError("runtime manifest framing differs")
        relative_path = Path(match.group(2))
        relative = relative_path.as_posix()
        if relative_path.is_absolute() or ".." in relative_path.parts:
            raise PilotRuntimeIdentityError("runtime manifest path escapes")
        if relative in manifest:
            raise PilotRuntimeIdentityError("runtime manifest path is duplicated")
        manifest[relative] = match.group(1)
    if not manifest:
        raise PilotRuntimeIdentityError("runtime manifest is empty")
    return manifest


def _parse_symlinks(raw: bytes) -> dict[str, str]:
    try:
        lines = raw.decode("utf-8").splitlines()
    except UnicodeDecodeError as exc:
        raise PilotRuntimeIdentityError(
            "runtime symlink inventory is not UTF-8"
        ) from exc
    if len(lines) > 4096:
        raise PilotRuntimeIdentityError("runtime symlink inventory is unbounded")
    result: dict[str, str] = {}
    for line in lines:
        relative, separator, target = line.partition("\t")
        path = Path(relative)
        normalized = path.as_posix()
        if (
            not separator
            or not relative
            or not target
            or len(line) > 8192
            or path.is_absolute()
            or ".." in path.parts
            or normalized in result
        ):
            raise PilotRuntimeIdentityError("runtime symlink inventory framing differs")
        result[normalized] = target
    return result


def _validate_external_python(values: dict[str, str]) -> Path:
    target = Path(values["python"])
    try:
        info = target.lstat()
    except FileNotFoundError as exc:
        raise PilotRuntimeIdentityError("external Python is absent") from exc
    if (
        not target.is_absolute()
        or target.is_symlink()
        or not stat.S_ISREG(info.st_mode)
        or info.st_nlink != 1
        or info.st_uid != _PYTHON_UID
        or info.st_gid != _PYTHON_GID
        or stat.S_IMODE(info.st_mode) != 0o755
        or _HEX64.fullmatch(values["python_sha256"]) is None
        or _digest(target) != values["python_sha256"]
    ):
        raise PilotRuntimeIdentityError("external Python identity differs")
    return target.resolve()


def verify_frozen_runtime(
    runtime: Path,
    expected_manifest_sha256: str,
    expected_source_identity_sha256: str,
) -> None:
    """Verify the complete published pilot tree against external anchors."""
    if (
        _HEX64.fullmatch(expected_manifest_sha256) is None
        or _HEX64.fullmatch(expected_source_identity_sha256) is None
    ):
        raise PilotRuntimeIdentityError("external runtime anchor is malformed")
    if not runtime.is_absolute():
        raise PilotRuntimeIdentityError("runtime path is not absolute")
    uid, gid = _service_ids()
    try:
        root_info = runtime.lstat()
    except FileNotFoundError as exc:
        raise PilotRuntimeIdentityError("runtime is absent") from exc
    if (
        runtime.is_symlink()
        or not stat.S_ISDIR(root_info.st_mode)
        or root_info.st_uid != uid
        or root_info.st_gid != gid
        or stat.S_IMODE(root_info.st_mode) != 0o550
    ):
        raise PilotRuntimeIdentityError("runtime root identity differs")
    runtime = runtime.resolve()

    manifest_path = runtime / "RUNTIME.SHA256SUMS"
    source_path = runtime / "FIRST_SOURCE_IDENTITIES"
    symlink_path = runtime / "RUNTIME.SYMLINKS"
    published_path = runtime / ".published"
    try:
        manifest_raw = manifest_path.read_bytes()
        source_raw = source_path.read_bytes()
        symlink_raw = symlink_path.read_bytes()
        published_raw = published_path.read_bytes()
    except OSError as exc:
        raise PilotRuntimeIdentityError(
            "runtime identity marker is unreadable"
        ) from exc
    if published_raw:
        raise PilotRuntimeIdentityError("runtime publication marker content differs")
    if hashlib.sha256(manifest_raw).hexdigest() != expected_manifest_sha256:
        raise PilotRuntimeIdentityError("runtime manifest anchor differs")
    if hashlib.sha256(source_raw).hexdigest() != expected_source_identity_sha256:
        raise PilotRuntimeIdentityError("runtime source identity anchor differs")

    source = _parse_key_values(source_raw, expected=_SOURCE_KEYS)
    if (
        source["schema_version"] != "1"
        or _HEX40.fullmatch(source["first_commit"]) is None
        or _HEX40.fullmatch(source["deployment_commit"]) is None
        or not source["first_source_date_epoch"].isdecimal()
        or source["runtime_packages"] != "first-common,first-pilot"
        or source["pilot_entrypoint"] != "first_pilot.control_api:entrypoint"
        or Path(source["bundle"]).is_absolute()
        or ".." in Path(source["bundle"]).parts
        or any(
            _HEX64.fullmatch(source[name]) is None
            for name in (
                "first_uv_lock_sha256",
                "site_policy_sha256",
                "source_wheel_lock_sha256",
                "uv_sha256",
                "uv_build_sha256",
            )
        )
    ):
        raise PilotRuntimeIdentityError("runtime source identity values differ")
    external_python = _validate_external_python(source)
    manifest = _parse_manifest(manifest_raw)
    expected_symlinks = _parse_symlinks(symlink_raw)

    regular_files: set[str] = set()
    actual_symlinks: dict[str, str] = {}
    for candidate in runtime.rglob("*"):
        try:
            info = candidate.lstat()
        except OSError as exc:
            raise PilotRuntimeIdentityError("runtime tree is unreadable") from exc
        relative = candidate.relative_to(runtime).as_posix()
        if stat.S_ISLNK(info.st_mode):
            if info.st_nlink != 1 or info.st_uid != uid or info.st_gid != gid:
                raise PilotRuntimeIdentityError(
                    f"runtime symlink identity differs: {relative}"
                )
            try:
                actual_symlinks[relative] = os.readlink(candidate)
            except OSError as exc:
                raise PilotRuntimeIdentityError(
                    f"runtime symlink is unreadable: {relative}"
                ) from exc
        elif stat.S_ISDIR(info.st_mode):
            if (
                info.st_uid != uid
                or info.st_gid != gid
                or stat.S_IMODE(info.st_mode) != 0o550
            ):
                raise PilotRuntimeIdentityError(
                    f"runtime directory identity differs: {relative}"
                )
        elif stat.S_ISREG(info.st_mode):
            if (
                info.st_nlink != 1
                or info.st_uid != uid
                or info.st_gid != gid
                or stat.S_IMODE(info.st_mode) not in {0o440, 0o550}
            ):
                raise PilotRuntimeIdentityError(
                    f"runtime file identity differs: {relative}"
                )
            regular_files.add(relative)
        else:
            raise PilotRuntimeIdentityError(
                f"runtime contains special entry: {relative}"
            )

    if actual_symlinks != expected_symlinks:
        raise PilotRuntimeIdentityError("runtime symlink inventory differs")
    for relative in actual_symlinks:
        try:
            resolved = (runtime / relative).resolve(strict=True)
        except (OSError, RuntimeError) as exc:
            raise PilotRuntimeIdentityError("runtime symlink is broken") from exc
        if relative in _PYTHON_LAUNCHERS:
            if resolved != external_python:
                raise PilotRuntimeIdentityError("runtime Python launcher differs")
        elif not resolved.is_relative_to(runtime):
            raise PilotRuntimeIdentityError("runtime symlink escapes")

    if set(manifest) != regular_files - _MANIFEST_EXEMPT:
        raise PilotRuntimeIdentityError("runtime manifest coverage differs")
    for relative, expected in manifest.items():
        if _digest(runtime / relative) != expected:
            raise PilotRuntimeIdentityError(f"runtime digest differs: {relative}")

    module_path = Path(__file__).resolve()
    if module_path.is_relative_to(runtime):
        relative_module = module_path.relative_to(runtime).as_posix()
        if manifest.get(relative_module) != _digest(module_path):
            raise PilotRuntimeIdentityError("runtime verifier is not manifest-bound")


def validate_configured_runtime(config: PilotRuntimeConfig) -> None:
    manifest_sha = config.pilot_runtime_manifest_sha256
    source_sha = config.pilot_source_identity_sha256
    if manifest_sha is None and source_sha is None:
        return
    if manifest_sha is None or source_sha is None:
        raise PilotRuntimeIdentityError("pilot runtime anchors are incomplete")
    verify_frozen_runtime(Path(sys.prefix), manifest_sha, source_sha)


def validate_nginx_identity(config: PilotRuntimeConfig) -> None:
    """Validate the configured immutable NGINX immediately before startup."""
    expected = config.nginx_sha256
    if expected is None:
        return
    try:
        uid, gid = _service_ids()
        info = config.nginx_path.lstat()
    except FileNotFoundError as exc:
        raise RuntimeError("frozen NGINX identity is unavailable") from exc
    if (
        not config.nginx_path.is_absolute()
        or config.nginx_path.is_symlink()
        or not stat.S_ISREG(info.st_mode)
        or info.st_nlink != 1
        or info.st_uid != uid
        or info.st_gid != gid
        or stat.S_IMODE(info.st_mode) != 0o550
        or not os.access(config.nginx_path, os.X_OK)
    ):
        raise RuntimeError("frozen NGINX executable identity differs")
    if _digest(config.nginx_path) != expected:
        raise RuntimeError("frozen NGINX executable digest differs")


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify a frozen FIRST pilot runtime")
    parser.add_argument("--runtime", required=True, type=Path)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--source-identity-sha256", required=True)
    args = parser.parse_args()
    verify_frozen_runtime(
        args.runtime,
        args.manifest_sha256,
        args.source_identity_sha256,
    )
    print("pilot_runtime_identity=PASS")


if __name__ == "__main__":
    main()
