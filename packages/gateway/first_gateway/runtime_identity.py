"""Fail-closed identity for the frozen FIRST v2 control-plane runtime."""

from __future__ import annotations

import grp
import hashlib
import os
import pwd
import re
import stat
import sys
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict

_SERVICE_USER = "openinference_svc"
_SERVICE_GROUP = "inference_service"
_PYTHON_UID = 0
_PYTHON_GID = 0
_HEX40 = re.compile(r"[0-9a-f]{40}")
_HEX64 = re.compile(r"[0-9a-f]{64}")
_MANIFEST_ROW = re.compile(r"([0-9a-f]{64})  ([A-Za-z0-9_./+-]+)")
_IDENTITY_KEYS = frozenset(
    {
        "schema_version",
        "runtime_kind",
        "runtime_status",
        "first_commit",
        "deployment_commit",
        "runtime_manifest_sha256",
        "gateway_entrypoint",
        "controller_entrypoint",
        "admin_entrypoint",
        "gateway_runtime_identity_path",
        "controller_runtime_identity_url",
        "authoritative_apply_sha256",
        "tara_renderer_sha256",
        "tara_resource_template_sha256",
        "site_policy_sha256",
        "python",
        "python_sha256",
    }
)
_MANIFEST_EXEMPT = frozenset(
    {
        "FIRST_CONTROL_RUNTIME_IDENTITIES",
        "RUNTIME.SHA256SUMS",
        ".published",
    }
)
_PYTHON_LAUNCHERS = frozenset({"bin/python", "bin/python3", "bin/python3.12"})


class RuntimeIdentityError(RuntimeError):
    """The externally provisioned runtime identity cannot be trusted."""


class RuntimeIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1]
    role: Literal["gateway", "controller"]
    runtime_kind: Literal["sophia-firstv2-control-plane"]
    runtime_status: Literal["published"]
    first_commit: str
    deployment_commit: str
    runtime_manifest_sha256: str


def _digest(path: Path) -> str:
    value = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1 << 20), b""):
                value.update(block)
    except OSError as exc:
        raise RuntimeIdentityError(f"runtime file is unreadable: {path}") from exc
    return value.hexdigest()


def _service_ids() -> tuple[int, int]:
    try:
        return (
            pwd.getpwnam(_SERVICE_USER).pw_uid,
            grp.getgrnam(_SERVICE_GROUP).gr_gid,
        )
    except KeyError as exc:
        raise RuntimeIdentityError(
            "production service identity does not resolve"
        ) from exc


def _require_directory(path: Path, *, uid: int, gid: int) -> None:
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise RuntimeIdentityError(f"runtime directory is absent: {path}") from exc
    if (
        path.is_symlink()
        or not stat.S_ISDIR(info.st_mode)
        or info.st_uid != uid
        or info.st_gid != gid
        or stat.S_IMODE(info.st_mode) != 0o550
    ):
        raise RuntimeIdentityError(f"runtime directory identity differs: {path}")


def _require_file(
    path: Path,
    *,
    uid: int,
    gid: int,
    modes: frozenset[int],
    max_bytes: int,
    allow_empty: bool = False,
) -> bytes:
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise RuntimeIdentityError(
            f"runtime identity file is absent: {path.name}"
        ) from exc
    if (
        path.is_symlink()
        or not stat.S_ISREG(info.st_mode)
        or info.st_nlink != 1
        or info.st_uid != uid
        or info.st_gid != gid
        or stat.S_IMODE(info.st_mode) not in modes
        or (not allow_empty and info.st_size <= 0)
        or info.st_size > max_bytes
    ):
        raise RuntimeIdentityError(
            f"runtime identity file metadata differs: {path.name}"
        )
    try:
        return path.read_bytes()
    except OSError as exc:
        raise RuntimeIdentityError(
            f"runtime identity file is unreadable: {path.name}"
        ) from exc


def _parse_identity(raw: bytes) -> dict[str, str]:
    try:
        lines = raw.decode("ascii").splitlines()
    except UnicodeDecodeError as exc:
        raise RuntimeIdentityError("runtime identity is not ASCII") from exc

    values: dict[str, str] = {}
    for line in lines:
        name, separator, value = line.partition("=")
        if (
            not separator
            or re.fullmatch(r"[a-z0-9_]+", name) is None
            or not value
            or value != value.strip()
            or name in values
        ):
            raise RuntimeIdentityError("runtime identity framing differs")
        values[name] = value
    if set(values) != _IDENTITY_KEYS:
        raise RuntimeIdentityError("runtime identity field set differs")
    return values


def _validate_controller_url(raw: str) -> None:
    try:
        parsed = urlsplit(raw)
        port = parsed.port
    except ValueError as exc:
        raise RuntimeIdentityError(
            "controller runtime identity URL is malformed"
        ) from exc
    if (
        parsed.scheme != "http"
        or parsed.hostname not in {"127.0.0.1", "::1"}
        or parsed.username is not None
        or parsed.password is not None
        or port is None
        or parsed.query
        or parsed.fragment
        or parsed.path != "/runtime-identity"
    ):
        raise RuntimeIdentityError(
            "controller runtime identity URL is not fixed loopback HTTP"
        )


def _parse_manifest(raw: bytes) -> dict[str, str]:
    try:
        lines = raw.decode("ascii").splitlines()
    except UnicodeDecodeError as exc:
        raise RuntimeIdentityError("runtime manifest is not ASCII") from exc
    manifest: dict[str, str] = {}
    for line in lines:
        match = _MANIFEST_ROW.fullmatch(line)
        if match is None:
            raise RuntimeIdentityError("runtime manifest framing differs")
        relative_path = Path(match.group(2))
        relative = relative_path.as_posix()
        if relative_path.is_absolute() or ".." in relative_path.parts:
            raise RuntimeIdentityError("runtime manifest path escapes the runtime")
        if relative in manifest:
            raise RuntimeIdentityError("runtime manifest contains a duplicate path")
        manifest[relative] = match.group(1)
    if not manifest:
        raise RuntimeIdentityError("runtime manifest is empty")
    return manifest


def _parse_symlinks(raw: bytes) -> dict[str, str]:
    try:
        lines = raw.decode("utf-8").splitlines()
    except UnicodeDecodeError as exc:
        raise RuntimeIdentityError("runtime symlink inventory is not UTF-8") from exc
    if len(lines) > 4096:
        raise RuntimeIdentityError("runtime symlink inventory is unbounded")

    symlinks: dict[str, str] = {}
    for line in lines:
        relative, separator, target = line.partition("\t")
        relative_path = Path(relative)
        normalized = relative_path.as_posix()
        if (
            not separator
            or not relative
            or not target
            or len(line) > 8192
            or relative_path.is_absolute()
            or ".." in relative_path.parts
            or normalized in symlinks
        ):
            raise RuntimeIdentityError("runtime symlink inventory framing differs")
        symlinks[normalized] = target
    return symlinks


def _validate_python(values: dict[str, str]) -> Path:
    python_target = Path(values["python"])
    try:
        info = python_target.lstat()
    except FileNotFoundError as exc:
        raise RuntimeIdentityError("Sophia Python target is absent") from exc
    if (
        not python_target.is_absolute()
        or python_target.is_symlink()
        or not stat.S_ISREG(info.st_mode)
        or info.st_nlink != 1
        or info.st_uid != _PYTHON_UID
        or info.st_gid != _PYTHON_GID
        or stat.S_IMODE(info.st_mode) != 0o755
        or _HEX64.fullmatch(values["python_sha256"]) is None
        or _digest(python_target) != values["python_sha256"]
    ):
        raise RuntimeIdentityError("Sophia Python target identity differs")
    return python_target


def _validate_runtime_tree(
    runtime: Path,
    *,
    uid: int,
    gid: int,
    manifest: dict[str, str],
    expected_symlinks: dict[str, str],
    python_target: Path,
) -> None:
    regular_files: set[str] = set()
    actual_symlinks: dict[str, str] = {}
    try:
        candidates = list(runtime.rglob("*"))
    except OSError as exc:
        raise RuntimeIdentityError("runtime tree is unreadable") from exc

    for candidate in candidates:
        try:
            info = candidate.lstat()
        except OSError as exc:
            raise RuntimeIdentityError(
                f"runtime entry is unreadable: {candidate}"
            ) from exc
        relative = candidate.relative_to(runtime).as_posix()
        if stat.S_ISLNK(info.st_mode):
            if info.st_nlink != 1 or info.st_uid != uid or info.st_gid != gid:
                raise RuntimeIdentityError(
                    f"runtime symlink identity differs: {relative}"
                )
            try:
                actual_symlinks[relative] = os.readlink(candidate)
            except OSError as exc:
                raise RuntimeIdentityError(
                    f"runtime symlink is unreadable: {relative}"
                ) from exc
        elif stat.S_ISDIR(info.st_mode):
            if (
                info.st_uid != uid
                or info.st_gid != gid
                or stat.S_IMODE(info.st_mode) != 0o550
            ):
                raise RuntimeIdentityError(
                    f"runtime directory identity differs: {relative}"
                )
        elif stat.S_ISREG(info.st_mode):
            if (
                info.st_nlink != 1
                or info.st_uid != uid
                or info.st_gid != gid
                or stat.S_IMODE(info.st_mode) not in {0o440, 0o550}
            ):
                raise RuntimeIdentityError(f"runtime file identity differs: {relative}")
            regular_files.add(relative)
        else:
            raise RuntimeIdentityError(
                f"runtime contains a special filesystem entry: {relative}"
            )

    if actual_symlinks != expected_symlinks:
        raise RuntimeIdentityError("runtime symlink inventory differs")
    for relative in actual_symlinks:
        candidate = runtime / relative
        try:
            resolved = candidate.resolve(strict=True)
        except (OSError, RuntimeError) as exc:
            raise RuntimeIdentityError(
                f"runtime symlink is broken: {relative}"
            ) from exc
        if relative in _PYTHON_LAUNCHERS:
            if resolved != python_target:
                raise RuntimeIdentityError("Sophia Python launcher target differs")
        elif not resolved.is_relative_to(runtime):
            raise RuntimeIdentityError(f"runtime symlink escapes: {relative}")

    if set(manifest) != regular_files - _MANIFEST_EXEMPT:
        raise RuntimeIdentityError(
            "runtime manifest does not cover the complete regular-file set"
        )
    for relative, expected in manifest.items():
        candidate = runtime / relative
        if _digest(candidate) != expected:
            raise RuntimeIdentityError(f"runtime manifest digest differs: {relative}")


def load_runtime_identity(
    role: Literal["gateway", "controller"],
) -> RuntimeIdentity:
    """Revalidate and return the externally frozen same-commit identity.

    Deliberately uncached: the service-owned runtime is writable by its owner,
    so a prior successful check cannot be reused as current integrity proof.
    """
    uid, gid = _service_ids()
    runtime_raw = Path(sys.prefix)
    if not runtime_raw.is_absolute():
        raise RuntimeIdentityError("executing runtime path is not absolute")
    _require_directory(runtime_raw, uid=uid, gid=gid)
    runtime = runtime_raw.resolve()
    _require_directory(runtime / "bin", uid=uid, gid=gid)

    identity_path = runtime / "FIRST_CONTROL_RUNTIME_IDENTITIES"
    manifest_path = runtime / "RUNTIME.SHA256SUMS"
    symlinks_path = runtime / "RUNTIME.SYMLINKS"
    site_policy_path = runtime / "SITE_ENDPOINT_IDENTITIES"
    published_path = runtime / ".published"
    identity_raw = _require_file(
        identity_path,
        uid=uid,
        gid=gid,
        modes=frozenset({0o440}),
        max_bytes=16_384,
    )
    manifest_raw = _require_file(
        manifest_path,
        uid=uid,
        gid=gid,
        modes=frozenset({0o440}),
        max_bytes=16 << 20,
    )
    symlinks_raw = _require_file(
        symlinks_path,
        uid=uid,
        gid=gid,
        modes=frozenset({0o440}),
        max_bytes=1 << 20,
        allow_empty=True,
    )
    site_policy_raw = _require_file(
        site_policy_path,
        uid=uid,
        gid=gid,
        modes=frozenset({0o440}),
        max_bytes=16_384,
    )
    published_raw = _require_file(
        published_path,
        uid=uid,
        gid=gid,
        modes=frozenset({0o440}),
        max_bytes=16_384,
        allow_empty=True,
    )
    if published_raw:
        raise RuntimeIdentityError("runtime publication marker content differs")

    values = _parse_identity(identity_raw)
    if (
        values["schema_version"] != "1"
        or values["runtime_kind"] != "sophia-firstv2-control-plane"
        or values["runtime_status"] != "published"
        or _HEX40.fullmatch(values["first_commit"]) is None
        or _HEX40.fullmatch(values["deployment_commit"]) is None
        or _HEX64.fullmatch(values["runtime_manifest_sha256"]) is None
        or values["runtime_manifest_sha256"] != hashlib.sha256(manifest_raw).hexdigest()
        or values["gateway_entrypoint"] != "first_gateway.apiserver.api:app"
        or values["controller_entrypoint"] != "first_gateway.controllers.manager"
        or values["admin_entrypoint"] != "alcf_ai:main"
        or values["gateway_runtime_identity_path"] != "/control/v1/runtime-identity"
        or any(
            _HEX64.fullmatch(values[name]) is None
            for name in (
                "authoritative_apply_sha256",
                "tara_renderer_sha256",
                "tara_resource_template_sha256",
                "site_policy_sha256",
            )
        )
        or values["site_policy_sha256"] != hashlib.sha256(site_policy_raw).hexdigest()
    ):
        raise RuntimeIdentityError("runtime identity values differ")

    _validate_controller_url(values["controller_runtime_identity_url"])
    python_target = _validate_python(values)
    manifest = _parse_manifest(manifest_raw)
    if (
        manifest.get("bin/firstv2-authoritative-apply")
        != values["authoritative_apply_sha256"]
    ):
        raise RuntimeIdentityError("authoritative apply is not bound by the manifest")
    _validate_runtime_tree(
        runtime,
        uid=uid,
        gid=gid,
        manifest=manifest,
        expected_symlinks=_parse_symlinks(symlinks_raw),
        python_target=python_target,
    )

    source = Path(__file__).resolve()
    if not source.is_relative_to(runtime):
        raise RuntimeIdentityError("runtime identity module escapes the frozen runtime")
    relative_source = source.relative_to(runtime).as_posix()
    if manifest.get(relative_source) != _digest(source):
        raise RuntimeIdentityError(
            "runtime identity module is not bound by the manifest"
        )

    return RuntimeIdentity(
        schema_version=1,
        role=role,
        runtime_kind="sophia-firstv2-control-plane",
        runtime_status="published",
        first_commit=values["first_commit"],
        deployment_commit=values["deployment_commit"],
        runtime_manifest_sha256=values["runtime_manifest_sha256"],
    )
