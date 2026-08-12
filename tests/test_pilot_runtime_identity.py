import hashlib
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from first_pilot import runtime_identity
from first_pilot.runtime_identity import (
    PilotRuntimeIdentityError,
    validate_nginx_identity,
    verify_frozen_runtime,
)


def _patch_service_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        runtime_identity,
        "_service_ids",
        lambda: (os.geteuid(), os.getegid()),
    )


def test_nginx_identity_accepts_exact_service_owned_binary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_service_ids(monkeypatch)
    nginx = tmp_path / "nginx"
    payload = b"frozen nginx\n"
    nginx.write_bytes(payload)
    nginx.chmod(0o550)
    config = SimpleNamespace(
        nginx_path=nginx,
        nginx_sha256=hashlib.sha256(payload).hexdigest(),
    )
    validate_nginx_identity(config)  # type: ignore[arg-type]


def test_nginx_identity_rejects_digest_mode_and_symlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_service_ids(monkeypatch)
    nginx = tmp_path / "nginx"
    nginx.write_bytes(b"frozen nginx\n")
    nginx.chmod(0o550)
    config = SimpleNamespace(nginx_path=nginx, nginx_sha256="0" * 64)
    with pytest.raises(RuntimeError, match="digest differs"):
        validate_nginx_identity(config)  # type: ignore[arg-type]

    config.nginx_sha256 = hashlib.sha256(nginx.read_bytes()).hexdigest()
    nginx.chmod(0o750)
    with pytest.raises(RuntimeError, match="identity differs"):
        validate_nginx_identity(config)  # type: ignore[arg-type]

    nginx.chmod(0o550)
    link = tmp_path / "nginx-link"
    link.symlink_to(nginx)
    config.nginx_path = link
    with pytest.raises(RuntimeError, match="identity differs"):
        validate_nginx_identity(config)  # type: ignore[arg-type]


def _build_frozen_pilot_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, str, str, Path]:
    _patch_service_ids(monkeypatch)
    monkeypatch.setattr(runtime_identity, "_PYTHON_UID", os.geteuid())
    monkeypatch.setattr(runtime_identity, "_PYTHON_GID", os.getegid())
    python = tmp_path / "site-python"
    python.write_bytes(b"site python\n")
    python.chmod(0o755)

    root = tmp_path / "runtime"
    module = root / "lib/python3.12/site-packages/first_pilot/control_api.py"
    module.parent.mkdir(parents=True)
    module.write_bytes(b"# frozen pilot module\n")
    module.chmod(0o440)
    for name in ("python", "python3", "python3.12"):
        launcher = root / "bin" / name
        launcher.parent.mkdir(parents=True, exist_ok=True)
        launcher.symlink_to(python)

    symlink_payload = "".join(
        f"bin/{name}\t{python}\n" for name in ("python", "python3", "python3.12")
    ).encode()
    symlinks = root / "RUNTIME.SYMLINKS"
    symlinks.write_bytes(symlink_payload)
    symlinks.chmod(0o440)
    python_sha = hashlib.sha256(python.read_bytes()).hexdigest()
    source_values = {
        "schema_version": "1",
        "first_commit": "1" * 40,
        "first_source_date_epoch": "1700000000",
        "first_uv_lock_sha256": "2" * 64,
        "deployment_commit": "3" * 40,
        "site_policy_sha256": "4" * 64,
        "source_wheel_lock_sha256": "5" * 64,
        "uv_version": "0.12.3",
        "uv_sha256": "6" * 64,
        "uv_build_version": "0.11.33",
        "uv_build_sha256": "7" * 64,
        "python": str(python),
        "python_sha256": python_sha,
        "runtime_packages": "first-common,first-pilot",
        "pilot_entrypoint": "first_pilot.control_api:entrypoint",
        "bundle": "share/tara-nemotron-firstv2",
    }
    source_payload = "".join(
        f"{key}={value}\n" for key, value in source_values.items()
    ).encode()
    source = root / "FIRST_SOURCE_IDENTITIES"
    source.write_bytes(source_payload)
    source.chmod(0o440)
    files = {
        "FIRST_SOURCE_IDENTITIES": source_payload,
        "RUNTIME.SYMLINKS": symlink_payload,
        module.relative_to(root).as_posix(): module.read_bytes(),
    }
    manifest_payload = "".join(
        f"{hashlib.sha256(payload).hexdigest()}  {relative}\n"
        for relative, payload in sorted(files.items())
    ).encode()
    manifest = root / "RUNTIME.SHA256SUMS"
    manifest.write_bytes(manifest_payload)
    manifest.chmod(0o440)
    (root / ".published").touch()
    (root / ".published").chmod(0o440)
    for directory in sorted(
        [root, *(path for path in root.rglob("*") if path.is_dir())],
        key=lambda path: len(path.parts),
        reverse=True,
    ):
        directory.chmod(0o550)
    return (
        root,
        hashlib.sha256(manifest_payload).hexdigest(),
        hashlib.sha256(source_payload).hexdigest(),
        module,
    )


def test_full_pilot_runtime_verifier_detects_module_tamper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, manifest_sha, source_sha, module = _build_frozen_pilot_runtime(
        tmp_path, monkeypatch
    )
    try:
        verify_frozen_runtime(root, manifest_sha, source_sha)
        module.chmod(0o640)
        module.write_bytes(b"# drifted pilot module\n")
        module.chmod(0o440)
        with pytest.raises(PilotRuntimeIdentityError, match="runtime digest differs"):
            verify_frozen_runtime(root, manifest_sha, source_sha)
    finally:
        for directory in [
            root,
            *(path for path in root.rglob("*") if path.is_dir()),
        ]:
            directory.chmod(0o750)


def test_full_pilot_runtime_verifier_rejects_extra_and_anchor_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, manifest_sha, source_sha, _module = _build_frozen_pilot_runtime(
        tmp_path, monkeypatch
    )
    try:
        with pytest.raises(PilotRuntimeIdentityError, match="manifest anchor differs"):
            verify_frozen_runtime(root, "0" * 64, source_sha)
        root.chmod(0o750)
        extra = root / "unexpected"
        extra.write_bytes(b"mutable\n")
        extra.chmod(0o440)
        root.chmod(0o550)
        with pytest.raises(
            PilotRuntimeIdentityError, match="manifest coverage differs"
        ):
            verify_frozen_runtime(root, manifest_sha, source_sha)
    finally:
        for directory in [
            root,
            *(path for path in root.rglob("*") if path.is_dir()),
        ]:
            directory.chmod(0o750)


def test_full_pilot_runtime_verifier_requires_exact_empty_publication_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, manifest_sha, source_sha, _module = _build_frozen_pilot_runtime(
        tmp_path, monkeypatch
    )
    try:
        marker = root / ".published"
        marker.chmod(0o640)
        marker.write_bytes(b"published\n")
        marker.chmod(0o440)
        with pytest.raises(
            PilotRuntimeIdentityError, match="publication marker content differs"
        ):
            verify_frozen_runtime(root, manifest_sha, source_sha)
    finally:
        for directory in [
            root,
            *(path for path in root.rglob("*") if path.is_dir()),
        ]:
            directory.chmod(0o750)


def test_full_pilot_runtime_verifier_rejects_hardlinked_symlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, manifest_sha, source_sha, _module = _build_frozen_pilot_runtime(
        tmp_path, monkeypatch
    )
    try:
        bin_dir = root / "bin"
        bin_dir.chmod(0o750)
        os.link(
            bin_dir / "python",
            bin_dir / "python-hardlink",
            follow_symlinks=False,
        )
        bin_dir.chmod(0o550)
        with pytest.raises(
            PilotRuntimeIdentityError, match="runtime symlink identity differs"
        ):
            verify_frozen_runtime(root, manifest_sha, source_sha)
    finally:
        for directory in [
            root,
            *(path for path in root.rglob("*") if path.is_dir()),
        ]:
            directory.chmod(0o750)
