import hashlib
import os
import sys
from collections.abc import Generator
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi.routing import APIRoute

from first_common.schema.auth import UserAuthEvent
from first_gateway import runtime_identity as runtime_identity_module
from first_gateway.apiserver.dependencies import get_admin_user
from first_gateway.apiserver.routes import control
from first_gateway.controllers import metrics_server
from first_gateway.runtime_identity import (
    RuntimeIdentity,
    RuntimeIdentityError,
    load_runtime_identity,
)


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _write(path: Path, payload: bytes, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    path.chmod(mode)


def _make_tree_readonly(runtime: Path) -> None:
    directories = [runtime, *(path for path in runtime.rglob("*") if path.is_dir())]
    for directory in sorted(
        directories, key=lambda path: len(path.parts), reverse=True
    ):
        directory.chmod(0o550)


@pytest.fixture
def frozen_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Generator[dict[str, Any], None, None]:
    uid, gid = os.geteuid(), os.getegid()
    monkeypatch.setattr(runtime_identity_module, "_service_ids", lambda: (uid, gid))
    monkeypatch.setattr(runtime_identity_module, "_PYTHON_UID", uid)
    monkeypatch.setattr(runtime_identity_module, "_PYTHON_GID", gid)

    external_python = tmp_path / "python3.12"
    _write(external_python, b"frozen external python\n", 0o755)
    runtime = tmp_path / "runtime"
    source = runtime / "lib/python3.12/site-packages/first_gateway/runtime_identity.py"
    apply = runtime / "bin/firstv2-authoritative-apply"
    source_payload = b"# frozen runtime identity module\n"
    apply_payload = b"#!/usr/bin/python3\n# frozen apply\n"
    site_payload = b"schema_version=1\n"
    _write(source, source_payload, 0o440)
    _write(apply, apply_payload, 0o550)
    _write(runtime / "SITE_ENDPOINT_IDENTITIES", site_payload, 0o440)

    symlinks = {
        f"bin/{name}": str(external_python)
        for name in ("python", "python3", "python3.12")
    }
    for relative, target in symlinks.items():
        (runtime / relative).symlink_to(target)
    symlink_payload = "".join(
        f"{relative}\t{target}\n" for relative, target in sorted(symlinks.items())
    ).encode()
    _write(runtime / "RUNTIME.SYMLINKS", symlink_payload, 0o440)

    manifest_files = {
        source.relative_to(runtime).as_posix(): source_payload,
        apply.relative_to(runtime).as_posix(): apply_payload,
        "SITE_ENDPOINT_IDENTITIES": site_payload,
        "RUNTIME.SYMLINKS": symlink_payload,
    }
    manifest_payload = "".join(
        f"{_digest(payload)}  {relative}\n"
        for relative, payload in sorted(manifest_files.items())
    ).encode("ascii")
    manifest_path = runtime / "RUNTIME.SHA256SUMS"
    _write(manifest_path, manifest_payload, 0o440)
    _write(runtime / ".published", b"", 0o440)

    values = {
        "schema_version": "1",
        "runtime_kind": "sophia-firstv2-control-plane",
        "runtime_status": "published",
        "first_commit": "1" * 40,
        "deployment_commit": "2" * 40,
        "runtime_manifest_sha256": _digest(manifest_payload),
        "gateway_entrypoint": "first_gateway.apiserver.api:app",
        "controller_entrypoint": "first_gateway.controllers.manager",
        "admin_entrypoint": "alcf_ai:main",
        "gateway_runtime_identity_path": "/control/v1/runtime-identity",
        "controller_runtime_identity_url": ("http://127.0.0.1:9100/runtime-identity"),
        "authoritative_apply_sha256": _digest(apply_payload),
        "tara_renderer_sha256": "4" * 64,
        "tara_resource_template_sha256": "5" * 64,
        "site_policy_sha256": _digest(site_payload),
        "python": str(external_python),
        "python_sha256": _digest(external_python.read_bytes()),
    }
    identity_path = runtime / "FIRST_CONTROL_RUNTIME_IDENTITIES"

    def publish(overrides: dict[str, str] | None = None) -> None:
        current = {**values, **(overrides or {})}
        payload = "".join(f"{name}={value}\n" for name, value in current.items())
        runtime.chmod(0o750)
        if identity_path.exists():
            identity_path.chmod(0o640)
        _write(identity_path, payload.encode("ascii"), 0o440)
        runtime.chmod(0o550)

    publish()
    _make_tree_readonly(runtime)
    monkeypatch.setattr(sys, "prefix", str(runtime))
    monkeypatch.setattr(runtime_identity_module, "__file__", str(source))
    try:
        yield {
            "runtime": runtime,
            "source": source,
            "identity": identity_path,
            "values": values,
            "publish": publish,
            "external_python": external_python,
        }
    finally:
        for directory in [
            runtime,
            *(path for path in runtime.rglob("*") if path.is_dir()),
        ]:
            directory.chmod(0o750)


def test_runtime_identity_matches_apply_guard_contract(
    frozen_runtime: dict[str, Any],
) -> None:
    gateway = load_runtime_identity("gateway")
    controller = load_runtime_identity("controller")

    expected = {
        "schema_version": 1,
        "runtime_kind": "sophia-firstv2-control-plane",
        "runtime_status": "published",
        "first_commit": "1" * 40,
        "deployment_commit": "2" * 40,
        "runtime_manifest_sha256": frozen_runtime["values"]["runtime_manifest_sha256"],
    }
    assert gateway.model_dump() == {**expected, "role": "gateway"}
    assert controller.model_dump() == {**expected, "role": "controller"}


def test_runtime_identity_fails_closed_when_absent(
    frozen_runtime: dict[str, Any],
) -> None:
    runtime = frozen_runtime["runtime"]
    runtime.chmod(0o750)
    frozen_runtime["identity"].unlink()
    runtime.chmod(0o550)
    with pytest.raises(RuntimeIdentityError, match="is absent"):
        load_runtime_identity("gateway")


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"runtime_status": "candidate"}, "values differ"),
        ({"first_commit": "not-a-commit"}, "values differ"),
        ({"python_sha256": "not-a-digest"}, "Python target identity differs"),
        (
            {
                "controller_runtime_identity_url": (
                    "http://controller:9100/runtime-identity"
                )
            },
            "not fixed loopback",
        ),
    ],
)
def test_runtime_identity_rejects_malformed_or_mismatched_values(
    frozen_runtime: dict[str, Any], overrides: dict[str, str], message: str
) -> None:
    frozen_runtime["publish"](overrides)
    with pytest.raises(RuntimeIdentityError, match=message):
        load_runtime_identity("gateway")


def test_runtime_identity_revalidates_after_first_success(
    frozen_runtime: dict[str, Any],
) -> None:
    load_runtime_identity("controller")
    source = frozen_runtime["source"]
    source.chmod(0o640)
    source.write_text("# modified after first proof\n")
    source.chmod(0o440)
    with pytest.raises(RuntimeIdentityError, match="manifest digest differs"):
        load_runtime_identity("controller")


def test_runtime_identity_rejects_extra_regular_file(
    frozen_runtime: dict[str, Any],
) -> None:
    runtime = frozen_runtime["runtime"]
    runtime.chmod(0o750)
    _write(runtime / "unexpected", b"mutable extra\n", 0o440)
    runtime.chmod(0o550)
    with pytest.raises(RuntimeIdentityError, match="complete regular-file set"):
        load_runtime_identity("gateway")


def test_runtime_identity_requires_exact_empty_publication_marker(
    frozen_runtime: dict[str, Any],
) -> None:
    marker = frozen_runtime["runtime"] / ".published"
    marker.chmod(0o640)
    marker.write_bytes(b"published\n")
    marker.chmod(0o440)
    with pytest.raises(RuntimeIdentityError, match="marker content differs"):
        load_runtime_identity("gateway")


def test_runtime_identity_rejects_wrong_mode_and_hardlink(
    frozen_runtime: dict[str, Any],
) -> None:
    source = frozen_runtime["source"]
    source.chmod(0o640)
    with pytest.raises(RuntimeIdentityError, match="file identity differs"):
        load_runtime_identity("gateway")
    source.chmod(0o440)

    source.parent.chmod(0o750)
    os.link(source, source.parent / "hardlink.py")
    source.parent.chmod(0o550)
    with pytest.raises(RuntimeIdentityError, match="file identity differs"):
        load_runtime_identity("gateway")


def test_runtime_identity_rejects_wrong_owner_group(
    frozen_runtime: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        runtime_identity_module,
        "_service_ids",
        lambda: (os.geteuid(), os.getegid() + 1),
    )
    with pytest.raises(RuntimeIdentityError, match="directory identity differs"):
        load_runtime_identity("gateway")


def test_runtime_identity_rejects_special_entry_and_symlink_change(
    frozen_runtime: dict[str, Any],
) -> None:
    runtime = frozen_runtime["runtime"]
    runtime.chmod(0o750)
    os.mkfifo(runtime / "unexpected-fifo")
    runtime.chmod(0o550)
    with pytest.raises(RuntimeIdentityError, match="special filesystem entry"):
        load_runtime_identity("gateway")

    runtime.chmod(0o750)
    (runtime / "unexpected-fifo").unlink()
    (runtime / "bin").chmod(0o750)
    launcher = runtime / "bin/python"
    launcher.unlink()
    launcher.symlink_to("/bin/sh")
    (runtime / "bin").chmod(0o550)
    runtime.chmod(0o550)
    with pytest.raises(RuntimeIdentityError, match="symlink inventory differs"):
        load_runtime_identity("gateway")


def test_runtime_identity_rejects_hardlinked_symlink(
    frozen_runtime: dict[str, Any],
) -> None:
    runtime = frozen_runtime["runtime"]
    bin_dir = runtime / "bin"
    launcher = bin_dir / "python"
    bin_dir.chmod(0o750)
    os.link(launcher, bin_dir / "python-hardlink", follow_symlinks=False)
    bin_dir.chmod(0o550)
    with pytest.raises(RuntimeIdentityError, match="symlink identity differs"):
        load_runtime_identity("gateway")


def test_gateway_runtime_identity_route_requires_admin() -> None:
    route = next(
        route
        for route in control.admin_router.routes
        if isinstance(route, APIRoute) and route.path == "/control/v1/runtime-identity"
    )
    assert any(
        dependency.call is get_admin_user for dependency in route.dependant.dependencies
    )


async def test_gateway_runtime_identity_response_is_exact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = RuntimeIdentity(
        schema_version=1,
        role="gateway",
        runtime_kind="sophia-firstv2-control-plane",
        runtime_status="published",
        first_commit="1" * 40,
        deployment_commit="2" * 40,
        runtime_manifest_sha256="3" * 64,
    )
    monkeypatch.setattr(control, "load_runtime_identity", lambda _role: expected)
    user = UserAuthEvent(
        id="11111111-1111-4111-8111-111111111111",
        name="Admin",
        username="admin@alcf.anl.gov",
        user_group_uuids=["22222222-2222-4222-8222-222222222222"],
        idp_id="33333333-3333-4333-8333-333333333333",
        idp_name="ALCF",
        auth_service="globus",
    )
    assert await control.runtime_identity(user) == expected


async def test_controller_runtime_identity_is_loopback_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = RuntimeIdentity(
        schema_version=1,
        role="controller",
        runtime_kind="sophia-firstv2-control-plane",
        runtime_status="published",
        first_commit="1" * 40,
        deployment_commit="2" * 40,
        runtime_manifest_sha256="3" * 64,
    )
    monkeypatch.setattr(metrics_server, "load_runtime_identity", lambda _role: expected)

    loopback = httpx.ASGITransport(app=metrics_server.app, client=("127.0.0.1", 12345))
    async with httpx.AsyncClient(
        transport=loopback, base_url="http://controller"
    ) as client:
        response = await client.get("/runtime-identity")
    assert response.status_code == 200
    assert response.json() == expected.model_dump(mode="json")

    remote = httpx.ASGITransport(app=metrics_server.app, client=("10.1.2.3", 12345))
    async with httpx.AsyncClient(
        transport=remote, base_url="http://controller"
    ) as client:
        response = await client.get("/runtime-identity")
    assert response.status_code == 404
