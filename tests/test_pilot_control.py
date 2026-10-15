"""Tests for PilotControlClient's built-in timeout/retry behavior."""

import re
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import httpx
import pytest
from jinja2 import Template

from first_common.errors import ReplicaStartError, ReplicaTeardownError
from first_common.schema.pilot import PilotClientRole
from first_gateway.services.pilot_control import STOP_TIMEOUT, PilotControlClient
from first_pilot.control_api import _PilotManager, app, get_manager, lifespan
from first_pilot.nginx_manager import ReplicaUpstream, _conf_template_str


def test_pilot_control_proxy_outlives_stop_read_without_changing_data_plane() -> None:
    rendered = Template(_conf_template_str).render(
        config=SimpleNamespace(
            control_uds_path=Path("/tmp/control.sock"),
            external_port=8443,
            ip_allowlist=["127.0.0.1"],
        ),
        nginx_tmpdir="/tmp/nginx",
        control_path="/control/",
        roles=PilotClientRole,
        ca_crt_path="/tmp/ca.crt",
        server_crt_path="/tmp/server.crt",
        server_key_path="/tmp/server.key",
        access_log_path="/tmp/audit/alpha.access.log",
        audit_log_path="/tmp/audit/alpha.control-access.jsonl",
        replicas=[
            ReplicaUpstream("model", "/tmp/model.sock", ("/v1/chat/completions",))
        ],
    )
    control = rendered.split("location /control/ {", 1)[1].split("}", 1)[0]
    model = rendered.split("location = /replicas/model/v1/chat/completions {", 1)[
        1
    ].split("}", 1)[0]
    control_timeout = re.findall(r"proxy_read_timeout\s+(\d+)s;", control)
    assert control_timeout == ["185"]
    assert STOP_TIMEOUT.read is not None and 162 < STOP_TIMEOUT.read < 185
    assert re.findall(r"proxy_read_timeout\s+(\d+)s;", model) == ["920"]
    assert "proxy_pass http://control_api/;" in control


def test_control_requests_are_audited_with_the_authenticated_subject() -> None:
    rendered = Template(_conf_template_str).render(
        config=SimpleNamespace(
            control_uds_path=Path("/tmp/control.sock"),
            external_port=8443,
            ip_allowlist=["127.0.0.1"],
        ),
        nginx_tmpdir="/tmp/nginx",
        control_path="/control/",
        roles=PilotClientRole,
        ca_crt_path="/tmp/ca.crt",
        server_crt_path="/tmp/server.crt",
        server_key_path="/tmp/server.key",
        access_log_path="/tmp/audit/alpha.access.log",
        audit_log_path="/tmp/audit/alpha.control-access.jsonl",
        replicas=[],
    )
    control = rendered.split("location /control/ {", 1)[1].split("}", 1)[0]

    assert "log_format control_audit escape=json" in rendered
    assert "$ssl_client_s_dn" in rendered
    assert "$ssl_client_verify" in rendered
    # Declared at http level, ahead of the server block's default-deny, so a
    # request rejected before reaching the location is still audited.
    http_level = rendered.split("server {", 1)[0]
    assert (
        "access_log /tmp/audit/alpha.control-access.jsonl control_audit "
        "buffer=64k flush=5s if=$audit_control;"
    ) in http_level
    # The full access log is kept next to it rather than in the job tmpdir.
    assert (
        "access_log /tmp/audit/alpha.access.log combined buffer=64k flush=5s;"
    ) in http_level
    assert '"~^/control/" 1;' in http_level
    assert "access_log" not in control


def _make_client(handler: object) -> PilotControlClient:
    client = PilotControlClient.__new__(PilotControlClient)
    client._client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler)  # type: ignore[arg-type]
    )
    return client


async def test_retries_transient_transport_error_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Skip the backoff sleeps to keep the test fast.
    async def _no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr("first_gateway.services.pilot_control.asyncio.sleep", _no_sleep)

    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectError("boom", request=request)
        return httpx.Response(200)

    client = _make_client(handler)
    resp = await client._request("POST", "https://mgr/stop-replica/r0")

    assert resp.status_code == 200
    assert calls["n"] == 2


async def test_retries_transient_5xx_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr("first_gateway.services.pilot_control.asyncio.sleep", _no_sleep)

    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(503)
        return httpx.Response(200)

    client = _make_client(handler)
    resp = await client._request("GET", "https://mgr/status")

    assert resp.status_code == 200
    assert calls["n"] == 3


async def test_gives_up_and_raises_after_persistent_transport_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr("first_gateway.services.pilot_control.asyncio.sleep", _no_sleep)

    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        raise httpx.ConnectError("down", request=request)

    client = _make_client(handler)
    with pytest.raises(httpx.ConnectError):
        await client._request("GET", "https://mgr/status")

    assert calls["n"] == 3


async def test_4xx_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr("first_gateway.services.pilot_control.asyncio.sleep", _no_sleep)

    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(409)

    client = _make_client(handler)
    resp = await client._request("POST", "https://mgr/start-replica")

    assert resp.status_code == 409
    assert calls["n"] == 1


async def test_pilot_api_returns_structured_teardown_error() -> None:
    manager = MagicMock()
    manager.stop_replica.side_effect = ReplicaTeardownError(
        "Replica r0 teardown incomplete; still present: model process group"
    )
    app.dependency_overrides[get_manager] = lambda: manager
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://pilot",
        ) as client:
            response = await client.post("/stop-replica/r0")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 400
    assert response.json() == {
        "error": {
            "code": "replica_teardown_incomplete",
            "message": (
                "Replica r0 teardown incomplete; still present: model process group"
            ),
            "info": {},
        }
    }


def _pilot_manager(
    replicas: list[SimpleNamespace],
) -> tuple[_PilotManager, MagicMock, MagicMock]:
    """A _PilotManager over mocked NGINX and replica managers."""
    nginx = MagicMock()
    replica_manager = MagicMock()
    replica_manager.get_replicas.return_value = replicas
    manager = _PilotManager.__new__(_PilotManager)
    manager.nginx = nginx
    manager.replica_manager = replica_manager
    manager._nginx_lock = threading.Lock()
    return manager, nginx, replica_manager


def _fake_replica(name: str) -> SimpleNamespace:
    return SimpleNamespace(name=name, uds=f"/tmp/{name}.sock", proxy_paths=("/x",))


def test_start_replica_rolls_back_when_nginx_reload_fails() -> None:
    manager, nginx, replica_manager = _pilot_manager([_fake_replica("r0")])
    nginx.reload.side_effect = RuntimeError("config test failed")

    with pytest.raises(ReplicaStartError, match="config test failed") as exc_info:
        manager.start_replica(SimpleNamespace(name="r0"))  # type: ignore[arg-type]

    assert "rollback incomplete" not in str(exc_info.value)
    replica_manager.stop_replica.assert_called_once_with("r0")


def test_start_replica_reports_incomplete_rollback_as_start_error() -> None:
    manager, nginx, replica_manager = _pilot_manager([_fake_replica("r0")])
    nginx.reload.side_effect = RuntimeError("config test failed")
    replica_manager.stop_replica.side_effect = ReplicaTeardownError("group survived")

    with pytest.raises(
        ReplicaStartError, match="rollback incomplete: group survived"
    ) as exc_info:
        manager.start_replica(SimpleNamespace(name="r0"))  # type: ignore[arg-type]

    # Never 409: the launcher would read that as "already placed".
    assert exc_info.value.status_code == 400


def test_stop_replica_tolerates_nginx_reload_failure() -> None:
    manager, nginx, replica_manager = _pilot_manager([])
    nginx.reload.side_effect = RuntimeError("config test failed")

    manager.stop_replica("r0")

    replica_manager.stop_replica.assert_called_once_with("r0")


def test_concurrent_reloads_apply_snapshots_in_order() -> None:
    """A reload must not apply a replica set older than one already applied."""
    replicas = [_fake_replica("a")]
    manager, nginx, replica_manager = _pilot_manager(replicas)
    applied: list[list[str]] = []
    first_in_reload = threading.Event()
    release_first = threading.Event()

    def reload(upstreams: list[ReplicaUpstream]) -> None:
        if not first_in_reload.is_set():
            first_in_reload.set()
            release_first.wait(timeout=5)
        applied.append([u.name for u in upstreams])

    nginx.reload.side_effect = reload

    first = threading.Thread(target=manager._reload_nginx)
    first.start()
    assert first_in_reload.wait(timeout=5)

    replicas.append(_fake_replica("b"))
    second = threading.Thread(target=manager._reload_nginx)
    second.start()
    # The second reload must wait for the first rather than race past it.
    second.join(timeout=0.2)
    assert second.is_alive()

    release_first.set()
    first.join(timeout=5)
    second.join(timeout=5)

    assert applied == [["a"], ["a", "b"]]


async def test_readyfile_is_removed_when_shutdown_fails(tmp_path: Path) -> None:
    readyfile = tmp_path / "alpha.ready.json"
    manager = MagicMock()
    manager.start.side_effect = lambda path: path.write_text("{}")
    manager.stop.side_effect = RuntimeError("replica did not stop")
    test_app = SimpleNamespace(
        state=SimpleNamespace(
            config=SimpleNamespace(
                ensure_dirs=lambda: None,
                readyfile_dir=tmp_path,
                job_name="alpha",
                nginx_base_dir=tmp_path,
            )
        )
    )

    with (
        patch("first_pilot.control_api._PilotManager", return_value=manager),
        patch("first_pilot.control_api.admit_nodes"),
    ):
        with pytest.raises(RuntimeError, match="replica did not stop"):
            async with lifespan(test_app):  # type: ignore[arg-type]
                assert readyfile.exists()

    assert not readyfile.exists()
