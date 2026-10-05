"""
End-to-end integration of PilotSubmitter against a real first-pilot
subprocess (real NGINX, real mTLS) driven through the LocalSchedulerAdapter
fixture.

Skipped automatically when nginx is not on PATH. Skipped on Windows since
the pilot uses POSIX process groups + SIGTERM.
"""

from __future__ import annotations

import asyncio
import socket
import sys
import time
from collections.abc import AsyncIterator, Iterator
from datetime import datetime, timezone
from pathlib import Path
from shutil import which
from tempfile import TemporaryDirectory

import httpx
import pytest
import yaml

from first_common.schema.base_scheduler import SchedulerJobState
from first_common.schema.pilot import (
    AddressInfo,
    PilotClientRole,
    PilotJobStatus,
    PilotResources,
)
from first_common.schema.resources.read import PilotJob
from first_common.schema.types import (
    HealthCheckParams,
    HealthCheckResult,
    PilotConfig,
    ReplicaState,
    ResolvedLaunchSpec,
)
from first_gateway.services.pilot_submitter import PilotSubmitter
from tests.fixtures.local_scheduler import (
    LocalSchedulerAdapter,
    make_mock_pilot_env,
)
from tests.fixtures.pki import PilotPKI

pytestmark = [
    pytest.mark.skipif(which("nginx") is None, reason="nginx not installed"),
    pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only"),
]


# A bash one-liner that becomes the replica process: a stdlib HTTP server
# bound to a Unix domain socket, answering 200 on GET /health and /metrics and
# on any POST. Every response carries X-Mock-Replica so tests can tell that a
# request reached the replica rather than being answered by NGINX. Rendered by
# Replica._render_script as Jinja (only `runtime.uds` is interpolated here).
_MOCK_REPLICA_SCRIPT = """\
#!/bin/bash
exec python -c '
import http.server, socket
class H(http.server.BaseHTTPRequestHandler):
    def reply(self, code):
        self.send_response(code)
        self.send_header("X-Mock-Replica", "1")
        self.send_header("Content-Length", "0")
        self.end_headers()
    def do_GET(self):
        self.reply(200 if self.path in ("/health", "/metrics") else 404)
    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        self.reply(200)
    def log_message(self, *a, **kw): pass
class S(http.server.HTTPServer):
    address_family = socket.AF_UNIX
S("{{ runtime.uds }}", H).serve_forever()
'
"""


def _free_port_window(n: int = 2) -> int:
    """
    Find a base port P such that P, P+1, ..., P+n-1 are all bindable on
    127.0.0.1. The pilot uses external_port for nginx and +1 for the internal
    control API (replicas now listen on Unix domain sockets, not ports).
    Picking only the first slot is racy, so we reserve the whole window.
    """
    for _ in range(100):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s0:
            s0.bind(("127.0.0.1", 0))
            base = int(s0.getsockname()[1])
        held: list[socket.socket] = []
        try:
            for off in range(1, n):
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.bind(("127.0.0.1", base + off))
                held.append(s)
            return base
        except OSError:
            continue
        finally:
            for s in held:
                s.close()
    raise RuntimeError(f"could not find {n} contiguous free ports")


@pytest.fixture
def workdir(request: pytest.FixtureRequest) -> Iterator[Path]:
    with TemporaryDirectory(prefix="pilot-it-") as td:
        path = Path(td)
        yield path
        if request.session.testsfailed:
            for log in sorted(path.rglob("*.log")):
                rel = log.relative_to(path)
                sys.stderr.write(f"\n--- {rel} ---\n{log.read_text()}\n")


@pytest.fixture
def pilot_config(workdir: Path, pilot_pki: PilotPKI) -> PilotConfig:
    external_port = _free_port_window(2)
    # The cluster's pre-staged runtime config: just the CA and server cert;
    # the submitter supplies everything job-specific as PILOT_* overrides.
    config_path = workdir / "pilot-config.yaml"
    config_path.write_text(yaml.safe_dump(pilot_pki.runtime_config()))
    return PilotConfig.model_validate(
        {
            "scheduler_adapter": (
                "first_gateway.platforms.schedulers."
                "globus_compute_pbs.GlobusComputePBSAdapter"
            ),
            "scheduler_config": {},
            "job_walltime_min": 60,
            "queue": "test",
            "account": "test",
            "max_num_nodes": 8,
            "gpus_per_node": 8,
            "scheduler_flags": "",
            "workdir": str(workdir),
            "external_port": external_port,
            "nginx_path": which("nginx"),
            "ip_allowlist": ["127.0.0.1/32"],
            "node_file_env": "TEST_PILOT_NODEFILE_UNSET",
            "submit_script_preamble": "#!/bin/bash\nset -eu",
            "pilot_path": "/test/first-pilot",
            "pilot_config_path": str(config_path),
        }
    )


@pytest.fixture
async def scheduler(workdir: Path) -> AsyncIterator[LocalSchedulerAdapter]:
    env = make_mock_pilot_env(workdir / "bin")
    adapter = LocalSchedulerAdapter(extra_env=env)
    try:
        yield adapter
    finally:
        adapter.close()


@pytest.fixture
def submitter(
    pilot_config: PilotConfig, scheduler: LocalSchedulerAdapter
) -> PilotSubmitter:
    return PilotSubmitter(pilot_config, scheduler)


def _make_pilot_job(name: str) -> PilotJob:
    return PilotJob(
        kind="PilotJob",
        name=name,
        uid=1,
        created_at=datetime.now(timezone.utc),
        scheduler_job_id="",
        cluster_name="testcluster",
        scheduler_state=SchedulerJobState.pending_submit,
        manager_url="",
        manager_health=HealthCheckResult.unknown,
        resources=PilotResources(hosts=[]),
        claimed_gpu_ids=[],
        assigned_replicas=[],
        walltime_min=60,
        num_nodes=1,
        gpus_per_node=2,
    )


def _control_client(pilot_pki: PilotPKI, base_url: str) -> httpx.AsyncClient:
    """An mTLS client holding the control identity, as the controller does."""
    ctx = pilot_pki.client_context(pilot_pki.clients[PilotClientRole.control])
    return httpx.AsyncClient(verify=ctx, base_url=base_url, timeout=10.0)


def _control_base_url(addr: AddressInfo) -> str:
    # nginx always binds loopback alongside the configured interfaces.
    return f"https://127.0.0.1:{addr.external_port}{addr.control_path.rstrip('/')}"


async def _submit_and_wait_ready(
    submitter: PilotSubmitter, scheduler: LocalSchedulerAdapter, name: str
) -> AddressInfo:
    result = await submitter.submit(_make_pilot_job(name))
    assert result.job_name.endswith(name)

    # Sees QUEUED before the pilot writes its readyfile, RUNNING after.
    statuses = await submitter.get_statuses()
    assert [s.state for s in statuses] == [SchedulerJobState.queued] or [
        s.state for s in statuses
    ] == [SchedulerJobState.running]

    async def _ready() -> bool:
        return name in await submitter.list_ready_endpoints()

    deadline = time.monotonic() + 15.0
    while time.monotonic() < deadline:
        if await _ready():
            break
        await asyncio.sleep(0.05)
    else:
        # Dump all logs while the subprocess (and its tmpdirs) are still alive.
        workdir = submitter.pilot_config.workdir
        for log in sorted(workdir.rglob("*.log")):
            rel = log.relative_to(workdir)
            sys.stderr.write(f"\n--- {rel} ---\n{log.read_text()}\n")
        raise AssertionError(f"pilot {name} never wrote its readyfile")

    statuses = await submitter.get_statuses()
    assert statuses[0].state == SchedulerJobState.running

    return await submitter.get_endpoint(name)


async def test_submit_brings_endpoint_online(
    submitter: PilotSubmitter,
    scheduler: LocalSchedulerAdapter,
    pilot_pki: PilotPKI,
) -> None:
    """PilotSubmitter.submit → real pilot → readyfile → gateway can mTLS in."""
    addr = await _submit_and_wait_ready(submitter, scheduler, "alpha")

    base_url = _control_base_url(addr)
    async with _control_client(pilot_pki, base_url) as client:
        resp = await client.get("/status")
        assert resp.status_code == 200
        status = PilotJobStatus.model_validate(resp.json())
        assert status.replicas == []
        assert any(g.name == "MockGPU" for h in status.resources.hosts for g in h.gpus)


async def _start_mock_replica(client: httpx.AsyncClient, name: str) -> None:
    """POST /start-replica for a mock replica and wait until it is ready."""
    start_req = {
        "name": name,
        "deployment_name": "depl",
        "launch_spec": ResolvedLaunchSpec(
            served_model_name="mock",
            gpus_per_node=1,
            num_nodes=1,
            max_model_len=None,
            env={},
            parameters={},
            serve_script_template=_MOCK_REPLICA_SCRIPT,
            pre_stop_script_template=None,
            post_stop_script_template=None,
            max_startup_sec=20,
            max_unhealthy_sec=None,
            pre_stop_timeout_sec=20.0,
            post_stop_timeout_sec=50.0,
            health_check=HealthCheckParams(url="http://localhost/health"),
        ).model_dump(mode="json"),
        "gpu_indices": [(0, 0)],
    }
    r = await client.post("/start-replica", json=start_req)
    assert r.status_code == 200, r.text

    async def _ready() -> bool:
        try:
            s = PilotJobStatus.model_validate((await client.get("/status")).json())
        except httpx.TransportError:
            return False
        return any(
            rep.name == name and rep.state == ReplicaState.ready for rep in s.replicas
        )

    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if await _ready():
            return
        await asyncio.sleep(0.05)
    logs = (await client.get(f"/logs/{name}")).text
    raise AssertionError(f"replica {name} never became ready; logs:\n{logs}")


async def test_replica_lifecycle(
    submitter: PilotSubmitter,
    scheduler: LocalSchedulerAdapter,
    pilot_pki: PilotPKI,
) -> None:
    """start_replica → poll until ready → logs → stop_replica."""
    addr = await _submit_and_wait_ready(submitter, scheduler, "beta")

    base_url = _control_base_url(addr)
    async with _control_client(pilot_pki, base_url) as client:
        await _start_mock_replica(client, "r0")

        logs_resp = await client.get("/logs/r0")
        assert logs_resp.status_code == 200
        assert isinstance(logs_resp.json(), str)

        r = await client.get("/status")
        assert r.status_code == 200, r.text
        assert "Replica r0 ready" in r.json()["replicas"][0]["state_message"]

        stop = await client.post("/stop-replica/r0")
        assert stop.status_code == 200

        s_after = PilotJobStatus.model_validate((await client.get("/status")).json())
        assert all(rep.name != "r0" for rep in s_after.replicas)


# Where a request ended up. Each "allowed" outcome is identified positively by
# its upstream, so a request cannot pass as allowed just because NGINX failed
# it with some status other than 403.
DENIED = "denied: NGINX 403"
CONTROL_API = "reached the pilot control API"
REPLICA = "reached the replica"


def _outcome(resp: httpx.Response) -> str:
    if resp.headers.get("x-mock-replica") == "1":
        return REPLICA
    if resp.headers.get("content-type", "").startswith("application/json"):
        return CONTROL_API  # NGINX's own responses are HTML
    if resp.status_code == 403:
        return DENIED
    return f"unexpected: {resp.status_code} {resp.text[:200]!r}"


# (caller, method, raw request target, expected outcome). Targets are sent
# byte-for-byte as written, so the dot-segment, double-slash and %2e cases
# reach NGINX un-normalized and exercise its $uri normalization.
_AUTHZ_MATRIX = [
    ("control", "GET", "/control/status", CONTROL_API),
    ("control", "POST", "/control/start-replica", CONTROL_API),
    ("control", "POST", "/control/stop-replica/nonexistent", CONTROL_API),
    ("control", "GET", "/control/logs/r0", CONTROL_API),
    ("control", "POST", "/replicas/r0/v1/chat/completions", REPLICA),
    ("control", "GET", "/replicas/r0/metrics", REPLICA),
    # The bypass targets below do route to the control API once normalized:
    ("control", "POST", "/replicas/x/../../control/start-replica", CONTROL_API),
    ("control", "POST", "//control/start-replica", CONTROL_API),
    ("control", "POST", "/replicas/%2e%2e/control/start-replica", CONTROL_API),
    ("router", "POST", "/replicas/r0/v1/chat/completions", REPLICA),
    ("router", "GET", "/replicas/r0/metrics", REPLICA),
    ("router", "GET", "/control/logs/r0", CONTROL_API),
    ("router", "POST", "/control/start-replica", DENIED),
    ("router", "POST", "/control/stop-replica/r0", DENIED),
    ("router", "GET", "/control/status", DENIED),
    ("router", "POST", "/control/logs/r0", DENIED),
    ("router", "POST", "/replicas/x/../../control/start-replica", DENIED),
    ("router", "POST", "//control/start-replica", DENIED),
    ("router", "POST", "/replicas/%2e%2e/control/start-replica", DENIED),
    ("router", "GET", "/replicas/%2e%2e/control/status", DENIED),
    ("metrics", "GET", "/replicas/r0/metrics", REPLICA),
    ("metrics", "POST", "/replicas/r0/v1/chat/completions", DENIED),
    ("metrics", "GET", "/replicas/r0/health", DENIED),
    ("metrics", "GET", "/control/status", DENIED),
    ("metrics", "GET", "/control/logs/r0", DENIED),
    ("metrics", "POST", "/control/start-replica", DENIED),
    ("metrics", "GET", "/replicas/x/../../control/metrics", DENIED),
    ("unknown", "GET", "/control/status", DENIED),
    ("unknown", "GET", "/control/logs/r0", DENIED),
    ("unknown", "POST", "/replicas/r0/v1/chat/completions", DENIED),
    ("unknown", "GET", "/replicas/r0/metrics", DENIED),
]


async def test_authorization_matrix(
    submitter: PilotSubmitter,
    scheduler: LocalSchedulerAdapter,
    pilot_pki: PilotPKI,
    subtests: pytest.Subtests,
) -> None:
    """
    Pilot NGINX authorizes each request by client certificate subject.

    One pilot and replica serve the whole matrix; each row is a subtest.
    """
    addr = await _submit_and_wait_ready(submitter, scheduler, "gamma")
    async with _control_client(pilot_pki, _control_base_url(addr)) as control:
        await _start_mock_replica(control, "r0")

    base_url = f"https://127.0.0.1:{addr.external_port}"
    identities = {role.name: pilot_pki.clients[role] for role in PilotClientRole}
    identities["unknown"] = pilot_pki.unknown_client
    clients = {
        caller: httpx.AsyncClient(
            verify=pilot_pki.client_context(identity), base_url=base_url, timeout=10
        )
        for caller, identity in identities.items()
    }
    assert set(clients) == {caller for caller, *_ in _AUTHZ_MATRIX}

    async def send(caller: str, method: str, target: str) -> httpx.Response:
        # httpx would normalize "..", "//" and friends in a URL; the "target"
        # extension puts `target` on the request line verbatim.
        return await clients[caller].request(
            method,
            "/",
            json={} if method == "POST" else None,
            extensions={"target": target.encode()},
        )

    try:
        # Canary: NGINX rejects a target that climbs above the root with 400.
        # A 200 here would mean the client normalized the path before sending,
        # so the bypass rows below would prove nothing.
        canary = await send("control", "GET", "/../control/status")
        assert canary.status_code == 400, canary.text

        for caller, method, target, expected in _AUTHZ_MATRIX:
            with subtests.test(f"{caller} {method} {target}"):
                assert _outcome(await send(caller, method, target)) == expected
    finally:
        for client in clients.values():
            await client.aclose()

    with subtests.test("server certificate presented as a client certificate"):
        ctx = pilot_pki.client_context(pilot_pki.server)
        async with httpx.AsyncClient(verify=ctx, base_url=base_url) as client:
            resp = await client.get("/control/status")
        # NGINX completes the handshake even when client verification fails
        # (here: serverAuth-only EKU) and then rejects every request with 400.
        assert resp.status_code == 400
        assert "The SSL certificate error" in resp.text
