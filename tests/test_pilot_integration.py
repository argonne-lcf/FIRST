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
import ssl
import sys
import time
from collections.abc import AsyncIterator, Iterator
from datetime import datetime, timezone
from pathlib import Path
from shutil import which
from tempfile import TemporaryDirectory

import httpx
import pytest

from first_common.schema.base_scheduler import JobPhase
from first_common.schema.pilot import AddressInfo, PilotJobStatus, PilotResources
from first_common.schema.resources.read import PilotJob
from first_common.schema.types import (
    GpuClaim,
    HealthEndpointStatus,
    PilotConfig,
    PilotLaunchSpec,
    ReplicaPhase,
)
from first_gateway.services.certmanager import gen_ca_pem, generate_client_cert
from first_gateway.services.pilot_submitter import PilotSubmitter
from tests.fixtures.local_scheduler import (
    LocalSchedulerAdapter,
    make_mock_pilot_env,
)

pytestmark = [
    pytest.mark.skipif(which("nginx") is None, reason="nginx not installed"),
    pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only"),
]


# A bash one-liner that becomes the replica process: a stdlib HTTP server
# answering 200 on /health. Rendered by Replica._render_script as Jinja
# (only `{{port}}` is interpolated here).
_MOCK_REPLICA_SCRIPT = """\
#!/bin/bash
exec python -c '
import http.server, sys
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        code = 200 if self.path == "/health" else 404
        self.send_response(code); self.end_headers()
    def log_message(self, *a, **kw): pass
http.server.HTTPServer(("127.0.0.1", {{port}}), H).serve_forever()
'
"""


def _free_port_window(n: int = 3) -> int:
    """
    Find a base port P such that P, P+1, ..., P+n-1 are all bindable on
    127.0.0.1. The pilot uses external_port for nginx, +1 for the internal
    control API, and +2 for the first replica — picking only the first slot
    is racy and made test_replica_lifecycle flake with EADDRINUSE on +2.
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
def ca_pair() -> tuple[str, str]:
    return gen_ca_pem(name="test-ca")


@pytest.fixture
def pilot_config(workdir: Path) -> PilotConfig:
    external_port = _free_port_window(3)
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
            "scheduler_flags": "",
            "workdir": str(workdir),
            "external_port": external_port,
            "nginx_path": which("nginx"),
            "ip_allowlist": ["127.0.0.1/32"],
            "node_file_env": "TEST_PILOT_NODEFILE_UNSET",
            "submit_script_preamble": "#!/bin/bash\nset -eu",
            "pilot_version": "0.0.0-test",
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
    pilot_config: PilotConfig,
    scheduler: LocalSchedulerAdapter,
    ca_pair: tuple[str, str],
) -> PilotSubmitter:
    ca_crt, ca_key = ca_pair
    return PilotSubmitter(pilot_config, scheduler, ca_crt, ca_key)


@pytest.fixture
def gateway_client_cert(ca_pair: tuple[str, str], workdir: Path) -> tuple[Path, Path]:
    """Issue gateway-side mTLS client cert + key; return (cert_path, key_path)."""
    ca_crt, ca_key = ca_pair
    crt, key = generate_client_cert(
        cn="first_gateway-test", ca_cert_pem=ca_crt, ca_key_pem=ca_key
    )
    crt_path = workdir / "client.crt"
    key_path = workdir / "client.key"
    crt_path.write_text(crt)
    key_path.write_text(key)
    return crt_path, key_path


def _make_pilot_job(name: str) -> PilotJob:
    return PilotJob(
        kind="PilotJob",
        name=name,
        uid=1,
        created_at=datetime.now(timezone.utc),
        scheduler_job_id="",
        cluster_name="testcluster",
        phase=JobPhase.pending_submit,
        manager_url="",
        manager_health=HealthEndpointStatus.unknown,
        resources=PilotResources(hosts=[]),
        assigned_replicas=[],
        walltime_min=60,
        num_nodes=1,
        gpus_per_node=2,
    )


def _build_mtls_client(
    ca_pair: tuple[str, str],
    gateway_client_cert: tuple[Path, Path],
    workdir: Path,
    base_url: str,
) -> httpx.AsyncClient:
    ca_path = workdir / "ca.crt"
    ca_path.write_text(ca_pair[0])

    ctx = ssl.create_default_context(cafile=str(ca_path))
    # The pilot server cert CN is the pilot job name (e.g. "alpha"), and we
    # connect to 127.0.0.1 — chain verification still happens, but hostname
    # match is intentionally relaxed for the test loopback.
    ctx.check_hostname = False
    crt_path, key_path = gateway_client_cert
    ctx.load_cert_chain(str(crt_path), str(key_path))

    return httpx.AsyncClient(verify=ctx, base_url=base_url, timeout=10.0)


def _control_base_url(addr: AddressInfo) -> str:
    # The advertised IP may be the host's externally-routable interface, but
    # nginx binds 0.0.0.0; 127.0.0.1 is always reachable in tests.
    return f"https://127.0.0.1:{addr.external_port}{addr.control_path.rstrip('/')}"


async def _submit_and_wait_ready(
    submitter: PilotSubmitter, scheduler: LocalSchedulerAdapter, name: str
) -> AddressInfo:
    result = await submitter.submit(_make_pilot_job(name))
    assert result.job_name.endswith(name)

    # Sees QUEUED before the pilot writes its readyfile, RUNNING after.
    statuses = await submitter.get_statuses()
    assert [s.state for s in statuses] == [JobPhase.queued] or [
        s.state for s in statuses
    ] == [JobPhase.running]

    async def _ready() -> bool:
        return name in await submitter.list_ready_endpoints()

    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        if await _ready():
            break
        await asyncio.sleep(0.05)
    else:
        raise AssertionError(f"pilot {name} never wrote its readyfile")

    statuses = await submitter.get_statuses()
    assert statuses[0].state == JobPhase.running

    return await submitter.get_endpoint(name)


async def test_submit_brings_endpoint_online(
    submitter: PilotSubmitter,
    scheduler: LocalSchedulerAdapter,
    ca_pair: tuple[str, str],
    gateway_client_cert: tuple[Path, Path],
    workdir: Path,
) -> None:
    """PilotSubmitter.submit → real pilot → readyfile → gateway can mTLS in."""
    addr = await _submit_and_wait_ready(submitter, scheduler, "alpha")

    base_url = _control_base_url(addr)
    async with _build_mtls_client(
        ca_pair, gateway_client_cert, workdir, base_url
    ) as client:
        resp = await client.get("/status")
        assert resp.status_code == 200
        status = PilotJobStatus.model_validate(resp.json())
        assert status.replicas == []
        assert any(g.name == "MockGPU" for h in status.resources.hosts for g in h.gpus)


async def test_replica_lifecycle(
    submitter: PilotSubmitter,
    scheduler: LocalSchedulerAdapter,
    ca_pair: tuple[str, str],
    gateway_client_cert: tuple[Path, Path],
    workdir: Path,
) -> None:
    """start_replica → poll until ready → logs → stop_replica."""
    addr = await _submit_and_wait_ready(submitter, scheduler, "beta")

    base_url = _control_base_url(addr)
    async with _build_mtls_client(
        ca_pair, gateway_client_cert, workdir, base_url
    ) as client:
        # Pick the first GPU advertised by query_resources().
        status0 = PilotJobStatus.model_validate((await client.get("/status")).json())
        host0 = status0.resources.hosts[0]
        start_req = {
            "name": "r0",
            "deployment_name": "depl",
            "launch_spec": PilotLaunchSpec(
                served_model_name="mock",
                gpus_per_node=1,
                num_nodes=1,
                venv_path=Path("/unused"),
                weights_path=Path("/unused"),
                weights_cache_path=Path("/unused"),
                env={},
                serve_script_template=_MOCK_REPLICA_SCRIPT,
                max_startup_sec=20,
                health_path="/health",
            ).model_dump(mode="json"),
            "resources": [
                GpuClaim(
                    hostname=host0.hostname, gpu_ids=[host0.gpus[0].index]
                ).model_dump(mode="json")
            ],
        }
        r = await client.post("/start-replica", json=start_req)
        assert r.status_code == 200, r.text

        async def _ready() -> bool:
            s = PilotJobStatus.model_validate((await client.get("/status")).json())
            return any(
                rep.name == "r0" and rep.phase == ReplicaPhase.ready
                for rep in s.replicas
            )

        deadline = time.monotonic() + 20.0
        while time.monotonic() < deadline:
            if await _ready():
                break
            await asyncio.sleep(0.05)
        else:
            logs = (await client.get("/logs/r0")).text
            raise AssertionError(f"replica r0 never became ready; logs:\n{logs}")

        logs_resp = await client.get("/logs/r0")
        assert logs_resp.status_code == 200
        assert isinstance(logs_resp.json(), str)

        r = await client.get("/status")
        assert r.status_code == 200, r.text
        assert "Replica r0 ready" in r.json()["replicas"][0]["status_info"]

        stop = await client.post("/stop-replica/r0")
        assert stop.status_code == 200

        s_after = PilotJobStatus.model_validate((await client.get("/status")).json())
        assert all(rep.name != "r0" for rep in s_after.replicas)
