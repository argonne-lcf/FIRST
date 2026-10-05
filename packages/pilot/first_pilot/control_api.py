import logging
import socket
from contextlib import asynccontextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated, AsyncGenerator, cast

import psutil
import uvicorn
from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse

from first_common.errors import FirstError
from first_common.schema.pilot import (
    AddressInfo,
    PilotJobStatus,
    PilotRuntimeConfig,
    ReplicaInfo,
    ReplicaStartRequest,
)

from .nginx_manager import NginxManager, ReplicaUpstream, check_server_cert_expiry
from .replica_manager import ReplicaManager

logger = logging.getLogger(__name__)


def resolve_interface_ips(interfaces: list[str]) -> list[str]:
    """
    IPv4 address of each configured interface, in configured order. Interfaces
    that are missing, down, or lack an IPv4 address are skipped so a faulty NIC
    (e.g. hsn0) does not stop the pilot from serving on the others.
    """
    addrs = psutil.net_if_addrs()
    stats = psutil.net_if_stats()
    ips: list[str] = []
    for ifname in interfaces:
        ipv4 = [a.address for a in addrs.get(ifname, []) if a.family == socket.AF_INET]
        if ifname not in stats or not stats[ifname].isup or not ipv4:
            logger.warning("Skipping network interface %r: down or no IPv4", ifname)
            continue
        logger.info("Network interface %r has IP %s", ifname, ipv4[0])
        ips.append(ipv4[0])
    if not ips:
        raise RuntimeError(
            f"None of the configured network_interfaces {interfaces} has an "
            "IPv4 address and is up"
        )
    return ips


class _PilotManager:
    def __init__(self, config: PilotRuntimeConfig, nginx_tmpdir: Path) -> None:
        self.config = config
        check_server_cert_expiry(config.server_crt, config.walltime_min)
        interface_ips = resolve_interface_ips(config.network_interfaces)
        self.nginx = NginxManager(self.config, nginx_tmpdir, interface_ips)
        self.replica_manager = ReplicaManager(self.config)
        self._endpoint = AddressInfo(
            hostname=socket.gethostname(),
            ip=interface_ips[0],
            external_port=self.config.external_port,
            control_path=self.nginx.control_path,
        )
        logger.info("Advertising pilot endpoint %s", self._endpoint.base_url)

    def start(self, readyfile: Path) -> None:
        self.nginx.start()
        self.nginx.wait_until_healthy()
        logger.info("nginx healthy on port %d", self.config.external_port)
        readyfile.write_text(self._endpoint.model_dump_json())
        logger.info("readyfile written: %s", readyfile)

    def stop(self) -> None:
        self.nginx.stop()
        self.replica_manager.stop_all()

    def _reload_nginx(self) -> None:
        upstreams = [
            ReplicaUpstream(name=r.name, uds=r.uds)
            for r in self.replica_manager.get_replicas()
        ]
        try:
            self.nginx.reload(upstreams)
        except Exception:
            logger.exception("nginx reload failed")

    def _replica_url(self, name: str) -> str:
        return f"{self._endpoint.base_url}/replicas/{name}/"

    def start_replica(self, replica: ReplicaStartRequest) -> None:
        self.replica_manager.start_replica(replica)
        self._reload_nginx()

    def stop_replica(self, replica_name: str) -> None:
        self.replica_manager.stop_replica(replica_name)
        self._reload_nginx()

    def get_status(self) -> PilotJobStatus:
        replica_statuses = [
            ReplicaInfo(
                name=r.name,
                url=self._replica_url(r.name),
                state=r.state,
                started_at=r.started_at,
                state_message=r.state_message,
                served_model_name=r.launch_spec.served_model_name,
                resources=r.resources,
                log_path=r.log_path,
            )
            for r in self.replica_manager.get_replicas()
        ]
        return PilotJobStatus(
            resources=self.replica_manager.query_resources(),
            replicas=replica_statuses,
        )

    def get_replica_logs(self, replica_name: str) -> str:
        replica = self.replica_manager.get_replica(replica_name)
        return replica.get_logs()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    config: PilotRuntimeConfig = app.state.config
    config.ensure_dirs()
    readyfile = config.readyfile_dir / f"{config.job_name}.ready.json"

    logging.basicConfig(
        level="INFO",
        format="%(asctime)s %(levelname)-8s %(name)s:%(lineno)d %(message)s",
    )

    with TemporaryDirectory(
        dir=config.nginx_base_dir,
        prefix=f"pilot-{config.job_name}-",
        ignore_cleanup_errors=True,
    ) as nginx_tmpdir:
        manager = _PilotManager(config, Path(nginx_tmpdir))
        try:
            manager.start(readyfile)
            app.state.pilot_manager = manager
            yield
        finally:
            manager.stop()
            readyfile.unlink(missing_ok=True)


app = FastAPI(lifespan=lifespan)


async def get_manager(request: Request) -> _PilotManager:
    return cast(_PilotManager, request.app.state.pilot_manager)


PilotManager = Annotated[_PilotManager, Depends(get_manager)]


@app.post("/start-replica")
def start_replica(replica: ReplicaStartRequest, manager: PilotManager) -> None:
    manager.start_replica(replica)


@app.post("/stop-replica/{replica_name:path}")
def stop_replica(replica_name: str, manager: PilotManager) -> None:
    manager.stop_replica(replica_name)


@app.get("/status", response_model=PilotJobStatus)
def get_status(manager: PilotManager) -> PilotJobStatus:
    return manager.get_status()


@app.get("/logs/{replica_name:path}")
def get_replica_logs(replica_name: str, manager: PilotManager) -> str:
    return manager.get_replica_logs(replica_name)


@app.exception_handler(FirstError)
def handle_app_error(_request: Request, exc: FirstError) -> JSONResponse:
    return JSONResponse(
        {"error": {"code": exc.code, "message": str(exc), "info": exc.info}},
        status_code=exc.status_code,
    )


def entrypoint() -> None:
    config = PilotRuntimeConfig.load()
    app.state.config = config
    uvicorn.run(
        app,
        uds=config.control_uds_path.as_posix(),
        log_level="INFO",
    )
