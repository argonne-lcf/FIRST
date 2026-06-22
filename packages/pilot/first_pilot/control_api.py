import logging
import socket
from contextlib import asynccontextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated, AsyncGenerator, cast

import uvicorn
from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse

from first_common.errors import FirstError
from first_common.schema.pilot import (
    AddressInfo,
    PilotJobStatus,
    ReplicaInfo,
    ReplicaLogTail,
    ReplicaStartRequest,
)

from .config import Config
from .nginx_manager import NginxManager, ReplicaPort
from .replica_manager import ReplicaManager


class _PilotManager:
    def __init__(self, config: Config, nginx_tmpdir: Path) -> None:
        self.config = config
        self.nginx = NginxManager(self.config, nginx_tmpdir)
        self.replica_manager = ReplicaManager(self.config)
        self._endpoint = self.discover_service_endpoint()

    def start(self, readyfile: Path) -> None:
        self.nginx.start()
        self.nginx.wait_until_healthy()
        readyfile.write_text(self._endpoint.model_dump_json())

    def stop(self) -> None:
        self.nginx.stop()
        self.replica_manager.stop_all()

    def discover_service_endpoint(self) -> AddressInfo:
        # UDP "connect" to a public IP — no traffic is sent, but the OS
        # picks the interface it *would* route through, giving us the
        # externally-reachable source address.
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]

        fqdn = socket.getfqdn(ip)
        return AddressInfo(
            hostname=fqdn,
            ip=ip,
            external_port=self.config.external_port,
            control_path=self.nginx.control_path,
        )

    def _reload_nginx(self) -> None:
        ports = [
            ReplicaPort(name=r.name, port=r.port)
            for r in self.replica_manager.get_replicas()
        ]
        try:
            self.nginx.reload(ports)
        except Exception:
            logging.getLogger(__name__).exception("nginx reload failed")

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
                phase=r.phase,
                started_at=r.started_at,
            )
            for r in self.replica_manager.get_replicas()
        ]
        return PilotJobStatus(
            resources=self.replica_manager.query_resources(),
            replicas=replica_statuses,
        )

    def get_replica_logs(self, replica_name: str) -> ReplicaLogTail:
        replica = self.replica_manager.get_replica(replica_name)
        return replica.get_logs()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    config = Config.load()
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
    return cast(_PilotManager, request.state.pilot_manager)


PilotManager = Annotated[_PilotManager, Depends(get_manager)]


@app.post("/start-replica")
def start_replica(replica: ReplicaStartRequest, manager: PilotManager) -> None:
    manager.start_replica(replica)


@app.post("/stop-replica/{replica_name}")
def stop_replica(replica_name: str, manager: PilotManager) -> None:
    manager.stop_replica(replica_name)


@app.get("/status", response_model=PilotJobStatus)
def get_status(manager: PilotManager) -> PilotJobStatus:
    return manager.get_status()


@app.get("/logs/{replica_name}", response_model=ReplicaLogTail)
def get_replica_logs(replica_name: str, manager: PilotManager) -> ReplicaLogTail:
    return manager.get_replica_logs(replica_name)


@app.exception_handler(FirstError)
def handle_app_error(_request: Request, exc: FirstError) -> JSONResponse:
    return JSONResponse(
        {"error": {"code": exc.code, "message": str(exc), "info": exc.info}},
        status_code=exc.status_code,
    )


def entrypoint() -> None:
    config = Config.load()
    uvicorn.run(
        app,
        host="127.0.0.1",
        port=config.control_port_internal,
        log_level="INFO",
    )
