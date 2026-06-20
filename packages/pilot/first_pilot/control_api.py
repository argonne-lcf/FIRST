import logging
import socket
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated, Generator, cast

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, computed_field

from first_common.errors import FirstError
from first_common.schema.pilot import ReplicaStartRequest

from .config import Config
from .nginx_manager import NginxManager
from .replica_manager import ReplicaManager


class AddressInfo(BaseModel):
    hostname: str
    ip: str
    external_port: int
    control_path: str

    @computed_field
    @property
    def control_url(self) -> str:
        return f"https://{self.ip}:{self.external_port}/{self.control_path.lstrip('/')}"


class _PilotManager:
    def __init__(self, config: Config, tmpdir: Path) -> None:
        self.config = config
        self.nginx = NginxManager(self.config, tmpdir)
        self.replica_manager = ReplicaManager(self.config)

    def start(self) -> None:
        self.nginx.start()
        self.nginx.wait_until_healthy()
        readyfile = self.config.workdir / f"{self.config.job_name}.ready.json"
        endpoint_info = self.discover_service_endpoint()
        readyfile.write_text(endpoint_info.model_dump_json())

    def stop(self) -> None:
        self.nginx.stop()

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

    def start_replica(self, replica: ReplicaStartRequest) -> None:
        self.replica_manager.start_replica(replica)


@contextmanager
def lifespan(_app: FastAPI) -> Generator[_PilotManager, None]:
    config = Config()
    config.workdir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level="INFO",
        format="%(asctime)s %(levelname)-8s %(name)s:%(lineno)d %(message)s",
    )

    with TemporaryDirectory(
        dir=config.workdir,
        prefix=f"nginx-tmp-{config.job_name}-",
        ignore_cleanup_errors=True,
    ) as tmpdir:
        manager = _PilotManager(config, tmpdir)
        try:
            manager.start()
            yield manager
        finally:
            manager.stop()


app = FastAPI(lifespan=lifespan)


async def get_manager(request: Request) -> _PilotManager:
    return cast(_PilotManager, request.state)


PilotManager = Annotated[_PilotManager, Depends(get_manager)]


@app.post("/start-replica")
def start_replica(replica: ReplicaStartRequest, manager: PilotManager):
    manager.start_replica(replica)


@app.post("/stop-replica")
def stop_replica(): ...


@app.get("/status")
def get_status(): ...


@app.get("/logs/{replica_name}")
def get_replica_logs(replica_name: str): ...


@app.exception_handler(FirstError)
def handle_app_error(_request: Request, exc: FirstError) -> JSONResponse:
    return JSONResponse(
        {"error": {"code": exc.code, "message": str(exc), "info": exc.info}},
        status_code=exc.status_code,
    )
