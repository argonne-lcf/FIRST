"""
The internet-facing gateway holds no credential that can submit work to a
cluster (P-01): it starts without the Globus Compute / Keycloak clients,
refuses to start with them, and its API never reaches a scheduler adapter.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
import pytest
from asgi_lifespan import LifespanManager
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from first_common.schema.base_scheduler import SchedulerJobState
from first_common.schema.types import ReplicaState
from first_gateway.apiserver.api import GATEWAY_ENV_FILES, app
from first_gateway.database.models import (
    AccessGroup,
    Cluster,
    Model,
    PilotDeployment,
    PilotJob,
    PilotReplica,
)
from first_gateway.platforms import schedulers
from first_gateway.settings import Settings

from .fixtures.auth import ADMIN_TOKEN, auth_header
from .fixtures.db import LAUNCH_TEMPLATE_NAME, launch_template

_KEYCLOAK_CLIENTS = """{"tara-pbs": {"base_url": "https://kc.invalid", "realm": "r",
  "impersonation_client_id": "c", "impersonation_client_secret": "s",
  "audience": "a"}}"""

# --- startup -----------------------------------------------------------------


async def test_gateway_starts_without_cluster_credentials(
    client: httpx.AsyncClient,
) -> None:
    resp = await client.get("/health")
    assert resp.status_code == 200, resp.text

    state = app.state.client_state
    assert state.compute_client is None
    assert state.keycloak_clients == {}
    assert state.settings.globus.compute_client_secret is None


@pytest.mark.parametrize(
    "name, value, reported",
    [
        (
            "FIRST_GLOBUS__COMPUTE_CLIENT_SECRET",
            "leaked-secret",
            "FIRST_GLOBUS__COMPUTE_CLIENT_SECRET",
        ),
        ("FIRST_KEYCLOAK_CLIENTS", _KEYCLOAK_CLIENTS, "FIRST_KEYCLOAK_CLIENTS"),
    ],
)
async def test_gateway_refuses_to_start_with_cluster_credentials(
    db: async_sessionmaker[AsyncSession],
    mock_globus: None,
    pilot_mtls_settings: None,
    gateway_env: None,
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    value: str,
    reported: str,
) -> None:
    monkeypatch.setenv(name, value)
    with pytest.raises(RuntimeError, match="Refusing to start the gateway") as exc:
        async with LifespanManager(app):
            pass
    assert reported in str(exc.value)
    # Names only: the message never carries the secret itself.
    assert "leaked-secret" not in str(exc.value)
    assert "impersonation_client_secret" not in str(exc.value)


def test_gateway_does_not_layer_shared_env_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    On a developer host the shared .env.secret also feeds a host-run
    controller. The gateway reads .env.secret.gateway instead.
    """
    monkeypatch.chdir(tmp_path)
    for name, value in {
        "FIRST_DB_URL": "postgresql+psycopg://u:p@localhost/x",
        "FIRST_REDIS_URL": "redis://localhost/0",
        "FIRST_GLOBUS__APP_ID": "app",
        "FIRST_GLOBUS__APP_SECRET": "app-secret",
        "FIRST_GLOBUS__ADMIN_GROUP": "admins",
        # No .env.default in tmp_path: set the global gates explicitly.
        "FIRST_GLOBUS__POLICIES": '["policy"]',
        "FIRST_GLOBUS__AUTHORIZED_IDP_DOMAINS": '["anl.gov"]',
    }.items():
        monkeypatch.setenv(name, value)
    for name in ("FIRST_GLOBUS__COMPUTE_CLIENT_SECRET", "FIRST_KEYCLOAK_CLIENTS"):
        monkeypatch.delenv(name, raising=False)
    (tmp_path / ".env.secret").write_text(
        "FIRST_GLOBUS__COMPUTE_CLIENT_SECRET=controller-only\n"
    )
    (tmp_path / ".env.secret.gateway").write_text(
        "FIRST_DATA_STAGING_CLIENT_ID=staging-app\n"
    )

    gateway = Settings(_env_file=GATEWAY_ENV_FILES)
    assert gateway.globus.compute_client_secret is None
    assert gateway.data_staging_client_id == "staging-app"

    # Host-run controller (and tooling) keep the shared file.
    controller = Settings()
    assert controller.globus.compute_client_secret is not None


def test_compute_client_secret_without_id_is_an_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FIRST_GLOBUS__COMPUTE_CLIENT_SECRET", "s")
    monkeypatch.setenv("FIRST_GLOBUS__COMPUTE_CLIENT_ID", "")
    with pytest.raises(ValueError, match="COMPUTE_CLIENT_ID"):
        Settings()._build_compute_client()


# --- finished-replica logs ---------------------------------------------------


async def _seed_replica(
    sess: AsyncSession,
    *,
    scheduler_state: SchedulerJobState,
    replica_state: ReplicaState,
) -> None:
    sess.add(AccessGroup(name="ag", allowed_groups=[], allowed_domains=[]))
    sess.add(
        Cluster(
            name="cl",
            health_check={"health_url": ""},
            pilot_system={"scheduler_adapter": "would-need-a-cluster-credential"},
        )
    )
    await sess.flush()
    sess.add(Model(name="mdl", access_group_name="ag", supported_endpoints=["chat"]))
    sess.add(launch_template())
    await sess.flush()
    sess.add(
        PilotDeployment(
            name="pd",
            cluster_name="cl",
            model_name="mdl",
            launch_template_name=LAUNCH_TEMPLATE_NAME,
            router_params={},
            prometheus_scrape_interval_sec=30,
            min_replicas=0,
            max_replicas=1,
            launch_spec={},
        )
    )
    sess.add(
        PilotJob(
            name="job",
            cluster_name="cl",
            walltime_min=60,
            num_nodes=1,
            gpus_per_node=4,
            scheduler_state=scheduler_state.value,
            manager_url="https://pilot.invalid:8443",
        )
    )
    await sess.flush()
    sess.add(
        PilotReplica(
            name="rep",
            pilot_deployment_name="pd",
            pilot_job_name="job",
            state=replica_state.value,
            log_path="/lus/flare/first/logs/rep.log",
        )
    )
    await sess.commit()


@pytest.fixture
def no_scheduler(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Record (and refuse) any attempt to build a scheduler adapter."""
    calls: list[Any] = []

    async def refuse(*args: Any, **kwargs: Any) -> Any:
        calls.append(args)
        raise AssertionError("the gateway must not build a scheduler adapter")

    monkeypatch.setattr(schedulers, "build_scheduler", refuse)
    for adapter in (schedulers.GlobusComputePBSAdapter, schedulers.GraphQLPBSAdapter):
        monkeypatch.setattr(adapter, "build", classmethod(refuse))
    return calls


async def test_finished_replica_logs_409_without_scheduler_call(
    db_session: AsyncSession,
    client: httpx.AsyncClient,
    no_scheduler: list[Any],
) -> None:
    await _seed_replica(
        db_session,
        scheduler_state=SchedulerJobState.gone,
        replica_state=ReplicaState.terminated,
    )
    resp = await client.get(
        "/catalog/v1/pilot-replicas/rep/logs", headers=auth_header(ADMIN_TOKEN)
    )
    assert resp.status_code == 409, resp.text
    assert "/lus/flare/first/logs/rep.log" in resp.text
    assert no_scheduler == []


async def test_live_replica_logs_come_from_the_pilot(
    db_session: AsyncSession,
    client: httpx.AsyncClient,
    no_scheduler: list[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _seed_replica(
        db_session,
        scheduler_state=SchedulerJobState.running,
        replica_state=ReplicaState.ready,
    )
    pilot_control = app.state.pilot_control_client
    seen: list[tuple[str, str, str]] = []

    async def fake_get_logs(
        manager_url: str, replica: str, *, cluster_name: str
    ) -> str:
        seen.append((manager_url, replica, cluster_name))
        return "log tail"

    monkeypatch.setattr(pilot_control, "get_logs", fake_get_logs)
    resp = await client.get(
        "/catalog/v1/pilot-replicas/rep/logs", headers=auth_header(ADMIN_TOKEN)
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == "log tail"
    assert seen == [("https://pilot.invalid:8443", "rep", "cl")]
    assert no_scheduler == []
