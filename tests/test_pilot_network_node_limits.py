"""
Sophia hardening (P-07): the pilot `ip_allowlist` rejects wildcard and broad
networks, and `max_nodes_per_job` bounds every PilotDeployment at plan/apply,
placement and submission time.
"""

import copy
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from jinja2 import Template
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import Session

from first_common.errors import InvalidSpecError
from first_common.schema.base_scheduler import SchedulerJobState
from first_common.schema.pilot import PilotClientRole, PilotRuntimeConfig
from first_common.schema.resources import ResourceManifest
from first_common.schema.resources.spec import ClusterSpec
from first_common.schema.types import PilotConfig, ReplicaState
from first_gateway.admin.cli import load_resources_from_yaml
from first_gateway.controllers.workers.pilot_job_controller import PilotJobController
from first_gateway.controllers.workers.replica_placement import (
    EXCEEDS_NODES_PER_JOB,
    ReplicaPlacer,
)
from first_gateway.database.models import (
    AccessGroup,
    Cluster,
    Model,
    PilotDeployment,
    PilotJob,
    PilotReplica,
)
from first_gateway.services.pilot_submitter import PilotSubmitter
from first_gateway.services.plan_apply import validate_resources
from first_pilot.nginx_manager import _conf_template_str

from .fixtures.auth import ADMIN_TOKEN, auth_header
from .fixtures.db import ALEMBIC_INI, LAUNCH_TEMPLATE_NAME, launch_template
from .test_pilot_job_controller import FakeSchedulerAdapter

RESOURCES_DIR = Path(__file__).parent / "resource_specs"

PILOT_SYSTEM: dict[str, Any] = {
    "scheduler_adapter": (
        "first_gateway.platforms.schedulers.globus_compute_pbs.GlobusComputePBSAdapter"
    ),
    "job_walltime_min": 60,
    "max_concurrent_jobs": 5,
    "max_num_nodes": 10,
    "max_nodes_per_job": 1,
    "gpus_per_node": 8,
    "queue": "infer-svc",
    "account": "TestAcct",
    "workdir": "/tmp/pilot_workdir",
    "external_port": 9443,
    "nginx_path": "/usr/sbin/nginx",
    "ip_allowlist": ["140.221.33.155/32", "140.221.33.25/32"],
    "node_file_env": "PBS_NODEFILE",
    "submit_script_preamble": "umask 077",
    "pilot_path": "/test/first-pilot",
    "pilot_config_path": "/tmp/pilot-config.yaml",
}


def _pilot_config(**overrides: Any) -> PilotConfig:
    return PilotConfig.model_validate({**PILOT_SYSTEM, **overrides})


def _runtime_config(ip_allowlist: Any) -> PilotRuntimeConfig:
    return PilotRuntimeConfig(
        ca_crt="ca",
        server_crt="crt",
        server_key="key",
        external_port=9443,
        nginx_path=Path("/usr/sbin/nginx"),
        ip_allowlist=ip_allowlist,
        workdir=Path("/tmp/w"),
        node_file_env="PBS_NODEFILE",
        num_nodes=1,
        gpus_per_node=8,
        job_name="job",
        cluster="sophia",
        walltime_min=60,
        network_interfaces=["eth0"],
        queue="infer-svc",
        account="TestAcct",
    )


# ---------------------------------------------------------------------------
# ip_allowlist
# ---------------------------------------------------------------------------

ACCEPTED = [
    ["140.221.33.155/32"],
    ["10.1.2.0/24"],
    ["2001:db8::/64"],
    ["2001:db8::1/128"],
    ["140.221.33.155/32", "140.221.33.25/32"],
]

REJECTED = [
    pytest.param([], id="empty"),
    pytest.param(["0.0.0.0/0"], id="ipv4-wildcard"),
    pytest.param(["::/0"], id="ipv6-wildcard"),
    pytest.param(["10.0.0.0/23"], id="ipv4-slash23"),
    pytest.param(["10.0.0.0/8"], id="ipv4-slash8"),
    pytest.param(["2001:db8::/63"], id="ipv6-slash63"),
    pytest.param(["140.221.33.155/32", "0.0.0.0/0"], id="wildcard-among-hosts"),
    pytest.param(["::ffff:0.0.0.0/96"], id="ipv4-mapped-wildcard"),
    pytest.param(["::ffff:10.0.0.0/119"], id="ipv4-mapped-slash23"),
    pytest.param(["any"], id="garbage"),
    pytest.param(["*"], id="star"),
    pytest.param(["140.221.33.155/33"], id="bad-prefix"),
    pytest.param(["10.0.0.1/24"], id="host-bits-set"),
    pytest.param([" 140.221.33.155/32"], id="whitespace"),
    pytest.param([167772160], id="integer"),
    pytest.param("140.221.33.155/32", id="bare-string-not-list"),
]


@pytest.mark.parametrize("allowlist", ACCEPTED)
def test_allowlist_accepts_narrow_networks(allowlist: list[str]) -> None:
    for config in (_pilot_config(ip_allowlist=allowlist), _runtime_config(allowlist)):
        assert [str(n) for n in config.ip_allowlist] == allowlist


@pytest.mark.parametrize("allowlist", REJECTED)
def test_allowlist_rejects_wildcards_broad_networks_and_garbage(
    allowlist: Any,
) -> None:
    with pytest.raises(ValidationError):
        _pilot_config(ip_allowlist=allowlist)
    with pytest.raises(ValidationError):
        _runtime_config(allowlist)


def test_allowlist_error_names_the_problem() -> None:
    with pytest.raises(ValidationError, match="too broad"):
        _pilot_config(ip_allowlist=["10.0.0.0/16"])
    with pytest.raises(ValidationError, match="matches every address"):
        _pilot_config(ip_allowlist=["0.0.0.0/0"])
    with pytest.raises(ValidationError, match="host bits set"):
        _pilot_config(ip_allowlist=["10.0.0.1/24"])


def test_bare_address_is_a_single_host() -> None:
    config = _pilot_config(ip_allowlist=["140.221.33.155", "2001:db8::1"])
    assert [str(n) for n in config.ip_allowlist] == [
        "140.221.33.155/32",
        "2001:db8::1/128",
    ]


def test_allowlist_serializes_as_unchanged_strings() -> None:
    config = _pilot_config()
    dumped = config.model_dump(mode="json")["ip_allowlist"]
    assert dumped == ["140.221.33.155/32", "140.221.33.25/32"]
    # The stored (JSONB) form re-validates to the same config.
    assert PilotConfig.model_validate(config.model_dump(mode="json")) == config
    assert (
        PilotConfig.model_json_schema()["properties"]["ip_allowlist"]["minItems"] == 1
    )


def test_submitter_and_nginx_render_plain_network_strings() -> None:
    config = _pilot_config()
    job = SimpleNamespace(
        name="sophia-pilot-1",
        cluster_name="sophia",
        num_nodes=1,
        gpus_per_node=8,
        walltime_min=60,
    )
    script = PilotSubmitter(config, MagicMock())._render_script(job)  # type: ignore[arg-type]
    assert 'PILOT_IP_ALLOWLIST=\'["140.221.33.155/32","140.221.33.25/32"]\'' in (script)

    # The pilot parses that environment value back into networks...
    runtime = _runtime_config(["140.221.33.155/32", "140.221.33.25/32"])
    # ...and NGINX renders them exactly as listed.
    rendered = Template(_conf_template_str).render(
        config=SimpleNamespace(
            control_uds_path=Path("/tmp/control.sock"),
            external_port=9443,
            ip_allowlist=runtime.ip_allowlist,
        ),
        nginx_tmpdir="/tmp/nginx",
        control_path="/control/",
        roles=PilotClientRole,
        ca_crt_path="/tmp/ca.crt",
        server_crt_path="/tmp/server.crt",
        server_key_path="/tmp/server.key",
        access_log_path="/tmp/a.log",
        audit_log_path="/tmp/a.jsonl",
        replicas=[],
    )
    control = rendered.split("location /control/ {", 1)[1].split("}", 1)[0]
    assert "allow 140.221.33.155/32;" in control
    assert "allow 140.221.33.25/32;" in control
    assert "deny all;" in control


def test_runtime_config_parses_allowlist_from_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "pilot.yaml"
    config_path.write_text(
        "ca_crt: ca\nserver_crt: crt\nserver_key: key\nnetwork_interfaces: [eth0]\n"
    )
    env = {
        "PILOT_CONFIG_FILE": str(config_path),
        "PILOT_JOB_NAME": "j",
        "PILOT_CLUSTER": "sophia",
        "PILOT_EXTERNAL_PORT": "9443",
        "PILOT_NGINX_PATH": "/n",
        "PILOT_WORKDIR": "/w",
        "PILOT_NODE_FILE_ENV": "PBS_NODEFILE",
        "PILOT_QUEUE": "q",
        "PILOT_ACCOUNT": "a",
        "PILOT_NUM_NODES": "1",
        "PILOT_GPUS_PER_NODE": "8",
        "PILOT_WALLTIME_MIN": "60",
    }
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    monkeypatch.setenv("PILOT_IP_ALLOWLIST", '["140.221.33.155/32"]')
    assert [str(n) for n in PilotRuntimeConfig.load().ip_allowlist] == [
        "140.221.33.155/32"
    ]

    # A pilot refuses to start with a wildcard, even if a gateway sent one.
    monkeypatch.setenv("PILOT_IP_ALLOWLIST", '["0.0.0.0/0"]')
    with pytest.raises(ValidationError):
        PilotRuntimeConfig.load()


# ---------------------------------------------------------------------------
# max_nodes_per_job: schema
# ---------------------------------------------------------------------------


def test_max_nodes_per_job_is_required() -> None:
    pilot_system = {k: v for k, v in PILOT_SYSTEM.items() if k != "max_nodes_per_job"}
    with pytest.raises(ValidationError, match="max_nodes_per_job"):
        PilotConfig.model_validate(pilot_system)
    with pytest.raises(ValidationError, match="max_nodes_per_job"):
        ResourceManifest.model_validate(
            {
                "kind": "Cluster",
                "name": "sophia",
                "spec": {"health_check": {"url": ""}, "pilot_system": pilot_system},
            }
        )


@pytest.mark.parametrize("value", [0, -1])
def test_max_nodes_per_job_must_be_positive(value: int) -> None:
    with pytest.raises(ValidationError, match="max_nodes_per_job"):
        _pilot_config(max_nodes_per_job=value)


def test_cluster_without_pilot_system_needs_no_limit() -> None:
    spec = ClusterSpec.model_validate({"health_check": {"url": ""}})
    assert spec.pilot_system is None


# ---------------------------------------------------------------------------
# max_nodes_per_job: plan/apply validation
# ---------------------------------------------------------------------------


def _baseline(max_nodes_per_job: int, num_nodes: int) -> list[ResourceManifest]:
    resources = load_resources_from_yaml(RESOURCES_DIR / "baseline")
    raw = [r.model_dump(mode="json") for r in resources]
    for r in raw:
        if r["kind"] == "Cluster":
            r["spec"]["pilot_system"]["max_nodes_per_job"] = max_nodes_per_job
        if r["kind"] == "PilotDeployment":
            r["spec"]["launch_spec"]["num_nodes"] = num_nodes
    return [ResourceManifest.model_validate(r) for r in raw]


def test_validate_rejects_deployment_over_the_limit() -> None:
    with pytest.raises(InvalidSpecError, match="max_nodes_per_job"):
        validate_resources(_baseline(max_nodes_per_job=1, num_nodes=2))


@pytest.mark.parametrize("num_nodes", [1, 2])
def test_validate_accepts_deployment_within_the_limit(num_nodes: int) -> None:
    validate_resources(_baseline(max_nodes_per_job=2, num_nodes=num_nodes))


async def test_plan_endpoint_rejects_deployment_over_the_limit(
    control_client: httpx.AsyncClient,
) -> None:
    resp = await control_client.post(
        "/control/v1/plan",
        json={
            "resources": [
                r.model_dump(mode="json")
                for r in _baseline(max_nodes_per_job=1, num_nodes=2)
            ]
        },
        headers=auth_header(ADMIN_TOKEN),
    )
    assert resp.status_code == 400
    body = resp.json()
    assert body["error"]["code"] == "resource_spec_invalid"
    assert "max_nodes_per_job (1)" in body["error"]["message"]


async def test_bare_address_round_trips_without_plan_drift(
    control_client: httpx.AsyncClient, client: httpx.AsyncClient
) -> None:
    raw = [r.model_dump(mode="json") for r in _baseline(1, 1)]
    for r in raw:
        if r["kind"] == "Cluster":
            r["spec"]["pilot_system"]["ip_allowlist"] = ["140.221.33.155"]
    headers = auth_header(ADMIN_TOKEN)

    plan = await control_client.post(
        "/control/v1/plan", json={"resources": raw}, headers=headers
    )
    assert plan.status_code == 200, plan.text
    applied = await control_client.post(
        "/control/v1/apply",
        json={"resources": raw, "approved_plan": plan.json()},
        headers=headers,
    )
    assert applied.status_code == 200, applied.text

    replan = await control_client.post(
        "/control/v1/plan", json={"resources": raw}, headers=headers
    )
    assert replan.status_code == 200
    assert replan.json()["to_update"] == []

    # The API (and so the alcf_ai CLI's JSON output) shows plain strings.
    detail = await client.get("/catalog/v1/clusters/sophia", headers=headers)
    assert detail.status_code == 200, detail.text
    pilot_system = detail.json()["pilot_system"]
    assert pilot_system["ip_allowlist"] == ["140.221.33.155/32"]
    assert pilot_system["max_nodes_per_job"] == 1


# ---------------------------------------------------------------------------
# max_nodes_per_job: controller refusal
# ---------------------------------------------------------------------------


async def _seed(sess: AsyncSession, *, num_nodes: int) -> None:
    sess.add(
        Cluster(name="sophia", health_check={"url": ""}, pilot_system=PILOT_SYSTEM)
    )
    sess.add(AccessGroup(name="ag", allowed_groups=[], allowed_domains=[]))
    await sess.flush()
    sess.add(Model(name="m", access_group_name="ag", supported_endpoints=["chat"]))
    sess.add(launch_template())
    await sess.flush()
    sess.add(
        PilotDeployment(
            name="sophia/m",
            cluster_name="sophia",
            model_name="m",
            launch_template_name=LAUNCH_TEMPLATE_NAME,
            router_params={},
            prometheus_scrape_interval_sec=30,
            min_replicas=0,
            max_replicas=1,
            launch_spec={"num_nodes": num_nodes, "gpus_per_node": 8},
        )
    )
    await sess.flush()


def _client_state(db: async_sessionmaker[AsyncSession]) -> MagicMock:
    cs = MagicMock()
    cs.db_sessionmaker = db
    cs.redis_pubsub.publish = AsyncMock()
    return cs


@pytest.mark.parametrize("existing_job", [False, True])
async def test_placer_refuses_replica_over_the_limit(
    db: async_sessionmaker[AsyncSession], existing_job: bool
) -> None:
    async with db.begin() as sess:
        await _seed(sess, num_nodes=2)
        if existing_job:
            # An empty 2-node job from before the limit is not reused either.
            sess.add(
                PilotJob(
                    name="old-2-node",
                    cluster_name="sophia",
                    scheduler_state=SchedulerJobState.running.value,
                    walltime_min=60,
                    num_nodes=2,
                    gpus_per_node=8,
                )
            )
        replica = PilotReplica(
            name="sophia/m/r0", pilot_deployment_name="sophia/m", state="pending"
        )
        sess.add(replica)
        await sess.flush()
        uid = replica.uid

    await ReplicaPlacer("placer", _client_state(db), MagicMock()).reconcile(uid)

    async with db() as sess:
        r = await sess.get(PilotReplica, uid)
        assert r is not None
        assert r.state == ReplicaState.pending.value
        assert r.pilot_job_name is None
        assert r.state_message == EXCEEDS_NODES_PER_JOB.format(
            req=2, cluster="sophia", limit=1
        )
        jobs = await sess.scalar(sa.select(sa.func.count()).select_from(PilotJob))
        assert jobs == (1 if existing_job else 0)


async def test_placer_accepts_replica_at_the_limit(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed(sess, num_nodes=1)
        replica = PilotReplica(
            name="sophia/m/r0", pilot_deployment_name="sophia/m", state="pending"
        )
        sess.add(replica)
        await sess.flush()
        uid = replica.uid

    await ReplicaPlacer("placer", _client_state(db), MagicMock()).reconcile(uid)

    async with db() as sess:
        r = await sess.get(PilotReplica, uid)
        assert r is not None and r.state == ReplicaState.placed.value


async def test_controller_refuses_to_submit_job_over_the_limit(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed(sess, num_nodes=1)
        job = PilotJob(
            name="sophia-pilot-big",
            cluster_name="sophia",
            walltime_min=60,
            num_nodes=2,
            gpus_per_node=8,
        )
        sess.add(job)
        await sess.flush()
        uid = job.uid

    adapter = FakeSchedulerAdapter()
    controller = PilotJobController("pjc", _client_state(db), MagicMock())
    with patch(
        "first_gateway.controllers.workers.pilot_job_controller.build_scheduler",
        new_callable=AsyncMock,
        return_value=adapter,
    ):
        await controller.reconcile(uid)
        async with db() as sess:
            refused = await sess.get(PilotJob, uid)
        assert refused is not None
        assert refused.scheduler_job_id is None
        assert refused.scheduled_deletion_at is not None
        assert refused.failure_reason is not None
        assert "max_nodes_per_job (1)" in refused.failure_reason

        # The next pass deletes it without ever contacting the scheduler.
        await controller.reconcile(uid)

    assert adapter.submitted == []
    assert adapter.terminated == []
    async with db() as sess:
        gone = await sess.get(PilotJob, uid)
    assert gone is not None
    assert gone.deleted_at is not None
    assert gone.scheduler_state == SchedulerJobState.gone.value


async def test_controller_submits_job_at_the_limit(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed(sess, num_nodes=1)
        job = PilotJob(
            name="sophia-pilot-ok",
            cluster_name="sophia",
            walltime_min=60,
            num_nodes=1,
            gpus_per_node=8,
        )
        sess.add(job)
        await sess.flush()
        uid = job.uid

    adapter = FakeSchedulerAdapter()
    controller = PilotJobController("pjc", _client_state(db), MagicMock())
    with patch(
        "first_gateway.controllers.workers.pilot_job_controller.build_scheduler",
        new_callable=AsyncMock,
        return_value=adapter,
    ):
        await controller.reconcile(uid)
    assert len(adapter.submitted) == 1


# ---------------------------------------------------------------------------
# Rollout: legacy stored rows (Alembic revision a68035973f3a)
# ---------------------------------------------------------------------------

_BEFORE = "e77cb1713597"
_BACKFILL = "a68035973f3a"


def _legacy(**overrides: Any) -> dict[str, Any]:
    """A pilot_system stored before max_nodes_per_job existed."""
    row = copy.deepcopy(PILOT_SYSTEM)
    del row["max_nodes_per_job"]
    row.update(overrides)
    return row


def _insert_clusters(engine: sa.Engine, rows: dict[str, Any]) -> None:
    # The cluster table is unchanged by the backfill revision, so the head ORM
    # model can write rows at the previous revision.
    with Session(engine) as sess, sess.begin():
        for name, pilot_system in rows.items():
            sess.add(Cluster(name=name, health_check={}, pilot_system=pilot_system))


def _pilot_systems(engine: sa.Engine) -> dict[str, Any]:
    with engine.connect() as conn:
        result = conn.execute(sa.text("SELECT name, pilot_system FROM first.cluster"))
        return {name: pilot_system for name, pilot_system in result}


@pytest.fixture
def migrated_url(db: async_sessionmaker[AsyncSession]) -> str:
    """A fresh database migrated to head (see the `db` fixture)."""
    return os.environ["FIRST_DB_URL"]


def _alembic(url: str) -> Config:
    cfg = Config(str(ALEMBIC_INI))
    cfg.attributes["connection_url"] = url
    return cfg


def test_migration_backfills_legacy_rows_readably(migrated_url: str) -> None:
    cfg = _alembic(migrated_url)
    command.downgrade(cfg, _BEFORE)
    engine = sa.create_engine(migrated_url)
    try:
        _insert_clusters(
            engine,
            {
                # Dev Sophia today: three /32s, total cap 10.
                "sophia": _legacy(
                    ip_allowlist=[
                        "140.221.33.155/32",
                        "140.221.69.42/32",
                        "140.221.69.53/32",
                    ]
                ),
                # Prod Tara today.
                "tara": _legacy(max_num_nodes=336, ip_allowlist=["140.221.33.25/32"]),
                # Static-only cluster.
                "metis": None,
                # Already carries the field (e.g. re-upgraded after apply).
                "kept": {**_legacy(), "max_nodes_per_job": 2},
            },
        )

        # Legacy rows are unreadable by the new schema before the backfill...
        with pytest.raises(ValidationError):
            PilotConfig.model_validate(_pilot_systems(engine)["sophia"])

        command.upgrade(cfg, _BACKFILL)

        stored = _pilot_systems(engine)
        assert stored["metis"] is None
        # ...and readable after it, with no new restriction until apply.
        sophia = PilotConfig.model_validate(stored["sophia"])
        assert sophia.max_nodes_per_job == 10
        assert [str(n) for n in sophia.ip_allowlist] == [
            "140.221.33.155/32",
            "140.221.69.42/32",
            "140.221.69.53/32",
        ]
        assert PilotConfig.model_validate(stored["tara"]).max_nodes_per_job == 336
        assert PilotConfig.model_validate(stored["kept"]).max_nodes_per_job == 2

        command.downgrade(cfg, _BEFORE)
        assert all(
            "max_nodes_per_job" not in ps
            for ps in _pilot_systems(engine).values()
            if ps is not None
        )
        command.upgrade(cfg, "head")
    finally:
        engine.dispose()


def test_migration_refuses_rows_the_new_schema_cannot_read(
    migrated_url: str,
) -> None:
    cfg = _alembic(migrated_url)
    command.downgrade(cfg, _BEFORE)
    engine = sa.create_engine(migrated_url)
    try:
        _insert_clusters(engine, {"wide": _legacy(ip_allowlist=["0.0.0.0/0"])})
        with pytest.raises(RuntimeError, match="Cluster wide"):
            command.upgrade(cfg, _BACKFILL)
        # Rolled back: the row is untouched and still at the old revision.
        assert "max_nodes_per_job" not in _pilot_systems(engine)["wide"]
        with engine.connect() as conn:
            version = conn.execute(
                sa.text("SELECT version_num FROM first.alembic_version")
            ).scalar()
        assert version == _BEFORE
    finally:
        engine.dispose()
