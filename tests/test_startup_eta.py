"""Startup ETA: per-deployment cold-start hints in the RouterConfig and the
503 message built from them."""

from datetime import datetime, timedelta, timezone

from first_common.schema.types import (
    OverloadPolicy,
    ReplicaState,
    RouterParams,
    UsagePolicy,
)
from first_gateway.controllers.workers.router_config_observer import (
    RouterConfigObserver,
)
from first_gateway.database import models as db
from first_gateway.database.redis.router_config import DeploymentConfig, ModelConfig
from first_gateway.services.orchestration import cold_start_error

NOW = datetime(2026, 7, 12, 11, 0, 0, tzinfo=timezone.utc)


def _dep(
    name: str,
    *,
    incoming: bool = False,
    placed_ago_sec: float | None = None,
    last_startup_sec: float | None = None,
    cluster_name: str = "c",
    maintenance_notice: str | None = None,
) -> DeploymentConfig:
    return DeploymentConfig(
        kind="pilot",
        name=name,
        cluster_name=cluster_name,
        maintenance_notice=maintenance_notice,
        router_params=RouterParams(),
        prometheus_metrics_path=None,
        prometheus_scrape_interval_sec=30,
        backends=[],
        incoming=incoming or placed_ago_sec is not None,
        # Anchored to the real clock: cold_start_error uses datetime.now().
        earliest_placed_at=(
            datetime.now(timezone.utc) - timedelta(seconds=placed_ago_sec)
            if placed_ago_sec is not None
            else None
        ),
        last_startup_sec=last_startup_sec,
    )


def _model(*deployments: DeploymentConfig) -> ModelConfig:
    return ModelConfig(
        name="open/model",
        aliases=[],
        allowed_groups=["all"],
        allowed_domains=[],
        supported_endpoints=["chat"],
        usage_limits=UsagePolicy(),
        overload=OverloadPolicy(),
        deployments=list(deployments),
    )


# ── cold_start_error ─────────────────────────────────────────────────


def test_eta_uses_each_deployments_own_placement_and_duration() -> None:
    """The soonest ETA wins, and each ETA pairs a deployment's own placed_at
    with its own last startup: the early-but-slow deployment (placed 60s ago,
    takes 600s -> 540s left) must not borrow the fast one's 100s duration."""
    model = _model(
        _dep("slow", placed_ago_sec=60, last_startup_sec=600),
        _dep("fast", placed_ago_sec=10, last_startup_sec=100),
    )
    exc = cold_start_error(model, 15, " Retry in 15 seconds.")

    # ~90s, allowing for test runtime between building the model and the call.
    assert 88 <= exc.info["startup_eta_sec"] <= 90
    assert "s until ready" in str(exc)
    assert str(exc).endswith(" Retry in 15 seconds.")
    assert exc.retry_after_sec == 15


def test_overdue_start_says_shortly_not_zero() -> None:
    model = _model(_dep("d", placed_ago_sec=500, last_startup_sec=100))
    exc = cold_start_error(model, None, "")
    assert exc.info["startup_eta_sec"] == 0
    assert str(exc) == "Model open/model is starting and should be ready shortly."


def test_deployment_scoped_request_ignores_other_deployments() -> None:
    """A request pinned to `slow` must not report `fast`'s ETA."""
    model = _model(
        _dep("slow", placed_ago_sec=60, last_startup_sec=600),
        _dep("fast", placed_ago_sec=10, last_startup_sec=100),
    )
    exc = cold_start_error(model, None, "", deployment_name="slow")
    assert 538 <= exc.info["startup_eta_sec"] <= 540
    assert exc.info["typical_startup_sec"] == 600


def test_incoming_without_eta_reports_typical() -> None:
    """A pending replica (not placed yet) is incoming but has no ETA."""
    model = _model(_dep("d", incoming=True, last_startup_sec=80))
    exc = cold_start_error(model, None, "")
    assert str(exc) == "Model open/model is starting. Startup typically takes ~80s."
    assert exc.info == {
        "startup_eta_sec": None,
        "typical_startup_sec": 80,
        "maintenance_notices": {},
    }


def test_not_running_reports_typical() -> None:
    model = _model(_dep("d", last_startup_sec=80))
    exc = cold_start_error(model, None, "")
    assert "is not running" in str(exc)
    assert "~80s" in str(exc)


def test_nothing_known_keeps_generic_message() -> None:
    exc = cold_start_error(_model(_dep("d")), 15, " Retry in 15 seconds.")
    assert str(exc) == "Backend not available yet. Retry in 15 seconds."
    assert exc.info == {
        "startup_eta_sec": None,
        "typical_startup_sec": None,
        "maintenance_notices": {},
    }


def test_all_deployments_in_maintenance_reports_notice() -> None:
    model = _model(
        _dep("d", last_startup_sec=80, maintenance_notice="Down until Monday 8am")
    )
    exc = cold_start_error(model, 15, " Retry in 15 seconds.")
    assert str(exc) == (
        "Model open/model is unavailable due to cluster maintenance: "
        "c: Down until Monday 8am. Retry in 15 seconds."
    )
    assert exc.info["maintenance_notices"] == {"c": "Down until Monday 8am"}


def test_partial_maintenance_appends_notice() -> None:
    """A deployment elsewhere is starting: report its ETA, then the notice."""
    model = _model(
        _dep("up", cluster_name="a", placed_ago_sec=10, last_startup_sec=100),
        _dep("down", cluster_name="b", maintenance_notice="PM today"),
    )
    exc = cold_start_error(model, None, "")
    assert "s until ready." in str(exc)
    assert str(exc).endswith(" Cluster maintenance: b: PM today.")
    assert exc.info["maintenance_notices"] == {"b": "PM today"}


def test_pinned_deployment_ignores_other_clusters_maintenance() -> None:
    model = _model(
        _dep("up", cluster_name="a", last_startup_sec=80),
        _dep("down", cluster_name="b", maintenance_notice="PM today"),
    )
    exc = cold_start_error(model, None, "", deployment_name="up")
    assert "maintenance" not in str(exc)
    assert exc.info["maintenance_notices"] == {}


# ── RouterConfigObserver._build_deployments ──────────────────────────


def _pilot(
    replicas: list[db.PilotReplica], last_startup_sec: float | None
) -> db.PilotDeployment:
    return db.PilotDeployment(
        name="c/pd",
        cluster_name="c",
        model_name="m",
        router_params={},
        prometheus_metrics_path=None,
        prometheus_scrape_interval_sec=30,
        min_replicas=0,
        max_replicas=1,
        launch_spec={},
        last_startup_sec=last_startup_sec,
        replicas=replicas,
    )


def _replica(state: ReplicaState, placed_at: datetime | None = None) -> db.PilotReplica:
    return db.PilotReplica(
        name=f"c/pd/replica/{state.value}",
        pilot_deployment_name="c/pd",
        state=state.value,
        placed_at=placed_at,
        model_url="http://x",
        observed_served_name="m",
    )


def test_pilot_deployment_listed_without_ready_replicas() -> None:
    """A deployment with only incoming replicas appears with no backends and
    the earliest placed_at among placed/launching replicas (pending has none)."""
    early, late = NOW - timedelta(seconds=30), NOW - timedelta(seconds=5)
    dep = _pilot(
        [
            _replica(ReplicaState.pending),
            _replica(ReplicaState.launching, placed_at=late),
            _replica(ReplicaState.placed, placed_at=early),
        ],
        last_startup_sec=120.0,
    )
    (cfg,) = RouterConfigObserver._build_deployments([dep], [], {})

    assert cfg.backends == []
    assert cfg.incoming is True
    assert cfg.earliest_placed_at == early
    assert cfg.last_startup_sec == 120.0
    assert cfg.startup_eta_sec(NOW) == 90.0


def test_idle_pilot_deployment_has_no_hints() -> None:
    (cfg,) = RouterConfigObserver._build_deployments([_pilot([], None)], [], {})
    assert cfg.backends == []
    assert cfg.incoming is False
    assert cfg.earliest_placed_at is None
    assert cfg.startup_eta_sec(NOW) is None


def test_terminating_replica_is_not_incoming() -> None:
    dep = _pilot([_replica(ReplicaState.terminating, placed_at=NOW)], 10.0)
    (cfg,) = RouterConfigObserver._build_deployments([dep], [], {})
    assert cfg.incoming is False
    assert cfg.earliest_placed_at is None


def _static(health: str) -> db.StaticDeployment:
    return db.StaticDeployment(
        uid=1,
        name="c/sd",
        cluster_name="c",
        model_name="m",
        api_url="http://x/v1",
        api_key=None,
        upstream_model_name="m",
        router_params={},
        health_check={},
        health=health,
        prometheus_metrics_path=None,
        prometheus_scrape_interval_sec=30,
    )


def test_unhealthy_static_omitted_outside_maintenance() -> None:
    assert RouterConfigObserver._build_deployments([], [_static("unhealthy")], {}) == []


def test_unhealthy_static_listed_without_backends_in_maintenance() -> None:
    (cfg,) = RouterConfigObserver._build_deployments(
        [], [_static("unhealthy")], {"c": "PM today"}
    )
    assert cfg.backends == []
    assert cfg.maintenance_notice == "PM today"


def test_healthy_static_keeps_routing_in_maintenance() -> None:
    (cfg,) = RouterConfigObserver._build_deployments(
        [], [_static("healthy")], {"c": "PM today"}
    )
    assert len(cfg.backends) == 1
    assert cfg.maintenance_notice == "PM today"


def test_pilot_deployment_carries_maintenance_notice() -> None:
    (cfg,) = RouterConfigObserver._build_deployments(
        [_pilot([], None)], [], {"c": "PM today"}
    )
    assert cfg.maintenance_notice == "PM today"


def test_pending_replica_is_not_incoming_in_maintenance() -> None:
    """Placement is blocked during maintenance, so pending won't progress."""
    dep = _pilot([_replica(ReplicaState.pending)], 10.0)
    (cfg,) = RouterConfigObserver._build_deployments([dep], [], {"c": "PM today"})
    assert cfg.incoming is False


def test_placed_replica_is_not_incoming_in_maintenance() -> None:
    """A placed replica waits on a pilot that won't be submitted or launch."""
    dep = _pilot([_replica(ReplicaState.placed, placed_at=NOW)], 10.0)
    (cfg,) = RouterConfigObserver._build_deployments([dep], [], {"c": "PM today"})
    assert cfg.incoming is False


def test_launching_replica_is_incoming_in_maintenance() -> None:
    dep = _pilot([_replica(ReplicaState.launching, placed_at=NOW)], 10.0)
    (cfg,) = RouterConfigObserver._build_deployments([dep], [], {"c": "PM today"})
    assert cfg.incoming is True
    assert cfg.earliest_placed_at == NOW
