import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Any, ClassVar

import httpx
import sqlalchemy as sa
from sqlalchemy.orm import selectinload

from first_common.schema.resources.runtime import (
    CommittedAlert,
    HealthAlertState,
    Severity,
    StagedTransition,
)
from first_common.schema.types import (
    HealthCheckResult,
    PilotConfig,
    PilotDeploymentState,
    ReplicaState,
)

from ...database.models import (
    Cluster,
    PilotDeployment,
    PilotJob,
    PilotReplica,
    StaticDeployment,
)
from ...platforms.schedulers import GlobusComputePBSAdapter, build_scheduler
from ...settings import ClientState
from ..wakeup import WakeupDispatcher
from ..worker import Worker
from .replica_placement import AT_CAPACITY

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Observation:
    key: str
    status: str
    summary: str
    severity: Severity
    owner: str = ""  # stamped by the check harness (producing check function)
    group: str = ""  # stamped by the check harness (Slack category)


@dataclass
class CheckResult:
    check_function: str
    success: bool
    error_msg: str | None
    observations: list[Observation]


@dataclass
class FlushPlan:
    """Matured transitions the state machine wants posted this tick."""

    degradations: list[tuple[str, StagedTransition]]  # status != ""
    recoveries: list[str]  # ALL matured recoveries (status == "")


# ---------------------------------------------------------------------------
# Slack formatting
# ---------------------------------------------------------------------------

_SEVERITY_ICON = {"crit": "🔴", "warn": "🟡", "info": "ℹ️"}
_SEVERITY_RANK = {"crit": 0, "warn": 1, "info": 2}

_GROUP_ORDER = [
    "Clusters",
    "Deployments",
    "Pilot Jobs",
    "Pilot Replicas",
    "Infrastructure",
]


def _render_grouped(lines_by_group: dict[str, list[str]]) -> str:
    """Render `{group: [line, ...]}` under group headers in canonical order."""
    out: list[str] = []
    for group in _GROUP_ORDER:
        for line in lines_by_group.get(group, []):
            if not out or out[-1] != f"*{group}*":
                out.append(f"*{group}*")
            out.append(f"  • {line}")
    text = "\n".join(out)
    if len(text) > 2900:
        text = text[:2900] + "\n…(truncated)"
    return text


def _build_alert_blocks(
    degradations: list[tuple[str, StagedTransition]],
    recoveries: list[tuple[str, StagedTransition]],
    failed_checks: list[tuple[str, str]],
) -> list[dict[str, Any]]:
    if degradations:
        has_crit = any(s.severity == "crit" for _, s in degradations)
        header = "🚨 Health degradation" if has_crit else "⚠️ Health update"
    elif recoveries:
        header = "✅ Recovery"
    else:
        header = "⚠️ Health update"

    blocks: list[dict[str, Any]] = [
        {"type": "header", "text": {"type": "plain_text", "text": header}},
    ]

    if degradations:
        degradations.sort(key=lambda x: _SEVERITY_RANK.get(x[1].severity, 3))
        grouped: dict[str, list[str]] = {}
        for key, staged in degradations:
            icon = _SEVERITY_ICON.get(staged.severity, "")
            grouped.setdefault(staged.group, []).append(
                f"{icon} {key} — {staged.summary}"
            )
        text = _render_grouped(grouped)
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})

    if recoveries:
        grouped_r: dict[str, list[str]] = {}
        for key, staged in recoveries:
            grouped_r.setdefault(staged.group, []).append(f"✅ {key} — recovered")
        text_r = _render_grouped(grouped_r)
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text_r}})

    if failed_checks:
        lines_f = ["*Check execution failures:*"]
        for fn, msg in failed_checks:
            lines_f.append(f"  • {fn}: {msg[:200]}")
        text_f = "\n".join(lines_f)
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text_f}})

    return blocks


def _build_digest_blocks(
    resource_counts: dict[str, tuple[int, int]],
    current_degradations: dict[str, str],
) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "📊 Daily Health Digest"},
        },
    ]
    lines: list[str] = []
    for category, (total, issues) in resource_counts.items():
        if issues > 0:
            lines.append(f"*{category}*: {total} total, {issues} open issue(s)")
        else:
            lines.append(f"*{category}*: {total} healthy")

    if current_degradations:
        lines.append("")
        lines.append("*Current degradations:*")
        for key, status in sorted(current_degradations.items()):
            lines.append(f"  • {key}: {status}")

    text = "\n".join(lines) or "All systems healthy."
    blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})
    return blocks


# ---------------------------------------------------------------------------
# Check functions — each returns only the unhealthy resources it finds.
# The harness stamps `owner` and `group` onto every Observation.
# ---------------------------------------------------------------------------


async def check_cluster_health(alerter: "HealthAlerter") -> list[Observation]:
    obs: list[Observation] = []
    async with alerter.client_state.db_sessionmaker() as sess:
        clusters = await Cluster.list(sess)
    for c in clusters:
        if c.health == HealthCheckResult.unhealthy.value:
            obs.append(
                Observation(
                    key=f"cluster/{c.uid}/health",
                    status="unhealthy",
                    summary=f"Cluster {c.name}: health unhealthy",
                    severity="crit",
                )
            )
    return obs


async def check_scheduler(alerter: "HealthAlerter") -> list[Observation]:
    obs: list[Observation] = []
    async with alerter.client_state.db_sessionmaker() as sess:
        clusters = await Cluster.list(sess)

    for c in clusters:
        if not c.pilot_system:
            continue
        key = f"cluster/{c.uid}/scheduler"
        try:
            pilot_config = PilotConfig.model_validate(c.pilot_system)
            adapter = await build_scheduler(pilot_config, alerter.client_state)

            if isinstance(adapter, GlobusComputePBSAdapter):
                try:
                    resp = adapter.client.get_endpoint_status(adapter.endpoint_id)
                    if resp.get("status") != "online":
                        obs.append(
                            Observation(
                                key=key,
                                status="offline",
                                summary=f"Cluster {c.name}: scheduler endpoint status={resp.get('status', 'unknown')}",
                                severity="crit",
                            )
                        )
                except Exception as e:
                    obs.append(
                        Observation(
                            key=key,
                            status="error",
                            summary=f"Cluster {c.name}: scheduler endpoint check failed: {str(e)[:200]}",
                            severity="crit",
                        )
                    )

            await adapter.get_job_statuses()
        except Exception as e:
            obs.append(
                Observation(
                    key=key,
                    status="error",
                    summary=f"Cluster {c.name}: scheduler unreachable: {str(e)[:200]}",
                    severity="crit",
                )
            )
    return obs


async def check_static_deployment(alerter: "HealthAlerter") -> list[Observation]:
    obs: list[Observation] = []
    async with alerter.client_state.db_sessionmaker() as sess:
        deps = await StaticDeployment.list(sess)
    for d in deps:
        if d.health == HealthCheckResult.unhealthy.value:
            obs.append(
                Observation(
                    key=f"staticdeployment/{d.uid}/health",
                    status="unhealthy",
                    summary=f"StaticDeployment {d.name}: health unhealthy",
                    severity="crit",
                )
            )
    return obs


# Pilot-deployment states worth alerting on, and their severity. States absent
# from this map (healthy, starting) are omitted; recovery is by absence.
_PILOT_DEPLOYMENT_SEVERITY: dict[str, Severity] = {
    PilotDeploymentState.failed.value: "crit",
    PilotDeploymentState.degraded.value: "warn",
    PilotDeploymentState.stopping.value: "info",
    PilotDeploymentState.awaiting_capacity.value: "info",
    PilotDeploymentState.offline.value: "info",
}


async def check_pilot_deployment(alerter: "HealthAlerter") -> list[Observation]:
    obs: list[Observation] = []
    async with alerter.client_state.db_sessionmaker() as sess:
        deps = (
            await sess.scalars(
                sa.select(PilotDeployment).options(
                    selectinload(PilotDeployment.replicas)
                )
            )
        ).all()

    for d in deps:
        sev = _PILOT_DEPLOYMENT_SEVERITY.get(d.state)
        if sev is not None:
            obs.append(
                Observation(
                    key=f"pilotdeployment/{d.uid}/state",
                    status=d.state,
                    summary=f"PilotDeployment {d.name}: state={d.state}",
                    severity=sev,
                )
            )

        active_replicas = [r for r in d.replicas if r.deleted_at is None]
        all_pending = bool(active_replicas) and all(
            r.state == ReplicaState.pending.value for r in active_replicas
        )
        any_at_capacity = any(r.state_message == AT_CAPACITY for r in active_replicas)
        if all_pending and any_at_capacity:
            obs.append(
                Observation(
                    key=f"pilotdeployment/{d.uid}/capacity",
                    status="replicas_awaiting_capacity",
                    summary=f"PilotDeployment {d.name}: all replicas awaiting cluster capacity",
                    severity="info",
                )
            )

    return obs


async def check_pilot_job(alerter: "HealthAlerter") -> list[Observation]:
    obs: list[Observation] = []
    async with alerter.client_state.db_sessionmaker() as sess:
        jobs = (
            await sess.scalars(sa.select(PilotJob).where(PilotJob.deleted_at.is_(None)))
        ).all()

    for j in jobs:
        if j.reconcile_failures > 0:
            err = (j.reconcile_last_error or "")[:300]
            obs.append(
                Observation(
                    key=f"pilotjob/{j.uid}/reconcile",
                    status=f"failures={j.reconcile_failures}",
                    summary=f"PilotJob {j.name}: {j.reconcile_failures} reconcile failures — {err}",
                    severity="crit",
                )
            )
        if j.manager_health == HealthCheckResult.unhealthy.value:
            since = (
                f" (since {j.manager_unhealthy_since})"
                if j.manager_unhealthy_since
                else ""
            )
            obs.append(
                Observation(
                    key=f"pilotjob/{j.uid}/health",
                    status="manager_unhealthy",
                    summary=f"PilotJob {j.name}: manager unhealthy{since}",
                    severity="crit",
                )
            )
        if j.idle_since is not None:
            obs.append(
                Observation(
                    key=f"pilotjob/{j.uid}/idle",
                    status="idle",
                    summary=f"PilotJob {j.name}: idle since {j.idle_since}",
                    severity="info",
                )
            )

    return obs


_BAD_REPLICA_STATES = {
    ReplicaState.unhealthy.value,
    ReplicaState.error.value,
    ReplicaState.start_timeout.value,
}


async def check_pilot_replica(alerter: "HealthAlerter") -> list[Observation]:
    obs: list[Observation] = []
    async with alerter.client_state.db_sessionmaker() as sess:
        replicas = (
            await sess.scalars(
                sa.select(PilotReplica).where(PilotReplica.deleted_at.is_(None))
            )
        ).all()

    for r in replicas:
        if r.state in _BAD_REPLICA_STATES:
            obs.append(
                Observation(
                    key=f"pilotreplica/{r.uid}/state",
                    status=r.state,
                    summary=f"PilotReplica {r.name}: {r.state} — {r.state_message or ''}",
                    severity="crit",
                )
            )
        if r.reconcile_failures > 0:
            err = (r.reconcile_last_error or "")[:300]
            obs.append(
                Observation(
                    key=f"pilotreplica/{r.uid}/reconcile",
                    status=f"failures={r.reconcile_failures}",
                    summary=f"PilotReplica {r.name}: {r.reconcile_failures} reconcile failures — {err}",
                    severity="crit",
                )
            )

    return obs


async def check_db_liveness(alerter: "HealthAlerter") -> list[Observation]:
    obs: list[Observation] = []
    try:
        async with alerter.client_state.db_sessionmaker() as sess:
            await sess.execute(sa.text("SELECT 1"))
    except Exception as e:
        obs.append(
            Observation(
                key="postgres",
                status="down",
                summary=f"Postgres unreachable: {str(e)[:200]}",
                severity="crit",
            )
        )
    try:
        await alerter.client_state.redis.ping()
    except Exception as e:
        obs.append(
            Observation(
                key="redis",
                status="down",
                summary=f"Redis unreachable: {str(e)[:200]}",
                severity="crit",
            )
        )
    return obs


def _disk_severity(use: int) -> Severity | None:
    if use > 90:
        return "crit"
    if use > 80:
        return "warn"
    if use > 70:
        return "info"
    return None


async def check_host(alerter: "HealthAlerter") -> list[Observation]:
    obs: list[Observation] = []

    gateway_url = alerter.client_state.settings.gateway_health_url
    try:
        resp = await alerter.http.get(gateway_url)
        if resp.status_code >= 300:
            obs.append(
                Observation(
                    key="gateway_health",
                    status="unreachable",
                    summary=f"Gateway /health returned {resp.status_code}",
                    severity="crit",
                )
            )
    except Exception as e:
        obs.append(
            Observation(
                key="gateway_health",
                status="unreachable",
                summary=f"Gateway /health unreachable: {str(e)[:200]}",
                severity="crit",
            )
        )

    try:
        resp = await alerter.http.get("http://127.0.0.1:9100/healthz")
        if resp.status_code == 503:
            obs.append(
                Observation(
                    key="controller_healthz",
                    status="stale",
                    summary=f"Controller /healthz: {resp.text[:200]}",
                    severity="crit",
                )
            )
    except Exception as e:
        obs.append(
            Observation(
                key="controller_healthz",
                status="stale",
                summary=f"Controller /healthz unreachable: {str(e)[:200]}",
                severity="crit",
            )
        )

    try:
        proc = await asyncio.create_subprocess_exec(
            "df",
            "-P",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, _ = await proc.communicate()
        for line in out.decode().splitlines()[1:]:
            fields = line.split()
            if len(fields) < 6:
                continue
            source = fields[0]
            if source in ("tmpfs", "devfs") or not fields[5].startswith("/"):
                continue
            try:
                use = int(fields[4].rstrip("%"))
            except ValueError:
                continue
            mount = fields[5]
            sev = _disk_severity(use)
            if sev is not None:
                obs.append(
                    Observation(
                        key=f"disk:{mount}",
                        status=f"{use}%",
                        summary=f"{mount} {use}% full",
                        severity=sev,
                    )
                )
    except Exception as e:
        logger.warning("df check failed: %s", e)

    return obs


@dataclass
class Check:
    func: Callable[["HealthAlerter"], Awaitable[list[Observation]]]
    group: str  # Slack category all this check's observations belong to


# Registry of all checks (function + Slack group). The group and the function
# name are stamped onto every Observation the check produces.
CHECKS = [
    Check(check_cluster_health, "Clusters"),
    Check(check_scheduler, "Clusters"),
    Check(check_static_deployment, "Deployments"),
    Check(check_pilot_deployment, "Deployments"),
    Check(check_pilot_job, "Pilot Jobs"),
    Check(check_pilot_replica, "Pilot Replicas"),
    Check(check_db_liveness, "Infrastructure"),
    Check(check_host, "Infrastructure"),
]


# ---------------------------------------------------------------------------
# Pure state machine (debounce). No I/O, no wall-clock — `now` is injected so
# this is unit-testable with plain dicts.
# ---------------------------------------------------------------------------


def advance(
    state: HealthAlertState,
    observed: dict[str, Observation],
    ran_checks: set[str],
    now: datetime,
    debounce: timedelta,
) -> FlushPlan:
    """Update `state.staging` for this tick and return the matured transitions.

    Mutates only `state.staging` (debounce bookkeeping). `state.committed` is
    left untouched — the caller commits it only after a successful Slack post,
    so a failed post is retried next tick with no double-send.
    """
    # Step A — candidate target status per key.
    candidates: dict[str, StagedTransition] = {}
    for key, o in observed.items():
        candidates[key] = StagedTransition(
            status=o.status,
            severity=o.severity,
            summary=o.summary,
            group=o.group,
            owner=o.owner,
            first_seen=now,
        )
    for key, ca in state.committed.items():
        # Recovery candidate: committed key gone from a check that actually ran.
        if key not in observed and ca.owner in ran_checks:
            candidates[key] = StagedTransition(
                status="",
                severity=ca.severity,
                group=ca.group,
                owner=ca.owner,
                first_seen=now,
            )

    # Step B — (re)stage: reset the debounce timer only when the target changes.
    for key, cand in candidates.items():
        existing = state.staging.get(key)
        if existing is None or existing.status != cand.status:
            state.staging[key] = cand

    # Drop entries that no longer represent a real transition:
    #  - flap back to what Slack already believes, or
    #  - a staged key its owning check ran but no longer observes and never committed.
    for key in list(state.staging.keys()):
        committed = state.committed.get(key)
        committed_status = committed.status if committed else ""
        staged = state.staging[key]
        if staged.status == committed_status:
            del state.staging[key]
        elif key not in observed and committed is None and staged.owner in ran_checks:
            del state.staging[key]

    # Step C — matured transitions (held steady past the debounce window).
    matured = [
        (k, s) for k, s in state.staging.items() if (now - s.first_seen) >= debounce
    ]
    return FlushPlan(
        degradations=[(k, s) for k, s in matured if s.status != ""],
        recoveries=[k for k, s in matured if s.status == ""],
    )


# ---------------------------------------------------------------------------
# HealthAlerter worker
# ---------------------------------------------------------------------------


class HealthAlerter(Worker):
    """Emits Slack health alerts (degradations, recoveries, daily digest)."""

    poll_interval = 30.0
    wakeup_channels: ClassVar[list[Any]] = []

    DEBOUNCE_S = 45.0
    CHECK_TIMEOUT_S = 30.0
    DAILY_HOUR_UTC = 13

    def __init__(
        self,
        name: str,
        client_state: ClientState,
        wakeup_dispatcher: WakeupDispatcher,
        *,
        restart_backoff: float = 1.0,
        max_backoff: float = 30.0,
        heartbeat_timeout: float = 120.0,
    ) -> None:
        super().__init__(
            name,
            client_state,
            wakeup_dispatcher,
            restart_backoff=restart_backoff,
            max_backoff=max_backoff,
            heartbeat_timeout=heartbeat_timeout,
        )
        self.http = httpx.AsyncClient(timeout=10.0)
        self._webhook_warned = False

    async def run(self) -> None:
        hb = self.register_heartbeat("poll")
        while True:
            hb.beat()
            await self.poll()
            await self.wait_for_wake()

    # ------------------------------------------------------------------
    # Check harness
    # ------------------------------------------------------------------

    async def _safe(self, check: Check) -> CheckResult:
        """Run one check, stamping owner + group onto each Observation."""
        name = check.func.__name__
        try:
            raw = await asyncio.wait_for(check.func(self), timeout=self.CHECK_TIMEOUT_S)
            observations = [replace(o, owner=name, group=check.group) for o in raw]
            return CheckResult(
                name, success=True, error_msg=None, observations=observations
            )
        except Exception as e:
            logger.exception("health check %s failed", name)
            return CheckResult(name, success=False, error_msg=str(e), observations=[])

    # ------------------------------------------------------------------
    # Slack posting
    # ------------------------------------------------------------------

    async def _post_slack(self, blocks: list[dict[str, Any]]) -> bool:
        url = self.client_state.settings.health_slack_webhook_url
        if not url:
            if not self._webhook_warned:
                logger.info(
                    "health_slack_webhook_url not configured; skipping Slack post"
                )
                self._webhook_warned = True
            return True
        try:
            resp = await self.http.post(url, json={"blocks": blocks})
            if 200 <= resp.status_code < 300:
                return True
            logger.warning(
                "Slack webhook returned %d: %s", resp.status_code, resp.text[:500]
            )
            return False
        except Exception:
            logger.warning("Slack webhook POST failed", exc_info=True)
            return False

    # ------------------------------------------------------------------
    # Main poll loop
    # ------------------------------------------------------------------

    async def poll(self, now: datetime | None = None) -> None:
        now = now or datetime.now(timezone.utc)

        # 1. Run all checks concurrently and collect observations.
        results = await asyncio.gather(*[self._safe(c) for c in CHECKS])
        ran_checks = {r.check_function for r in results if r.success}
        observed: dict[str, Observation] = {}
        for r in results:
            for o in r.observations:
                observed[o.key] = o

        # 2. Read state, advance the debounce machine.
        state = await self.client_state.redis_repo.get_health_alert_state()
        plan = advance(
            state, observed, ran_checks, now, timedelta(seconds=self.DEBOUNCE_S)
        )

        # 3. Flush matured transitions. Recoveries from info-level observations
        #    are cleared silently; only crit/warn recoveries get a message.
        visible_recoveries = [
            (k, state.staging[k])
            for k in plan.recoveries
            if state.staging[k].severity != "info"
        ]
        if plan.degradations or visible_recoveries:
            blocks = _build_alert_blocks(plan.degradations, visible_recoveries, [])
            if await self._post_slack(blocks):
                self._commit(state, plan)
        elif plan.recoveries:
            # Only silent info recoveries matured — commit without posting.
            self._commit(state, plan)

        # 4. Flush check-execution failures (new or changed error messages).
        await self._flush_failed_checks(state, results)

        # 5. Daily digest.
        today = now.strftime("%Y-%m-%d")
        if now.hour >= self.DAILY_HOUR_UTC and state.last_daily_report != today:
            blocks = await self._build_daily_digest(observed, state.committed)
            if await self._post_slack(blocks):
                state.last_daily_report = today

        # 6. Persist.
        await self.client_state.redis_repo.set_health_alert_state(state)

    @staticmethod
    def _commit(state: HealthAlertState, plan: FlushPlan) -> None:
        """Apply a flushed plan to committed state and clear its staging entries."""
        for key, staged in plan.degradations:
            state.committed[key] = CommittedAlert(
                status=staged.status,
                severity=staged.severity,
                group=staged.group,
                owner=staged.owner,
            )
        for key in plan.recoveries:
            state.committed.pop(key, None)
        for key, _ in plan.degradations:
            state.staging.pop(key, None)
        for key in plan.recoveries:
            state.staging.pop(key, None)

    async def _flush_failed_checks(
        self, state: HealthAlertState, results: list[CheckResult]
    ) -> None:
        to_report: list[tuple[str, str]] = []
        for r in results:
            if not r.success and r.error_msg:
                if state.reported_failures.get(r.check_function) != r.error_msg:
                    to_report.append((r.check_function, r.error_msg))
            elif r.success:
                state.reported_failures.pop(r.check_function, None)

        if to_report:
            blocks = _build_alert_blocks([], [], to_report)
            if await self._post_slack(blocks):
                for fn, msg in to_report:
                    state.reported_failures[fn] = msg

    # ------------------------------------------------------------------
    # Daily digest
    # ------------------------------------------------------------------

    async def _build_daily_digest(
        self, observed: dict[str, Observation], committed: dict[str, CommittedAlert]
    ) -> list[dict[str, Any]]:
        """Snapshot: per-group totals + this tick's open issues (reuses checks)."""
        async with self.client_state.db_sessionmaker() as sess:
            totals = {
                "Clusters": await _count(sess, Cluster),
                "Deployments": await _count(sess, StaticDeployment)
                + await _count(sess, PilotDeployment),
                "Pilot Jobs": await _count(sess, PilotJob, soft_deletable=True),
                "Pilot Replicas": await _count(sess, PilotReplica, soft_deletable=True),
            }

        issues_by_group: dict[str, int] = {}
        for o in observed.values():
            issues_by_group[o.group] = issues_by_group.get(o.group, 0) + 1

        resource_counts = {
            group: (total, issues_by_group.get(group, 0))
            for group, total in totals.items()
        }
        current_degradations = {k: ca.status for k, ca in committed.items()}
        return _build_digest_blocks(resource_counts, current_degradations)


async def _count(sess: Any, model: Any, *, soft_deletable: bool = False) -> int:
    stmt = sa.select(sa.func.count()).select_from(model)
    if soft_deletable:
        stmt = stmt.where(model.deleted_at.is_(None))
    return int((await sess.scalar(stmt)) or 0)
