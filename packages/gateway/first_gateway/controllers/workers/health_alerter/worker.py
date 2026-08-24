import asyncio
import logging
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any, ClassVar

import httpx
import sqlalchemy as sa

from first_common.schema.resources.runtime import (
    CommittedAlert,
    HealthAlertState,
    StagedTransition,
)
from first_gateway.controllers.wakeup import WakeupDispatcher
from first_gateway.controllers.worker import Worker
from first_gateway.controllers.workers.health_alerter.checks import (
    CHECK_REGISTRY,
    Check,
)
from first_gateway.controllers.workers.health_alerter.slack import (
    build_alert_blocks,
    build_digest_blocks,
)
from first_gateway.controllers.workers.health_alerter.types import (
    CheckResult,
    FlushPlan,
    Observation,
)
from first_gateway.database.models import (
    Cluster,
    PilotDeployment,
    PilotJob,
    PilotReplica,
    StaticDeployment,
)
from first_gateway.settings import ClientState

logger = logging.getLogger(__name__)


async def _count(sess: Any, model: Any, *, soft_deletable: bool = False) -> int:
    """Count rows for daily digest"""
    stmt = sa.select(sa.func.count()).select_from(model)
    if soft_deletable:
        stmt = stmt.where(model.deleted_at.is_(None))
    return int((await sess.scalar(stmt)) or 0)


def advance(
    state: HealthAlertState,
    observed: list[Observation],
    ran_checks: set[str],
    now: datetime,
    debounce: timedelta,
) -> FlushPlan:
    """Update `state.staging` for this tick and return the matured transitions.

    Mutates only `state.staging` (debounce bookkeeping). `state.committed` is
    left untouched — the caller commits it only after a successful Slack post,
    so a failed post is retried next tick with no double-send.
    """
    observed_keys = {obs.key for obs in observed}

    # Populate all current observed issues
    candidates = {
        o.key: StagedTransition(
            key=o.key,
            status=o.status,
            severity=o.severity,
            summary=o.summary,
            display_name=o.display_name,
            recovery_hint=o.recovery_hint,
            group=o.group,
            owner=o.owner,
            debounce_s=o.debounce_s,
            first_seen=now,
        )
        for o in observed
    }

    # Populate recoveries (check ran successfully & did not observe issue).
    # Carry the committed alert's human context so the recovery line reads
    # "{resource} recovered after {recovery_hint}" rather than a bare key.
    for key, ca in state.committed.items():
        if key not in observed_keys and ca.owner in ran_checks:
            candidates[key] = StagedTransition(
                key=key,
                status="",
                severity=ca.severity,
                display_name=ca.display_name,
                recovery_hint=ca.recovery_hint,
                group=ca.group,
                owner=ca.owner,
                debounce_s=ca.debounce_s,
                first_seen=now,
            )

    # Stage all new issues, changed statuses, recoveries. A benign change to
    # summary/recovery_hint alone (same status) refreshes the staged detail in
    # place without resetting the debounce timer, so a rising retry count does
    # not re-alert.
    for key, cand in candidates.items():
        existing = state.staging.get(key)
        if existing is None or existing.status != cand.status:
            state.staging[key] = cand
        else:
            state.staging[key] = cand.model_copy(
                update={"first_seen": existing.first_seen}
            )

    # Unstage entries that no longer represent a real transition:
    for key in list(state.staging):
        staged = state.staging[key]
        committed = state.committed.get(key)
        committed_status = committed.status if committed else ""
        if staged.status == committed_status:
            # Flapped back to what Slack already believes. If the underlying
            # resource is still observed (same status, but e.g. a higher retry
            # count), refresh the committed human context so a later recovery
            # line reports the latest detail — without re-alerting.
            if committed is not None and key in observed_keys:
                committed.summary = staged.summary
                committed.recovery_hint = staged.recovery_hint
            del state.staging[key]
        elif (
            key not in observed_keys
            and committed is None
            and staged.owner in ran_checks
        ):
            # The problem resolved itself before we alerted Slack
            del state.staging[key]

    # Return matured transitions (held steady past the debounce window). A
    # transition may set its own debounce (e.g. flappy health checks use a
    # longer window); otherwise the worker default applies.
    def _matured(s: StagedTransition) -> bool:
        window = timedelta(seconds=s.debounce_s) if s.debounce_s else debounce
        return (now - s.first_seen) >= window

    matured = [s for s in state.staging.values() if _matured(s)]
    return FlushPlan(
        degradations=[s for s in matured if s.status != ""],
        recoveries=[s for s in matured if s.status == ""],
    )


class HealthAlerter(Worker):
    """Emits Slack health alerts (degradations, recoveries, daily digest)."""

    poll_interval = 30.0
    wakeup_channels: ClassVar[list[Any]] = []

    DEBOUNCE_S = 110.0
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

    async def _safe(self, check: Check) -> CheckResult:
        """Run one check, stamping owner + group onto each Observation."""
        name = check.func.__name__
        try:
            raw = await asyncio.wait_for(
                check.func(self.client_state), timeout=self.CHECK_TIMEOUT_S
            )
            observations = [replace(o, owner=name, group=check.group) for o in raw]
            return CheckResult(
                name, success=True, error_msg=None, observations=observations
            )
        except Exception as e:
            logger.exception("health check %s failed", name)
            return CheckResult(name, success=False, error_msg=str(e), observations=[])

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

    async def poll(self, now: datetime | None = None) -> None:
        now = now or datetime.now(timezone.utc)

        # 1. Run all checks concurrently and collect observations.
        results = await asyncio.gather(*[self._safe(c) for c in CHECK_REGISTRY])
        ran_checks = {r.check_function for r in results if r.success}
        observed = [obs for r in results for obs in r.observations]

        # 2. Read state, advance the debounce machine.
        state = await self.client_state.redis_repo.get_health_alert_state()
        plan = advance(
            state, observed, ran_checks, now, timedelta(seconds=self.DEBOUNCE_S)
        )

        # 3. Flush matured transitions. info-level transitions (offline, idle)
        # are digest-only: they are committed so the daily digest reflects them,
        # but never posted in real time. Only warn/crit transitions ping Slack.
        visible_degradations = [d for d in plan.degradations if d.severity != "info"]
        visible_recoveries = [r for r in plan.recoveries if r.severity != "info"]
        if visible_degradations or visible_recoveries:
            blocks = build_alert_blocks(visible_degradations, visible_recoveries, [])
            if await self._post_slack(blocks):
                self._commit(state, plan)
        elif plan.degradations or plan.recoveries:
            # Only silent info transitions matured — commit without posting.
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
        for staged in plan.recoveries:
            state.staging.pop(staged.key, None)
            state.committed.pop(staged.key, None)

        for staged in plan.degradations:
            state.staging.pop(staged.key, None)
            state.committed[staged.key] = CommittedAlert(
                key=staged.key,
                status=staged.status,
                severity=staged.severity,
                summary=staged.summary,
                display_name=staged.display_name,
                recovery_hint=staged.recovery_hint,
                group=staged.group,
                owner=staged.owner,
                debounce_s=staged.debounce_s,
            )

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
            blocks = build_alert_blocks([], [], to_report)
            if await self._post_slack(blocks):
                for fn, msg in to_report:
                    state.reported_failures[fn] = msg

    async def _build_daily_digest(
        self, observed: list[Observation], committed: dict[str, CommittedAlert]
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
        for o in observed:
            issues_by_group[o.group] = issues_by_group.get(o.group, 0) + 1

        resource_counts = {
            group: (total, issues_by_group.get(group, 0))
            for group, total in totals.items()
        }
        current_degradations = {
            k: (ca.summary or f"{ca.display_name or k}: {ca.status}")
            for k, ca in committed.items()
        }
        return build_digest_blocks(resource_counts, current_degradations)
