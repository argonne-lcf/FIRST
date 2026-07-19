"""Pilot Autoscaler Controller.

Reconciles **per Model** and enacts scaling on each of that model's child
PilotDeployments. The demand signal is inherently per-model, so the reconcile
unit is the model: sample the shared signal once, then drive every child
deployment's own ladder from it.

Sole writer of `PilotDeployment.desired_replicas`, including pinning it to 0
for deployments whose launches keep failing.

With nothing launching, `consecutive_launch_failures` can never reset on its own, so this
clears only via operator action — a spec edit (`plan_apply` resets the counter)
or `alcf-ai admin reconcile-reset`.
"""

import logging
from datetime import datetime, timedelta, timezone

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from first_common.schema.resources.runtime import (
    RejectSample,
    ScaledownCandidate,
)
from first_common.schema.types import DemandSignalConfig, DemandThresholdStrategy

from ...database.models import Model, PilotDeployment
from ...database.redis.pubsub import Channel
from ..controller import Controller

logger = logging.getLogger(__name__)

# At desired_replicas == 0 the cold-start signal is reject-driven: a capacity
# rejection this recent counts as live demand.
COLD_START_REJECT_WINDOW = timedelta(minutes=5)


def update_reject_window(
    window: list[RejectSample],
    now: datetime,
    rejects_total: int,
    reject_window_sec: int,
) -> tuple[list[RejectSample], float]:
    """Append the current sample, drop stale ones, and return the new window
    together with the average reject rate (rejects/sec) over it.

    The rate uses the retained sample closest to ``reject_window_sec`` ago (the
    oldest retained one). ``Δrejects`` is clamped at 0 so a Redis flush/restore
    that resets the monotonic counter produces a rate of 0, not a negative spike.
    """
    cutoff = now - timedelta(seconds=reject_window_sec)
    retained = [s for s in window if s.ts >= cutoff]
    retained.append(RejectSample(ts=now, rejects_total=rejects_total))

    ref = retained[0]
    dt = (now - ref.ts).total_seconds()
    if dt <= 0:
        return retained, 0.0
    delta = max(0, rejects_total - ref.rejects_total)
    return retained, delta / dt


def update_ewma(prev: float, instantaneous: float, alpha: float) -> float:
    return alpha * instantaneous + (1 - alpha) * prev


def ladder_target(
    ewma: float,
    thresholds: list[tuple[float, int]],
    min_replicas: int,
    max_replicas: int,
) -> int:
    """Map an EWMA demand to a replica count via the threshold ladder.

    ``thresholds`` is ordered ``(demand_lower_bound_exclusive, num_replicas)``.
    Below the bottom rung → ``min_replicas``; above the top → the top rung's
    count. Every result is capped at ``max_replicas``.
    """
    if not thresholds or ewma <= thresholds[0][0]:
        return min_replicas
    target = min_replicas
    for lower_bound, num in thresholds:
        if ewma > lower_bound:
            target = num
        else:
            break
    return min(target, max_replicas)


def decide_scale(
    *,
    now: datetime,
    ewma: float,
    thresholds: list[tuple[float, int]],
    min_replicas: int,
    max_replicas: int,
    current_desired: int,
    candidates: list[ScaledownCandidate],
    sustain_sec: int,
) -> tuple[int, list[ScaledownCandidate]]:
    """Decide a deployment's desired_replicas and its updated scale-down
    candidate list from the shared EWMA demand.

    Scale-up is immediate. Scale-down is damped: a candidate must hold its rung
    for ``sustain_sec`` before it is enacted. If the EWMA lifts back above a
    candidate's rung, that candidate is dropped (its sustain clock resets).
    """
    target = ladder_target(ewma, thresholds, min_replicas, max_replicas)

    # Drop candidates whose rung the EWMA has lifted back above.
    candidates = [c for c in candidates if target <= c.num_replicas]

    if target > current_desired:
        # Scale up immediately — any pending scale-down candidates are now moot.
        return target, []

    if target < current_desired and not any(
        c.num_replicas == target for c in candidates
    ):
        # New candidate scale-down: start its sustain clock.
        candidates = candidates + [
            ScaledownCandidate(num_replicas=target, starting_from=now)
        ]

    eligible = [
        c
        for c in candidates
        if c.num_replicas < current_desired
        and (now - c.starting_from).total_seconds() >= sustain_sec
    ]
    if eligible:
        new_desired = min(c.num_replicas for c in eligible)
        # Clear candidates at or below the enacted level (enacted or superseded).
        candidates = [c for c in candidates if c.num_replicas < new_desired]
        return new_desired, candidates

    return current_desired, candidates


class PilotAutoscaler(Controller):
    resource_type = Model
    poll_interval = 10.0  # fixed sampling clock — the EWMA + reject window
    wakeup_channels = []  # assume a regular ~10s tick, so NO early wakes

    async def list_actionable(self, sess: AsyncSession) -> list[int]:
        # Every Model with at least one PilotDeployment. No reconcile_retry_at
        # gate: the work is pure compute + Redis and must run on a fixed clock.
        stmt = sa.select(Model.uid).where(
            sa.exists().where(PilotDeployment.model_name == Model.name)
        )
        return list(await sess.scalars(stmt))

    async def reconcile(self, uid: int) -> None:
        now = datetime.now(timezone.utc)

        async with self.client_state.db_sessionmaker() as sess:
            model = await sess.get(
                Model, uid, options=[selectinload(Model.pilot_deployments)]
            )
            if model is None:
                return

        signal_cfg = DemandSignalConfig.model_validate(model.demand_signal)

        # -- A. Sample the model's demand signal once --
        rt = await self.client_state.redis_repo.get_autoscaler_model_runtime(model.name)
        model_rt = await self.client_state.redis_repo.get_model_runtime(model.name)

        window, reject_rate = update_reject_window(
            rt.reject_window,
            now,
            model_rt.capacity_rejects_total,
            signal_cfg.reject_window_sec,
        )
        instantaneous = signal_cfg.calculate_demand(
            model_rt.total_inflight, reject_rate
        )
        ewma = update_ewma(rt.demand_ewma, instantaneous, signal_cfg.ewma_alpha)

        rt.reject_window = window
        rt.demand_ewma = ewma

        # -- B. Decide + write desired_replicas per child deployment --
        live_names: set[str] = set()

        for dep in model.pilot_deployments:
            live_names.add(dep.name)
            new_desired = self._decide_deployment(
                dep=dep,
                now=now,
                ewma=ewma,
                last_capacity_reject=model_rt.last_capacity_reject,
                candidates=rt.scale_down_candidates,
            )
            if new_desired != dep.desired_replicas:
                await self._write_desired(dep, new_desired)

        # Prune candidate state for deployments that no longer exist.
        rt.scale_down_candidates = {
            name: cs
            for name, cs in rt.scale_down_candidates.items()
            if name in live_names
        }

        await self.client_state.redis_repo.set_autoscaler_model_runtime(model.name, rt)

    def _decide_deployment(
        self,
        *,
        dep: PilotDeployment,
        now: datetime,
        ewma: float,
        last_capacity_reject: datetime | None,
        candidates: dict[str, list[ScaledownCandidate]],
    ) -> int:
        """Return the deployment's new desired_replicas, mutating ``candidates``
        (the per-model scale-down bookkeeping) as a side effect."""
        # 1. One-way latch: too many launch failures pins desired to 0.
        if dep.consecutive_launch_failures > dep.max_consecutive_launch_failures:
            candidates.pop(dep.name, None)
            return 0

        # 2. Manual scaling: leave desired_replicas alone.
        if dep.scaling_strategy is None:
            candidates.pop(dep.name, None)
            return dep.desired_replicas

        strategy = DemandThresholdStrategy.model_validate(dep.scaling_strategy)

        # 3. Cold start: jump off zero on the first sign of demand.
        if (
            dep.desired_replicas == 0
            and strategy.immediate_cold_start
            and last_capacity_reject is not None
            and now - last_capacity_reject < COLD_START_REJECT_WINDOW
        ):
            candidates.pop(dep.name, None)
            return max(
                1,
                ladder_target(
                    ewma,
                    strategy.scaling_thresholds,
                    dep.min_replicas,
                    dep.max_replicas,
                ),
            )

        # 4. Ladder + scale-down sustain.
        new_desired, new_candidates = decide_scale(
            now=now,
            ewma=ewma,
            thresholds=strategy.scaling_thresholds,
            min_replicas=dep.min_replicas,
            max_replicas=dep.max_replicas,
            current_desired=dep.desired_replicas,
            candidates=candidates.get(dep.name, []),
            sustain_sec=strategy.scale_down_sustain_sec,
        )
        if new_candidates:
            candidates[dep.name] = new_candidates
        else:
            candidates.pop(dep.name, None)
        return new_desired

    async def _write_desired(self, dep: PilotDeployment, new_desired: int) -> None:
        """Premised UPDATE of one deployment's desired_replicas. A stale premise
        is logged and skipped (the next tick re-reads) rather than failing the
        whole model's reconcile."""
        async with self.client_state.db_sessionmaker.begin() as sess:
            result = await sess.execute(
                sa.update(PilotDeployment)
                .where(
                    PilotDeployment.uid == dep.uid,
                    PilotDeployment.desired_replicas == dep.desired_replicas,
                    PilotDeployment.consecutive_launch_failures
                    == dep.consecutive_launch_failures,
                )
                .values(desired_replicas=new_desired)
            )
        if result.rowcount == 0:  # type: ignore[attr-defined]
            logger.warning(
                "%s: deployment %s desired_replicas premise stale "
                "(desired=%d, failures=%d); retrying next tick",
                self.name,
                dep.name,
                dep.desired_replicas,
                dep.consecutive_launch_failures,
            )
            return

        logger.info(
            "%s: deployment %s desired_replicas %d -> %d",
            self.name,
            dep.name,
            dep.desired_replicas,
            new_desired,
        )
        await self.client_state.redis_pubsub.publish(
            Channel.desired_replicas_changed, dep.name
        )
