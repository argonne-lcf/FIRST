# ruff: noqa
# mypy: ignore-errors
# TODO: fix this file when ready to implement redis state readers; ignore for now.
"""Redis state layer"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Optional, Sequence

from pydantic import BaseModel, Field
from redis.asyncio import Redis

__all__ = [
    "Keys",
    "CONFIG_CHANNEL",
    "AdmitStatus",
    "QuotaReason",
    "CapacityReason",
    "AdmitResult",
    "CandidateReplica",
    "QuotaLimits",
    "CooldownPolicy",
    "ScalingPolicy",
    "OverloadPolicy",
    "QuotaPolicy",
    "ReplicaConfig",
    "DeploymentConfig",
    "ModelConfig",
    "RouterConfig",
    "DemandSnapshot",
    "RedisBatch",
    "ReplicaStatus",
    "ModelStatus",
    "GatewayStore",
]


# --------------------------------------------------------------------------
# Runtime status views (validated reads of rt:* state).
#
# Reader architecture: stage/parse over a shared pipeline.
#   * stage(batch, ...) appends this object's commands to the batch's pipeline
#     and registers a parser for its slice of the results.
#   * RedisBatch.execute() runs ONE round trip and feeds each parser its slice.
# `read` is a one-item batch; `read_many` a homogeneous batch; heterogeneous
# reads (replicas + models together) compose on one RedisBatch.
#
# Deliberately not an ORM: fields here live in other objects' hashes (inflight
# is a field of the MODEL-level hash), in TTLs (bench_remaining_s), or nowhere
# (benched is derived from counter x config).  Attribute<->key magic cannot
# express any of those; explicit staging can, and keeps round trips visible.
#
# Consistency note: a pipeline is not a transaction.  These are advisory
# views and may be microseconds-incoherent across fields; every invariant
# read/write lives in the Lua scripts, never here.
# --------------------------------------------------------------------------


class RedisBatch:
    """Composes staged reads from any status classes into one pipeline
    round trip.  Usage:

        batch = RedisBatch(client)
        h1 = ReplicaStatus.stage(batch, "llama", "pbs-991123", cooldown)
        h2 = ModelStatus.stage(batch, "llama")
        await batch.execute()
        replica, model = h1.value, h2.value
    """

    class Handle:
        __slots__ = ("value",)

        def __init__(self) -> None:
            self.value = None

    def __init__(self, client: Redis) -> None:
        self.pipe = client.pipeline(transaction=False)
        self._jobs: list[tuple[int, "RedisBatch.Handle", object]] = []

    def register(self, n_cmds: int, parse) -> "RedisBatch.Handle":
        """Called by stage() implementations after appending n_cmds commands."""
        handle = RedisBatch.Handle()
        self._jobs.append((n_cmds, handle, parse))
        return handle

    async def execute(self) -> None:
        rows = await self.pipe.execute()
        i = 0
        for n, handle, parse in self._jobs:
            handle.value = parse(rows[i : i + n])
            i += n


class ReplicaStatus(BaseModel):
    model: str
    replica_id: str
    inflight: int = 0
    error_count: int = 0
    benched: bool = False
    bench_remaining_s: Optional[float] = None  # TTL of the error key while benched

    # -- stage/parse ---------------------------------------------------------
    @classmethod
    def stage(
        cls,
        batch: RedisBatch,
        model: str,
        replica_id: str,
        cooldown: CooldownPolicy,
    ) -> RedisBatch.Handle:
        """Append this replica's 4 reads to the batch; parse from the slice."""
        p = batch.pipe
        p.hget(Keys.inflight(model), replica_id)
        p.get(Keys.replica_errors(replica_id))
        p.ttl(Keys.replica_errors(replica_id))
        return batch.register(
            3,
            lambda rows: cls._from_rows(
                model,
                replica_id,
                cooldown,
                inflight=rows[0],
                errors=rows[1],
                ttl=rows[2],
            ),
        )

    @classmethod
    def _from_rows(
        cls,
        model: str,
        replica_id: str,
        cooldown: CooldownPolicy,
        *,
        inflight,
        errors,
        ttl,
    ) -> "ReplicaStatus":
        error_count = int(errors or 0)
        benched = cooldown.threshold > 0 and error_count >= cooldown.threshold
        return cls(
            model=model,
            replica_id=replica_id,
            inflight=int(inflight or 0),
            error_count=error_count,
            benched=benched,
            bench_remaining_s=float(ttl)
            if (benched and ttl and int(ttl) > 0)
            else None,
        )

    # -- reads ----------------------------------------------------------------
    @classmethod
    async def read(
        cls, client: Redis, model: str, replica_id: str, cooldown: CooldownPolicy
    ) -> "ReplicaStatus":
        return (await cls.read_many(client, model, {replica_id: cooldown}))[replica_id]

    @classmethod
    async def read_many(
        cls, client: Redis, model: str, cooldowns: dict[str, CooldownPolicy]
    ) -> dict[str, "ReplicaStatus"]:
        """Bulk read for replicas of ONE model, with the shared-hash read
        hoisted: one HGETALL of the model inflight hash replaces N HGETs —
        3 commands per replica + 1, in a single round trip."""
        pipe = client.pipeline(transaction=False)
        pipe.hgetall(Keys.inflight(model))
        rids = list(cooldowns)
        for rid in rids:
            pipe.get(Keys.replica_errors(rid))
            pipe.ttl(Keys.replica_errors(rid))
        rows = await pipe.execute()
        inflight_all = rows[0] or {}
        out: dict[str, ReplicaStatus] = {}
        i = 1
        for rid in rids:
            out[rid] = cls._from_rows(
                model,
                rid,
                cooldowns[rid],
                inflight=inflight_all.get(rid),
                errors=rows[i],
                ttl=rows[i + 1],
            )
            i += 3
        return out

    async def clear_errors(self, client: Redis) -> None:
        """Operator escape hatch: manually un-bench an incarnation."""
        await client.delete(Keys.replica_errors(self.replica_id))
        self.error_count = 0
        self.benched = False
        self.bench_remaining_s = None


class DemandSnapshot(BaseModel):
    """Autoscaler-facing facts: gauge + monotonic counter + last-event ts.
    The scaler diffs `capacity_rejects_total` over any window it likes; the
    `replicas == 0 and last_reject_ts within horizon` rule drives scale-from-zero."""

    inflight: int = 0
    capacity_rejects_total: int = 0
    last_reject_ts: Optional[float] = None


class ModelStatus(BaseModel):
    model: str
    per_replica_inflight: dict[str, int] = Field(default_factory=dict)
    demand: DemandSnapshot = Field(default_factory=DemandSnapshot)

    @property
    def inflight_total(self) -> int:
        return sum(self.per_replica_inflight.values())

    # -- stage/parse ---------------------------------------------------------
    @classmethod
    def stage(cls, batch: RedisBatch, model: str) -> RedisBatch.Handle:
        batch.pipe.hgetall(Keys.inflight(model))
        batch.pipe.hgetall(Keys.demand(model))
        return batch.register(
            2, lambda rows: cls._from_rows(model, inflight=rows[0], demand=rows[1])
        )

    @classmethod
    def _from_rows(cls, model: str, *, inflight, demand) -> "ModelStatus":
        demand = demand or {}
        return cls(
            model=model,
            per_replica_inflight={k: int(v) for k, v in (inflight or {}).items()},
            demand=DemandSnapshot(
                inflight=int(demand.get("inflight", 0) or 0),
                capacity_rejects_total=int(
                    demand.get("capacity_rejects_total", 0) or 0
                ),
                last_reject_ts=_opt_float(demand.get("last_reject_ts")),
            ),
        )

    # -- reads ----------------------------------------------------------------
    @classmethod
    async def read(cls, client: Redis, model: str) -> "ModelStatus":
        return (await cls.read_many(client, [model]))[model]

    @classmethod
    async def read_many(
        cls, client: Redis, models: Sequence[str]
    ) -> dict[str, "ModelStatus"]:
        """One pipeline round trip for N models — the autoscaler's poll loop."""
        batch = RedisBatch(client)
        handles = {m: cls.stage(batch, m) for m in models}
        await batch.execute()
        return {m: h.value for m, h in handles.items()}

    # -- writer: the reconciler's REFRESH MATERIALIZED VIEW ------------------
    async def reconcile_inflight(
        self, client: Redis, true_counts: dict[str, int]
    ) -> None:
        """Overwrite cached counters from ledger-derived truth.  Converts any
        drift into bounded staleness.  Also HDELs fields for replicas that no
        longer exist (hash fields do not TTL away on their own)."""
        pipe = client.pipeline(transaction=False)
        stale = [rid for rid in self.per_replica_inflight if rid not in true_counts]
        if stale:
            pipe.hdel(Keys.inflight(self.model), *stale)
        if true_counts:
            pipe.hset(
                Keys.inflight(self.model),
                mapping={k: v for k, v in true_counts.items()},
            )
        pipe.hset(Keys.demand(self.model), "inflight", sum(true_counts.values()))
        await pipe.execute()
        self.per_replica_inflight = dict(true_counts)
        self.demand.inflight = sum(true_counts.values())


def _opt_float(v) -> Optional[float]:
    if v is None or v == "":
        return None
    return float(v)
