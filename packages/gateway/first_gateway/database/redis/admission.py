from __future__ import annotations

import time
from enum import Enum
from pathlib import Path
from typing import Any, Optional, Sequence

from pydantic import BaseModel
from redis.asyncio import Redis

from first_common.schema.types import RouterParams, UsageLimits

from .keys import QUOTA_PREFIX, RESERVE_PREFIX, RT_PREFIX, Keys

LUA_DIR = Path(__file__).parent / "lua"
_ADMIT_LUA = (LUA_DIR / "admit.lua").read_text()
_SETTLE_LUA = (LUA_DIR / "settle.lua").read_text()
_RENEW_LUA = (LUA_DIR / "renew.lua").read_text()
_RECORD_ERROR_LUA = (LUA_DIR / "record_error.lua").read_text()


def to_str(v: Any) -> str:
    return v.decode() if isinstance(v, (bytes, bytearray)) else str(v)


class AdmitStatus(int, Enum):
    ADMITTED = 1
    REJECT_QUOTA = 2
    REJECT_CAPACITY = 3


class QuotaReason(str, Enum):
    USER_CONCURRENCY = "user_concurrency"
    USER_RPM = "user_rpm"
    USER_TPM = "user_tpm"


class CapacityReason(str, Enum):
    SATURATED = "saturated"
    ALL_BENCHED = "all_benched"
    NO_CANDIDATES = "no_candidates"


class AdmitResult(BaseModel):
    """Decoded return of the admit.lua script"""

    status: AdmitStatus
    replica_id: str | None = None
    quota_reason: QuotaReason | None = None
    capacity_reason: CapacityReason | None = None
    retry_after_s: float | None = None

    @property
    def admitted(self) -> bool:
        return self.status is AdmitStatus.ADMITTED

    @classmethod
    def from_lua(cls, raw: Sequence) -> "AdmitResult":
        code = int(raw[0])
        if code == AdmitStatus.ADMITTED:
            return cls(status=AdmitStatus.ADMITTED, replica_id=to_str(raw[1]))
        if code == AdmitStatus.REJECT_QUOTA:
            ms = int(raw[2])
            return cls(
                status=AdmitStatus.REJECT_QUOTA,
                quota_reason=QuotaReason(to_str(raw[1])),
                retry_after_s=(ms / 1000.0) if ms >= 0 else None,
            )
        return cls(
            status=AdmitStatus.REJECT_CAPACITY,
            capacity_reason=CapacityReason(to_str(raw[1])),
        )


class CandidateReplica(BaseModel):
    """
    One entry of the ordered candidate list, to be checked for capacity.
    """

    uid: str
    max_replica_concurrency: int
    cooldown_threshold: int


class AdmissionController:
    """
    Owns the Lua script inventory (admit, settle, renew, record_error) and
    the sweep loop.  These four entry points are the only writers of
    ledger-governed state; everything else in the process must go through them.
    """

    def __init__(
        self,
        client: Redis,
        *,
        lease_s: int = 30,
        max_stream_s: int = 3600,
        renew_chunk: int = 500,
    ) -> None:
        self.client = client
        self.lease_s = lease_s
        self.max_stream_s = max_stream_s
        self.renew_chunk = renew_chunk
        self._admit = client.register_script(_ADMIT_LUA)
        self._settle = client.register_script(_SETTLE_LUA)
        self._renew = client.register_script(_RENEW_LUA)
        self._record_error = client.register_script(_RECORD_ERROR_LUA)

    # -- admission -----------------------------------------------------------
    async def admit(
        self,
        *,
        request_id: str,
        model_name: str,
        user_id: str,
        candidates: Sequence[CandidateReplica],
        estimated_tokens: int,
        quota: UsageLimits,
    ) -> AdmitResult:
        """
        One atomic round trip: quota checks once, then walk `candidates` in the
        random-sampled order the router chose
        """
        args: list = [
            request_id,
            model_name,
            user_id,
            estimated_tokens,
            quota.max_user_concurrency,
            quota.tokens_per_sec,
            quota.burst_tokens,
            quota.requests_per_sec,
            quota.burst_requests,
            self.lease_s,
            RESERVE_PREFIX,
            RT_PREFIX,
        ]
        for c in candidates:
            args.extend([c.uid, c.max_replica_concurrency, c.cooldown_threshold])
        raw = await self._admit(
            keys=[
                Keys.quota(model_name, user_id, "tokens"),
                Keys.quota(model_name, user_id, "rpm"),
                Keys.quota(model_name, user_id, "inflight"),
                Keys.inflight(model_name),
                Keys.demand(model_name),
                Keys.deadlines(),
            ],
            args=args,
        )
        return AdmitResult.from_lua(raw)

    # -- settlement (the idempotent compensator) ------------------------------
    async def settle(
        self, request_id: str, actual_tokens: Optional[int] = None
    ) -> bool:
        """Reverse one reservation's effects.  Safe to call from the request
        `finally`, the sweeper, and retries simultaneously; returns True iff
        this call was the one that applied."""
        raw = await self._settle(
            keys=[Keys.deadlines()],
            args=[
                request_id,
                "" if actual_tokens is None else int(actual_tokens),
                RT_PREFIX,
                QUOTA_PREFIX,
                RESERVE_PREFIX,
            ],
        )
        return bool(int(raw[0]))

    # -- leases ---------------------------------------------------------------
    async def renew(self, request_ids: Sequence[str]) -> int:
        """Batched, chunked lease renewal for this worker's in-process registry.
        Call every ~lease_s/3 seconds with ALL live request ids."""
        renewed = 0
        for i in range(0, len(request_ids), self.renew_chunk):
            chunk = list(request_ids[i : i + self.renew_chunk])
            renewed += int(
                await self._renew(
                    keys=[Keys.deadlines()],
                    args=[self.lease_s, self.max_stream_s, RESERVE_PREFIX, *chunk],
                )
            )
        return renewed

    async def sweep(self, batch: int = 100) -> int:
        """Settle reservations whose lease lapsed (crashed worker, stuck
        handler past max_stream_s).  Lock-free: settle's idempotency makes
        concurrent sweeps merely wasteful, never wrong.  Any worker may run
        this opportunistically on a timer."""
        now = (
            time.time()
        )  # cutoff only; per-row decisions use server TIME inside settle
        expired = await self.client.zrangebyscore(
            Keys.deadlines(), "-inf", now, start=0, num=batch
        )
        settled = 0
        for rid in expired:
            if await self.settle(to_str(rid), actual_tokens=None):
                settled += 1
        return settled

    # -- reachability ----------------------------------------------------------
    async def record_error(
        self, replica_id: str, router_params: RouterParams
    ) -> tuple[int, bool]:
        """Register an upstream failure; returns (count, benched).  The counter
        IS the cooldown: admit treats count >= threshold as benched, and the
        TTL (window, re-armed to bench_s at threshold) is the un-bench."""
        raw = await self._record_error(
            keys=[Keys.replica_errors(replica_id)],
            args=[
                router_params.cooldown_window_sec,
                router_params.cooldown_threshold,
                router_params.cooldown_bench_sec,
            ],
        )
        return int(raw[0]), bool(int(raw[1]))

    # -- reconciliation ----------------------------------------------------------
    async def rebuild_inflight_from_ledger(self) -> dict[str, dict[str, int]]:
        """Recompute per-model, per-replica inflight from the reservation
        ledger (the base table).  Feed the result to
        ModelStatus.reconcile_inflight per model.  Uses SCAN, never KEYS."""
        counts: dict[str, dict[str, int]] = {}
        async for key in self.client.scan_iter(match=f"{RESERVE_PREFIX}*", count=200):
            k = to_str(key)
            if k == Keys.deadlines():
                continue
            raw = await self.client.get(k)
            if not raw:
                continue
            try:
                import json

                row = json.loads(raw)
                counts.setdefault(row["model"], {}).setdefault(row["replica_id"], 0)
                counts[row["model"]][row["replica_id"]] += 1
            except (ValueError, KeyError):
                continue
        return counts
