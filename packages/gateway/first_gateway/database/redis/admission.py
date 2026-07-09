from __future__ import annotations

import json
import time
from enum import Enum
from pathlib import Path
from typing import Any, Optional, Sequence

from pydantic import BaseModel
from redis.asyncio import Redis

from first_common.schema.types import RouterParams, UsageLimits

from .keys import Keys

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
    """Decoded return of the admit.lua script."""

    status: AdmitStatus
    replica_id: str | None = None
    quota_reason: QuotaReason | None = None
    capacity_reason: CapacityReason | None = None
    retry_after_s: float | None = None

    @property
    def admitted(self) -> bool:
        return self.status is AdmitStatus.ADMITTED

    @classmethod
    def from_lua(cls, raw: Sequence[Any]) -> "AdmitResult":
        code = int(raw[0])
        if code == AdmitStatus.ADMITTED:
            return cls(status=AdmitStatus.ADMITTED, replica_id=to_str(raw[1]))
        if code == AdmitStatus.REJECT_QUOTA:
            retry_after = float(raw[2])
            return cls(
                status=AdmitStatus.REJECT_QUOTA,
                quota_reason=QuotaReason(to_str(raw[1])),
                retry_after_s=retry_after if retry_after >= 0 else None,
            )
        return cls(
            status=AdmitStatus.REJECT_CAPACITY,
            capacity_reason=CapacityReason(to_str(raw[1])),
        )


class CandidateReplica(BaseModel):
    """One entry of the ordered candidate list, to be checked for capacity."""

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
        random-sampled order the router chose.
        """
        keys: list[str] = [
            Keys.quota(model_name, user_id, "tokens"),
            Keys.quota(model_name, user_id, "rpm"),
            Keys.quota(model_name, user_id, "inflight"),
            Keys.model_inflight(model_name),
            Keys.model_demand(model_name),
            Keys.deadlines(),
            Keys.reservation(request_id),
        ]
        for c in candidates:
            keys.append(Keys.replica_errors(c.uid))

        args: list[str | int | float] = [
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
        ]
        for c in candidates:
            args.extend([c.uid, c.max_replica_concurrency, c.cooldown_threshold])

        raw = await self._admit(keys=keys, args=args)
        return AdmitResult.from_lua(raw)

    # -- settlement (the idempotent compensator) ------------------------------
    async def settle(
        self,
        request_id: str,
        actual_tokens: Optional[int] = None,
        *,
        model_name: str | None = None,
        user_id: str | None = None,
    ) -> bool:
        """Reverse one reservation's effects.  Safe to call from the request
        `finally`, the sweeper, and retries simultaneously; returns True iff
        this call was the one that applied.

        Pass model_name and user_id from the request context to skip the
        pre-read round trip on the hot path.  The sweeper omits them and
        pays one extra GET to discover the reservation's identity."""
        reservation_key = Keys.reservation(request_id)

        if model_name is None or user_id is None:
            raw_reservation = await self.client.get(reservation_key)
            if raw_reservation is None:
                await self.client.zrem(Keys.deadlines(), request_id)
                return False
            row = json.loads(raw_reservation)
            model_name = row["model_name"]
            user_id = row["user_id"]

        raw = await self._settle(
            keys=[
                reservation_key,
                Keys.deadlines(),
                Keys.model_inflight(model_name),
                Keys.quota(model_name, user_id, "inflight"),
                Keys.quota(model_name, user_id, "tokens"),
                Keys.model_demand(model_name),
            ],
            args=[
                "" if actual_tokens is None else int(actual_tokens),
                request_id,
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
            keys: list[str] = [Keys.deadlines()]
            keys.extend(Keys.reservation(rid) for rid in chunk)
            renewed += int(
                await self._renew(
                    keys=keys,
                    args=[self.lease_s, self.max_stream_s, *chunk],
                )
            )
        return renewed

    async def sweep(self, batch: int = 100) -> int:
        """Settle reservations whose lease lapsed (crashed worker, stuck
        handler past max_stream_s).  Lock-free: settle's idempotency makes
        concurrent sweeps merely wasteful, never wrong.  Any worker may run
        this opportunistically on a timer."""
        now = time.time()
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
        async for key in self.client.scan_iter(
            match=Keys.reservation_scan_pattern(), count=200
        ):
            raw = await self.client.get(key)
            if not raw:
                continue
            try:
                row = json.loads(raw)
                model = row["model_name"]
                replica = row["replica_id"]
                counts.setdefault(model, {}).setdefault(replica, 0)
                counts[model][replica] += 1
            except (ValueError, KeyError):
                continue
        return counts
