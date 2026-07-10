import json
import logging
import time
from enum import Enum
from pathlib import Path
from typing import Any, NamedTuple, Optional, Sequence

from pydantic import BaseModel
from redis.asyncio import Redis

from first_common.schema.types import RouterParams, UsageLimits

from .keys import Keys

logger = logging.getLogger(__name__)

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
    backend_id: str | None = None
    quota_reason: QuotaReason | None = None
    capacity_reason: CapacityReason | None = None
    retry_after_sec: float | None = None

    @property
    def admitted(self) -> bool:
        return self.status is AdmitStatus.ADMITTED

    @classmethod
    def from_lua(cls, raw: Sequence[Any]) -> "AdmitResult":
        code = int(raw[0])
        if code == AdmitStatus.ADMITTED:
            return cls(status=AdmitStatus.ADMITTED, backend_id=to_str(raw[1]))
        if code == AdmitStatus.REJECT_QUOTA:
            retry_after = float(raw[2])
            return cls(
                status=AdmitStatus.REJECT_QUOTA,
                quota_reason=QuotaReason(to_str(raw[1])),
                retry_after_sec=retry_after if retry_after >= 0 else None,
            )
        return cls(
            status=AdmitStatus.REJECT_CAPACITY,
            capacity_reason=CapacityReason(to_str(raw[1])),
        )


class CandidateBackend(BaseModel):
    """One entry of the ordered candidate list, to be checked for capacity."""

    uid: str
    max_backend_concurrency: int
    cooldown_threshold: int


class InflightCounts(NamedTuple):
    """
    Inflight model-grouped utilization counts.

    - by_backend provides a count for each active backend
    - by_user provides a count for each active user
    """

    by_backend: dict[str, dict[str, int]]
    by_user: dict[str, dict[str, int]]


class AdmissionController:
    """
    Controller for distributed request admission: performs quota bookkeeping
    (RPM, TPM, Concurrency) per user/model and backend capacity bookkeeping (
    cooldown state, per-backend concurrency).

    Owns the Lua script inventory (admit, settle, renew, record_error) and the
    sweep loop.
    """

    def __init__(
        self,
        client: Redis,
        *,
        lease_sec: float = 30.0,
        max_request_sec: float = 3600.0,
        renew_chunk: int = 500,
    ) -> None:
        """
        - lease_sec: default reservation duration
        - max_request_sec: lease can be renewed for up to this long (backstop
          for stuck requests that never stop renewing the lease)
        - renew_chunk: lease renewal batch size: how many requests at a time
        """
        self.client = client
        self.lease_sec = lease_sec
        self.max_request_sec = max_request_sec
        self.renew_chunk = renew_chunk
        self._admit = client.register_script(_ADMIT_LUA)
        self._settle = client.register_script(_SETTLE_LUA)
        self._renew = client.register_script(_RENEW_LUA)
        self._record_error = client.register_script(_RECORD_ERROR_LUA)

    async def admit(
        self,
        *,
        request_id: str,
        model_name: str,
        user_id: str,
        candidates: Sequence[CandidateBackend],
        estimated_tokens: int,
        quota: UsageLimits,
    ) -> AdmitResult:
        """
        Attempt to admit a request.  Performs one atomic round trip to Redis:
        check all quotas once, then iterate candidates and assign the first one
        with available capacity.

        A reservation is created if admitted.

        The reservation has an expiring lease (see `lease_sec` above) that must
        be maintained during the lifetime of the request, using renew().

        All reservations must be cleared used settle().  Reservations cannot use
        a TTL, because several pieces of related state must be rolled back
        atomically.

        - `request_id`: unique per-request identifier
        - `model_name`: unique model name
        - `user_id`: unique user ID
        - `candidates`: ordered list of backends (uid, max_concurrency, max_errors).
          The router must select healthy backends in scope for the current request and sort them
          into the desired weighted random sampling order.
        - `estimated_tokens`:  estimated heuristic prompt_tokens+max_tokens for
           this request to be reserved.  Set to 0 for non-LLM requests.
        - `quota`: the configured usage rate limits for this model_name.
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
            keys.append(Keys.backend_errors(c.uid))

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
            self.lease_sec,
        ]
        for c in candidates:
            args.extend([c.uid, c.max_backend_concurrency, c.cooldown_threshold])

        raw = await self._admit(keys=keys, args=args)
        return AdmitResult.from_lua(raw)

    async def settle(
        self,
        request_id: str,
        actual_tokens: Optional[int] = None,
        *,
        model_name: str | None = None,
        user_id: str | None = None,
    ) -> bool:
        """
        Reverse one reservation's effects idempotently.

        Safe to call from the request `finally`, the sweeper, and retries
        simultaneously; returns True iff this call was the one that applied.

        Pass model_name and user_id from the request context to skip the
        pre-read round trip on the hot path.  The sweeper omits them and
        pays one extra GET to discover the reservation's identity.
        """
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

    async def renew(self, request_ids: Sequence[str]) -> int:
        """
        Batched, chunked lease renewal.  Each apiserver worker MUST call this
        method with all live request_ids every ~lease_sec/3 seconds to maintain
        the lease on its requests.

        Crashed workers stop renewing the lease, and another sweeper cleans up
        the stale reservations within ~lease_sec.
        """
        renewed = 0
        for i in range(0, len(request_ids), self.renew_chunk):
            chunk = list(request_ids[i : i + self.renew_chunk])
            keys = [Keys.deadlines()] + [Keys.reservation(rid) for rid in chunk]
            renewed += int(
                await self._renew(
                    keys=keys,
                    args=[self.lease_sec, self.max_request_sec, *chunk],
                )
            )
        return renewed

    async def sweep(self, batch: int = 100) -> int:
        """
        Settle reservations whose lease lapsed (crashed worker, stuck
        handler past max_request_sec).

        Lock-free: settle's idempotency makes concurrent sweeps merely wasteful,
        never wrong.  Any worker may run this opportunistically on a timer.
        """
        now = time.time()
        expired = await self.client.zrangebyscore(
            Keys.deadlines(), "-inf", now, start=0, num=batch
        )
        settled = 0
        for rid in expired:
            if await self.settle(to_str(rid), actual_tokens=None):
                settled += 1

        if settled > 0:
            logger.info(f"Reservation sweeper settled {settled} expired reservations")

        return settled

    async def record_error(
        self, backend_id: str, router_params: RouterParams
    ) -> tuple[int, bool]:
        """
        Register an upstream failure.  Returns (error_count, is_benched).

        admit() treats error_count >= router_params.cooldown_threshold as benched: the backend
        is removed from the pool for router_params.cooldown_bench_sec.

        For example: given cooldown threshold=2, window=30s, bench=120s: any
        backend that experiences 2 errors within a 30second window is benched
        for 2 minutes.

        The Lua script correctly re-arms the TTL under concurrent failures.
        """
        raw = await self._record_error(
            keys=[Keys.backend_errors(backend_id)],
            args=[
                router_params.cooldown_window_sec,
                router_params.cooldown_threshold,
                router_params.cooldown_bench_sec,
            ],
        )
        return int(raw[0]), bool(int(raw[1]))

    async def rebuild_inflight_from_ledger(self) -> InflightCounts:
        """
        Recompute per-model, per-backend inflight from the reservation ledger
        (the base table).  Feed the result to ModelStatus.reconcile_inflight per
        model.  Uses SCAN, never KEYS.
        """
        by_backend: dict[str, dict[str, int]] = {}
        by_user: dict[str, dict[str, int]] = {}
        async for key in self.client.scan_iter(
            match=Keys.reservation_scan_pattern(), count=200
        ):
            raw = await self.client.get(key)
            if not raw:
                continue
            try:
                row = json.loads(raw)
                model = row["model_name"]
                backend = row["backend_id"]
                user_id = row["user_id"]
                by_backend.setdefault(model, {}).setdefault(backend, 0)
                by_user.setdefault(model, {}).setdefault(user_id, 0)
                by_backend[model][backend] += 1
                by_user[model][user_id] += 1
            except (ValueError, KeyError):
                logger.warning(f"Reconcile cannot parse malformed reservation: {raw!r}")
                continue

        return InflightCounts(by_backend=by_backend, by_user=by_user)
