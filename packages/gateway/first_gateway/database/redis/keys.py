from typing import Literal

CONFIG_CHANNEL = "cfg:changed"


class Keys:
    """Redis key builders — the single source of truth for all key patterns.

    Every Redis key used by the gateway MUST be constructed here.
    Lua scripts receive pre-built keys via KEYS[] and never assemble keys
    from prefix strings.
    """

    @staticmethod
    def config() -> str:
        """The Main Router Configuration Blob"""
        return "router-cfg"

    @staticmethod
    def backend_inflight(model: str, backend_id: str) -> str:
        """ZSET request_id → admit_ts. ZCARD = per-backend inflight."""
        return f"rt:model:{model}:backend:{backend_id}:inflight"

    @staticmethod
    def user_inflight(model: str, user_id: str) -> str:
        """ZSET request_id → admit_ts. ZCARD = user concurrency."""
        return f"rt:user-inflight:{model}:{user_id}"

    @staticmethod
    def model_inflight(model: str) -> str:
        """ZSET request_id → admit_ts. ZCARD = model total inflight."""
        return f"rt:model:{model}:inflight"

    @staticmethod
    def model_rejects(model: str) -> str:
        """HASH {capacity_rejects_total, last_reject_ts}."""
        return f"rt:model:{model}:rejects"

    @staticmethod
    def backend_errors(backend_id: str) -> str:
        """Error counter with TTL; count >= threshold IS the cooldown bench."""
        return f"rt:backend:{backend_id}:errors"

    @staticmethod
    def reservation(request_id: str) -> str:
        """JSON blob written by admit, read/deleted by settle."""
        return f"rt:reserve:{request_id}"

    @staticmethod
    def deadlines() -> str:
        """ZSET of request_ids scored by deadline timestamp."""
        return "rt:deadlines"

    @staticmethod
    def user_rate_limit(
        model: str, user: str, resource: Literal["tokens", "rpm"]
    ) -> str:
        """Per-user, per-model TPM/RPM Bucket Arrival Times"""
        return f"quota:{model}:{user}:{resource}"

    @staticmethod
    def backend_inflight_scan_pattern() -> str:
        """SCAN match pattern for all per-backend inflight ZSETs.
        Keep in sync with backend_inflight()."""
        return "rt:model:*:backend:*:inflight"

    @staticmethod
    def user_inflight_scan_pattern() -> str:
        """SCAN match pattern for all per-user inflight ZSETs.
        Keep in sync with user_inflight()."""
        return "rt:user-inflight:*"

    @staticmethod
    def model_inflight_scan_pattern() -> str:
        """SCAN match pattern for all per-model reservation ZSETs.
        Keep in sync with model_inflight()."""
        return "rt:model:*:inflight"

    @staticmethod
    def token_introspect(token_hash: str) -> str:
        """Cached token introspection result, keyed by SHA-256 of the bearer token."""
        return f"auth:token_introspect:{token_hash}"

    @staticmethod
    def authed_user(user_id: str) -> str:
        """NX-guarded flag to emit a UserAuthEvent once per TTL window."""
        return f"auth:user:{user_id}"

    @staticmethod
    def log_dedup_5xx(user: str, status_code: int) -> str:
        """NX-guarded flag to suppress repeated 5xx log lines."""
        return f"logdedup:{user}:{status_code}"

    @staticmethod
    def log_dedup_4xx(user: str, fingerprint: str, status_code: int) -> str:
        """NX-guarded flag to suppress repeated 4xx log lines."""
        return f"logdedup:{user}:{fingerprint}:{status_code}"

    @staticmethod
    def pilot_job_resources(uid: int) -> str:
        """JSON Blob of serialized PilotResources"""
        return f"pilot_job:{uid}:resources"
