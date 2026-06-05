# inference_gateway/cache_backend.py
import logging
import time
from collections.abc import Iterable, Mapping
from typing import Any

from django.core.cache import caches
from django.core.cache.backends.base import DEFAULT_TIMEOUT, BaseCache

log = logging.getLogger(__name__)

# Django constructs a new cache instance per request, so this state needs to be
# module-scoped:
_primary_healthy = True
_last_failure_at = 0.0


class FallbackCache(BaseCache):
    """
    Cache backend that uses a primary cache (e.g. Redis) when available
    and falls back to a secondary cache (e.g. locmem) on failure.

    Health is checked lazily and cached to avoid hammering a downed Redis.  A
    circuit breaker with HEALTH_CHECK_INTERVAL cooldown means that Redis can
    stop/start at any time without requiring this API service to restart.

    This backend helps to clean up the pattern of call sites that repeatedly
    handle cacheing errors.
    """

    def __init__(self, _server: Any, params: dict[str, Any]) -> None:
        super().__init__(params)
        options = params.get("OPTIONS", {})
        self._primary_alias: str = options["PRIMARY_ALIAS"]
        self._fallback_alias: str = options["FALLBACK_ALIAS"]
        self._health_check_interval: float = options.get("HEALTH_CHECK_INTERVAL", 30.0)

    @property
    def _primary(self) -> BaseCache:
        return caches[self._primary_alias]

    @property
    def _fallback(self) -> BaseCache:
        return caches[self._fallback_alias]

    def _should_try_primary(self) -> bool:
        global _primary_healthy, _last_failure_at

        if _primary_healthy:
            return True
        # Circuit-breaker: retry primary after the cooldown
        if time.monotonic() - _last_failure_at >= self._health_check_interval:
            return True
        return False

    def _mark_primary_failed(self, exc: Exception) -> None:
        global _primary_healthy, _last_failure_at

        if _primary_healthy:
            log.warning(f"Primary cache failed, falling back: {exc}")
        _primary_healthy = False
        _last_failure_at = time.monotonic()

    def _mark_primary_healthy(self) -> None:
        global _primary_healthy, _last_failure_at

        if not _primary_healthy:
            log.info("Primary cache recovered")
        _primary_healthy = True

    def _call(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        if self._should_try_primary():
            try:
                result = getattr(self._primary, method_name)(*args, **kwargs)
                self._mark_primary_healthy()
                return result
            except Exception as e:
                self._mark_primary_failed(e)
        return getattr(self._fallback, method_name)(*args, **kwargs)

    # Implement the BaseCache interface by delegating
    def get(
        self,
        key: str,
        default: Any = None,
        version: int | None = None,
    ) -> Any:
        return self._call("get", key, default, version=version)

    def set(
        self,
        key: str,
        value: Any,
        timeout: float | object | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> None:
        resp: None = self._call("set", key, value, timeout, version=version)
        return resp

    def add(
        self,
        key: str,
        value: Any,
        timeout: float | object | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> bool:
        resp: bool = self._call("add", key, value, timeout, version=version)
        return resp

    def delete(self, key: str, version: int | None = None) -> bool:
        resp: bool = self._call("delete", key, version=version)
        return resp

    def get_many(
        self,
        keys: Iterable[str],
        version: int | None = None,
    ) -> dict[str, Any]:
        resp: dict[str, Any] = self._call("get_many", keys, version=version)
        return resp

    def set_many(
        self,
        mapping: Mapping[str, Any],
        timeout: float | object | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> list[str]:
        resp: list[str] = self._call("set_many", mapping, timeout, version=version)
        return resp

    def delete_many(self, keys: Iterable[str], version: int | None = None) -> None:
        resp: None = self._call("delete_many", keys, version=version)
        return resp

    def has_key(self, key: str, version: int | None = None) -> bool:
        resp: bool = self._call("has_key", key, version=version)
        return resp

    def clear(self) -> None:
        resp: None = self._call("clear")
        return resp

    def incr(
        self,
        key: str,
        delta: int = 1,
        version: int | None = None,
    ) -> int:
        resp: int = self._call("incr", key, delta, version=version)
        return resp

    def decr(
        self,
        key: str,
        delta: int = 1,
        version: int | None = None,
    ) -> int:
        resp: int = self._call("decr", key, delta, version=version)
        return resp

    def touch(
        self,
        key: str,
        timeout: float | object | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> bool:
        resp: bool = self._call("touch", key, timeout, version=version)
        return resp
