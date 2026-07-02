import asyncio
import random
import warnings
from typing import ClassVar, Generic, TypeVar

from pydantic import BaseModel
from redis.asyncio import Redis

from first_common.errors import StatusCASFailed

T = TypeVar("T", bound=BaseModel)


class StatusStore(Generic[T]):
    """Typed access to one resource type's Redis-backed status.

    Status models must define a default value for every field — the store
    materializes an empty T() when Redis is cold, and patches are passed
    as partially-populated T instances.
    """

    resource: ClassVar[str]
    model: ClassVar[type[T]]
    ttl_seconds: ClassVar[int]
    max_cas_attempts: ClassVar[int] = 5

    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    def _key(self, name: str) -> str:
        return f"status:{self.resource}:{name}"

    def _parse(self, raw: bytes | str | None) -> T:
        if raw is None:
            return self.model()
        return self.model.model_validate_json(raw)

    async def get(self, name: str) -> T:
        return self._parse(await self._redis.get(self._key(name)))

    async def get_many(self, names: list[str]) -> dict[str, T]:
        if not names:
            return {}
        raws = await self._redis.mget([self._key(n) for n in names])
        return {n: self._parse(r) for n, r in zip(names, raws)}

    async def update(self, name: str, patch: T) -> T:
        """Atomic compare-and-swap merge of the fields explicitly set on
        patch onto the current value."""
        key = self._key(name)
        explicit = patch.model_dump(exclude_unset=True)

        for attempt in range(self.max_cas_attempts):
            raw = await self._redis.get(key)
            current = self._parse(raw)
            new = self.model.model_validate(current.model_dump() | explicit)
            new_raw = new.model_dump_json()
            if raw is None:
                ok = await self._redis.set(key, new_raw, ex=self.ttl_seconds, nx=True)
            else:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", UserWarning)
                    ok = await self._redis.set(
                        key, new_raw, ex=self.ttl_seconds, ifeq=raw
                    )
            if ok:
                return new
            base = 0.005 * (2**attempt)
            await asyncio.sleep(base * random.uniform(0.5, 1.5))
        raise StatusCASFailed(f"{key}: lost CAS race {self.max_cas_attempts}x")

    async def delete(self, name: str) -> None:
        await self._redis.delete(self._key(name))
