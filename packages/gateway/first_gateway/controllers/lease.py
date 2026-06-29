import asyncio
import logging
import os
import uuid

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ..database.models import controller_manager_lease

logger = logging.getLogger(__name__)

RENEW_INTERVAL = 10.0


class ManagerLease:
    def __init__(self, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
        self._sessionmaker = sessionmaker
        self.holder_id = uuid.uuid4().hex

    async def acquire(self) -> bool:
        """Attempt to claim the lease (insert, or take over an expired one)."""
        t = controller_manager_lease
        stmt = (
            pg_insert(t)
            .values(
                singleton=True,
                holder_id=self.holder_id,
                renewed_at=sa.func.now(),
            )
            .on_conflict_do_update(
                index_elements=[t.c.singleton],
                set_={"holder_id": self.holder_id, "renewed_at": sa.func.now()},
                where=(t.c.renewed_at + t.c.lease_duration < sa.func.now()),
            )
        )
        async with self._sessionmaker() as sess:
            result = await sess.execute(stmt)
            await sess.commit()
            return bool(result.rowcount)  # type: ignore[attr-defined]

    async def renew(self) -> bool:
        """Refresh renewed_at. Returns False if the row is missing or held by someone else."""
        t = controller_manager_lease
        stmt = (
            sa.update(t)
            .where(t.c.singleton.is_(True), t.c.holder_id == self.holder_id)
            .values(renewed_at=sa.func.now())
        )
        async with self._sessionmaker() as sess:
            result = await sess.execute(stmt)
            await sess.commit()
            return bool(result.rowcount)  # type: ignore[attr-defined]

    async def release(self) -> None:
        """Delete the lease row on clean shutdown."""
        t = controller_manager_lease
        stmt = sa.delete(t).where(t.c.holder_id == self.holder_id)
        async with self._sessionmaker() as sess:
            await sess.execute(stmt)
            await sess.commit()

    async def run_renewal(self) -> None:
        """Renew every RENEW_INTERVAL seconds. Kill process after 2 consecutive failures."""
        consecutive_failures = 0
        while True:
            await asyncio.sleep(RENEW_INTERVAL)
            try:
                if await self.renew():
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
                    logger.error(
                        "Lease renewal failed (holder mismatch or row missing); "
                        "consecutive_failures=%d",
                        consecutive_failures,
                    )
            except Exception:
                consecutive_failures += 1
                logger.exception(
                    "Lease renewal error; consecutive_failures=%d",
                    consecutive_failures,
                )
            if consecutive_failures >= 2:
                logger.critical(
                    "Two consecutive lease renewal failures; terminating process"
                )
                os._exit(1)
