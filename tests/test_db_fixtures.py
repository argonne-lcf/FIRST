"""Smoke tests for the template-database test fixtures"""

import os

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from first_gateway.database.models import AccessGroup


async def test_schema_and_tables_exist(db_session: AsyncSession) -> None:
    """The cloned database carries the `first` schema and ORM tables."""
    schema = await db_session.scalar(
        sa.text(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name = 'first'"
        )
    )
    assert schema == "first"

    # A query against an ORM model proves the tables were cloned in.
    assert await AccessGroup.list(db_session) == []


async def test_settings_env_is_patched(db: async_sessionmaker[AsyncSession]) -> None:
    """FIRST_DB_URL is redirected at this test's throwaway database."""
    assert "/test_" in os.environ["FIRST_DB_URL"]


async def test_writes_are_isolated_part_one(db_session: AsyncSession) -> None:
    db_session.add(AccessGroup(name="group-a", allowed_groups=[], allowed_domains=[]))
    await db_session.commit()
    assert len(await AccessGroup.list(db_session)) == 1


async def test_writes_are_isolated_part_two(db_session: AsyncSession) -> None:
    # Despite part_one committing a row, this test gets a fresh database.
    assert await AccessGroup.list(db_session) == []
