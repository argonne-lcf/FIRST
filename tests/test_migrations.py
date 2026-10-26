"""
Schema changes ship as Alembic revisions: the migrated schema must match the
SQLAlchemy models, and every revision must downgrade cleanly.

If test_models_match_migrations fails, run `make migration m="..."` and review
the generated revision. Never edit a revision that has been applied in prod.
"""

import os
from typing import Any

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from first_gateway.database.models import Base

from .fixtures.db import ALEMBIC_INI

SCHEMA = Base.metadata.schema


def _include_name(name: str | None, type_: str, _parent: Any) -> bool:
    if type_ == "schema":
        return name == SCHEMA
    return not (type_ == "table" and name == "alembic_version")


def _diff(url: str) -> Any:
    engine = sa.create_engine(url)
    try:
        with engine.connect() as conn:
            context = MigrationContext.configure(
                conn,
                opts={"include_schemas": True, "include_name": _include_name},
            )
            return compare_metadata(context, Base.metadata)
    finally:
        engine.dispose()


@pytest.fixture
def db_url(db: async_sessionmaker[AsyncSession]) -> str:
    """A fresh database migrated to head (see the `db` fixture)."""
    return os.environ["FIRST_DB_URL"]


def test_models_match_migrations(db_url: str) -> None:
    assert _diff(db_url) == []


def test_downgrade_to_base_and_upgrade_to_head(db_url: str) -> None:
    cfg = Config(str(ALEMBIC_INI))
    cfg.attributes["connection_url"] = db_url
    command.downgrade(cfg, "base")
    command.upgrade(cfg, "head")
    assert _diff(db_url) == []
