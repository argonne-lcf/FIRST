import uuid
from pathlib import Path
from typing import AsyncGenerator, Generator

import pytest
from alembic import command as alembic_command
from alembic.config import Config as AlembicConfig
from redis.asyncio import Redis as AsyncRedis
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import first_gateway.database
from first_gateway import Settings

SCHEMA = "first"
ALEMBIC_INI = Path(first_gateway.database.__file__).parent / "alembic.ini"


def _drop_database(admin: Engine, name: str) -> None:
    """Terminate any lingering connections and drop ``name`` if it exists."""

    if not (
        name.startswith("test_")
        or name.endswith("_test")
        or name.endswith("dev_template")
    ):
        raise RuntimeError(f"Will not delete {name=}")

    with admin.connect() as conn:
        # A template DB can't be dropped while is_template is set.
        conn.execute(
            text("UPDATE pg_database SET datistemplate = false WHERE datname = :name"),
            {"name": name},
        )
        conn.execute(
            text(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = :name AND pid <> pg_backend_pid()"
            ),
            {"name": name},
        )
        conn.execute(text(f'DROP DATABASE IF EXISTS "{name}"'))


@pytest.fixture(scope="session")
def _db_base_url() -> URL:
    """The configured application database URL, parsed for re-targeting."""
    return make_url(Settings().db_url.get_secret_value())


@pytest.fixture(scope="session")
def _admin_engine(_db_base_url: URL) -> Generator[Engine, None, None]:
    """
    A sync, AUTOCOMMIT engine bound to the ``postgres`` maintenance database.

    CREATE/DROP DATABASE cannot run inside a transaction, nor while connected to
    the database being created from / dropped, so all such DDL goes through here.
    """
    engine = create_engine(
        _db_base_url.set(database="postgres"),
        isolation_level="AUTOCOMMIT",
    )
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture(scope="session")
def template_db(_db_base_url: URL, _admin_engine: Engine) -> Generator[str, None, None]:
    """
    Build the template database once per test session.

    See https://gajus.com/blog/setting-up-postgre-sql-for-running-integration-tests.
    Two tricks are employed that make PostgreSQL integration testing much faster, while
    retaining full isolation between test cases:

    1. (Here) Cloning a template DB instead of running DDL or TRUNCATE between test cases
    2. (compose.dev.yaml) Mounting a tmpfs disk to keep postgres data in-memory
    """
    template_name = f"{_db_base_url.database}_template"

    # Start from a clean slate, then create an empty database to populate.
    _drop_database(_admin_engine, template_name)
    with _admin_engine.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{template_name}"'))

    # Run alembic migrations to build schema + triggers.
    template_url = _db_base_url.set(database=template_name)
    alembic_cfg = AlembicConfig(str(ALEMBIC_INI))
    alembic_cfg.attributes["connection_url"] = template_url.render_as_string(
        hide_password=False
    )
    alembic_command.upgrade(alembic_cfg, "head")

    # Mark as template only after the schema is in place and connections closed.
    with _admin_engine.connect() as conn:
        conn.execute(
            text("UPDATE pg_database SET datistemplate = true WHERE datname = :name"),
            {"name": template_name},
        )

    try:
        yield template_name
    finally:
        _drop_database(_admin_engine, template_name)


@pytest.fixture
async def db(
    _db_base_url: URL,
    _admin_engine: Engine,
    template_db: str,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[async_sessionmaker[AsyncSession], None]:
    """
    A pristine, isolated database for a single test.

    Clones the session template into a uniquely-named database, points the
    application settings at it via ``FIRST_DB_URL`` (so anything that builds a
    fresh ``Settings()`` — e.g. the FastAPI lifespan — uses this database), and
    yields an ``async_sessionmaker``. The database is dropped on teardown.

    Redis is pointed at a throwaway logical DB and flushed before startup so
    caching can't leak between tests.
    """
    test_name = f"test_{uuid.uuid4().hex}"

    # Clone Test DB
    with _admin_engine.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{test_name}" TEMPLATE "{template_db}"'))

    # Configure Test DB
    test_url = _db_base_url.set(database=test_name)
    monkeypatch.setenv("FIRST_DB_URL", test_url.render_as_string(hide_password=False))

    # Configure Test Redis
    redis_base, *_ = Settings().redis_url.rsplit("/", 1)
    redis_url = f"{redis_base}/15"
    monkeypatch.setenv("FIRST_REDIS_URL", redis_url)

    # Flush Test Redis
    cache = AsyncRedis.from_url(redis_url)
    await cache.flushdb()
    await cache.aclose()

    engine = create_async_engine(test_url, pool_size=5, max_overflow=10)
    sessionmaker = async_sessionmaker(engine, expire_on_commit=False)

    try:
        yield sessionmaker
    finally:
        # Drop the engine's connections before dropping the database out from
        # under them.
        await engine.dispose()
        _drop_database(_admin_engine, test_name)


@pytest.fixture
async def db_session(
    db: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncSession, None]:
    """A single AsyncSession against this test's isolated database."""
    async with db() as session:
        yield session
