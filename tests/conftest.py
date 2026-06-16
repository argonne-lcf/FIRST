"""
Integration-test database fixtures.

Strategy (see https://gajus.com/blog/setting-up-postgre-sql-for-running-integration-tests):
a session-scoped *template* database is built once with the full schema, then each
test clones it into a throwaway database via ``CREATE DATABASE ... TEMPLATE ...``.
Cloning from a template that lives on a tmpfs disk (see deploy/compose.dev.yaml) is
fast (~tens of ms) and gives every test a pristine, fully-isolated database.

Requires a running Postgres (``make dev-db-up``).
"""

import uuid
from typing import AsyncGenerator, Generator

import pytest
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateSchema

from first_gateway import Settings
from first_gateway.database.models import Base

# Schema namespace used by all ORM models (see Base.metadata in database/models.py).
SCHEMA = "first"


def _drop_database(admin: Engine, name: str) -> None:
    """Terminate any lingering connections and drop ``name`` if it exists."""
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
    Build the schema-blueprint template database once per test session.

    Mirrors tests/bootstrap_db.py: create the ``first`` schema and all tables,
    then mark the database as a template so tests can clone it cheaply.
    """
    template_name = f"{_db_base_url.database}_template"

    # Start from a clean slate, then create an empty database to populate.
    _drop_database(_admin_engine, template_name)
    with _admin_engine.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{template_name}"'))

    # Build the schema inside the (not-yet-template) database.
    builder = create_engine(_db_base_url.set(database=template_name))
    try:
        with builder.begin() as conn:
            conn.execute(CreateSchema(SCHEMA, if_not_exists=True))
            Base.metadata.create_all(conn)
    finally:
        builder.dispose()

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
    """
    test_name = f"test_{uuid.uuid4().hex}"

    # CREATE DATABASE ... TEMPLATE requires that no session is connected to the
    # template; the builder engine above is already disposed, so this is safe.
    with _admin_engine.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{test_name}" TEMPLATE "{template_db}"'))

    test_url = _db_base_url.set(database=test_name)
    monkeypatch.setenv("FIRST_DB_URL", test_url.render_as_string(hide_password=False))

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
