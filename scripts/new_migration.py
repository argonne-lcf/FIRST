"""
Autogenerate an Alembic revision from the difference between the SQLAlchemy
models and the current migration head.

Diffs against a throwaway database migrated to head, so the result never
depends on (or touches) the state of the dev database.  Review the generated
file before committing: autogenerate misses server defaults, enum changes,
renames (emitted as drop + add) and data migrations.

    make migration m="add static deployment mtls"
"""

import sys
from pathlib import Path

import sqlalchemy as sa
from alembic import command
from alembic.config import Config

import first_gateway.database
from first_gateway.settings import Settings

ALEMBIC_INI = Path(first_gateway.database.__file__).parent / "alembic.ini"


def main(message: str) -> None:
    base_url = sa.make_url(Settings().db_url.get_secret_value())
    scratch = f"{base_url.database}_autogenerate"
    admin = sa.create_engine(
        base_url.set(database="postgres"), isolation_level="AUTOCOMMIT"
    )
    with admin.connect() as conn:
        conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{scratch}" WITH (FORCE)'))
        conn.execute(sa.text(f'CREATE DATABASE "{scratch}"'))
    try:
        cfg = Config(str(ALEMBIC_INI))
        cfg.attributes["connection_url"] = base_url.set(
            database=scratch
        ).render_as_string(hide_password=False)
        command.upgrade(cfg, "head")
        command.revision(cfg, message=message, autogenerate=True)
    finally:
        with admin.connect() as conn:
            conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{scratch}" WITH (FORCE)'))
        admin.dispose()


if __name__ == "__main__":
    if len(sys.argv) != 2 or not sys.argv[1].strip():
        sys.exit('usage: make migration m="short description"')
    main(sys.argv[1])
