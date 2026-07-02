import os
from logging.config import fileConfig

import sqlalchemy as sa
from alembic import context

from first_gateway.database.models import Base

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata
SCHEMA = target_metadata.schema


def get_url() -> str:
    override = config.attributes.get("connection_url")
    if override:
        return str(override)
    return os.environ["FIRST_DB_URL"]


def run_migrations_offline() -> None:
    context.configure(
        url=get_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        version_table_schema=SCHEMA,
        include_schemas=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = sa.create_engine(get_url())
    with connectable.connect() as connection:
        connection.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))
        connection.commit()
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table_schema=SCHEMA,
            include_schemas=True,
        )
        with context.begin_transaction():
            context.run_migrations()
    connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
