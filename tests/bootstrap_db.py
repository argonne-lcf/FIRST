# type: ignore
import asyncio
import logging

from sqlalchemy.schema import CreateSchema

from first_gateway import Settings
from first_gateway.database.models import Base

logging.basicConfig(level="INFO")


async def main():
    async with Settings().build_clients() as state, state["db_sessionmaker"]() as sess:
        conn = await sess.connection()
        await conn.execute(CreateSchema("first", if_not_exists=True))
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
        await sess.commit()


if __name__ == "__main__":
    asyncio.run(main())
