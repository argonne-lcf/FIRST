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
        await conn.run_sync(Base.metadata.create_all)
        await sess.commit()
    print("Migration OK")


if __name__ == "__main__":
    asyncio.run(main())
