from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from first.settings import Settings


def get_async_sessionmaker() -> async_sessionmaker[AsyncSession]:
    settings = Settings.load()
    engine = create_async_engine(
        settings.db_url.get_secret_value(),
        pool_size=5,
        max_overflow=10,
    )
    return async_sessionmaker(engine, expire_on_commit=False)


__all__ = ["get_async_sessionmaker", "AsyncSession"]
