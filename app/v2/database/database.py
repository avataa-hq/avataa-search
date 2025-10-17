from typing import AsyncGenerator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
    async_sessionmaker,
)
from settings.config import DATABASE_URL, TITLE, DB_SCHEMA
from v2.database.schema import Base


async_engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_size=20,
    max_overflow=100,
    pool_pre_ping=True,
    connect_args={
        "server_settings": {
            "application_name": f"{TITLE} MS",
            "search_path": DB_SCHEMA,
        },
    },
)
session_maker = async_sessionmaker(async_engine, expire_on_commit=False)


async def init_tables():
    """Initialize tables"""
    async with async_engine.begin() as conn:
        await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}"))
        await conn.execute(text(f"SET search_path TO {DB_SCHEMA};"))
        await conn.run_sync(Base.metadata.create_all)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Yields AsyncSession object"""
    async with session_maker() as session:
        yield session
