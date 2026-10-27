from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from shared.config import get_settings

_async_engine: Optional[AsyncEngine] = None
_async_session_factory: Optional[async_sessionmaker[AsyncSession]] = None


def get_async_engine() -> AsyncEngine:
    global _async_engine
    if _async_engine is None:
        settings = get_settings()
        _async_engine = create_async_engine(
            settings.async_database_url,
            pool_size=50,
            max_overflow=50,
            pool_pre_ping=True,
            pool_recycle=3600,
            connect_args={
                "server_settings": {
                    "application_name": "pulse_api",
                },
                "statement_cache_size": 1000,
            },
        )
    return _async_engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    global _async_session_factory
    if _async_session_factory is None:
        engine = get_async_engine()
        _async_session_factory = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
        )
    return _async_session_factory


@asynccontextmanager
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    factory = get_session_factory()
    async with factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise


async def get_db_session_dep() -> AsyncGenerator[AsyncSession, None]:
    async with get_db_session() as session:
        yield session


async def check_db_health() -> bool:
    try:
        async with get_db_session() as session:
            result = await session.execute(text("SELECT 1"))
            return result.scalar() == 1
    except Exception:
        return False


async def close_db_engine() -> None:
    global _async_engine, _async_session_factory
    if _async_engine is not None:
        await _async_engine.dispose()
        _async_engine = None
        _async_session_factory = None
