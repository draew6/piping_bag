from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Annotated

import asyncpg
from fastapi import Depends, FastAPI

from piping_bag.config import PipingBagConfig

_pool: asyncpg.Pool | None = None


async def init_pool(database_url: str, **kwargs) -> asyncpg.Pool:
    global _pool
    _pool = await asyncpg.create_pool(database_url, **kwargs)
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("Connection pool not initialized. Call setup(app) first.")
    return _pool


def setup(app: FastAPI) -> None:
    """Set up piping_bag on a FastAPI app. Manages the asyncpg connection pool automatically."""
    original_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def wrapped_lifespan(app: FastAPI) -> AsyncGenerator[None]:
        config = PipingBagConfig()  # type: ignore[call-arg]
        await init_pool(config.database_url)
        try:
            async with original_lifespan(app) as state:
                yield state
        finally:
            await close_pool()

    app.router.lifespan_context = wrapped_lifespan


def create_queries_dependency[T](queries_class: type[T]) -> type[T]:
    async def _get_queries() -> AsyncGenerator[T]:
        pool = get_pool()
        async with pool.acquire() as conn:
            yield queries_class(conn)  # type: ignore[call-arg]

    return Annotated[queries_class, Depends(_get_queries)]  # type: ignore[return-value]
