import asyncio

import asyncpg
import pytest
import pytest_asyncio
from py_pglite import PGliteConfig, PGliteManager

PGLITE_PORT = 15432


@pytest.fixture(scope="session")
def pglite():
    config = PGliteConfig(use_tcp=True, tcp_port=PGLITE_PORT, timeout=30)
    with PGliteManager(config) as manager:
        yield manager, config


@pytest.fixture(scope="session")
def database_url(pglite):
    _, config = pglite
    return (
        f"postgresql://postgres:postgres@{config.tcp_host}:{config.tcp_port}/postgres"
    )


@pytest_asyncio.fixture(scope="session")
async def pool(database_url):
    """Session-scoped asyncpg pool. Terminated at session end."""
    p = await asyncpg.create_pool(
        database_url, min_size=1, max_size=1, server_settings={}, ssl=False
    )
    yield p
    p.terminate()
