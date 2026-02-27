import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session")
def postgres():
    with PostgresContainer("postgres:17") as pg:
        yield pg


@pytest.fixture(scope="session")
def database_url(postgres):
    host = postgres.get_container_host_ip()
    port = postgres.get_exposed_port(5432)
    return f"postgresql://test:test@{host}:{port}/test"


@pytest_asyncio.fixture(scope="session")
async def pool(database_url):
    p = await asyncpg.create_pool(database_url)
    yield p
    await p.close()
