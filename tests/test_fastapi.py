from dataclasses import dataclass
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest

import piping_bag.fastapi as pb_fastapi
from piping_bag.fastapi import get_pool, init_pool


class TestGetPool:
    def test_raises_without_init(self):
        original = pb_fastapi._pool
        pb_fastapi._pool = None
        try:
            with pytest.raises(RuntimeError, match="not initialized"):
                get_pool()
        finally:
            pb_fastapi._pool = original

    def test_returns_pool_when_set(self, pool):
        pb_fastapi._pool = pool
        try:
            assert get_pool() is pool
        finally:
            pb_fastapi._pool = None


class TestInitPool:
    async def test_sets_module_pool(self):
        sentinel = object()
        with patch(
            "piping_bag.fastapi.asyncpg.create_pool",
            new_callable=AsyncMock,
            return_value=sentinel,
        ):
            result = await init_pool("postgresql://localhost/test")
            assert result is sentinel
            assert pb_fastapi._pool is sentinel
        pb_fastapi._pool = None


class TestPoolQueries:
    """Integration tests using real pglite database."""

    async def test_simple_query(self, pool):
        async with pool.acquire() as conn:
            result = await conn.fetchval("SELECT 1")
            assert result == 1

    async def test_create_table_and_query(self, pool):
        async with pool.acquire() as conn:
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS test_items (id SERIAL PRIMARY KEY, name TEXT NOT NULL)"
            )
            await conn.execute("INSERT INTO test_items (name) VALUES ($1)", "test_item")
            row = await conn.fetchrow(
                "SELECT * FROM test_items WHERE name = $1", "test_item"
            )
            assert row["name"] == "test_item"
            await conn.execute("DROP TABLE test_items")

    async def test_transaction_rollback(self, pool):
        async with pool.acquire() as conn:
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS tx_test (id SERIAL PRIMARY KEY, val TEXT)"
            )
            try:
                async with conn.transaction():
                    await conn.execute(
                        "INSERT INTO tx_test (val) VALUES ($1)", "committed"
                    )

                async with conn.transaction():
                    await conn.execute(
                        "INSERT INTO tx_test (val) VALUES ($1)", "rolled_back"
                    )
                    raise Exception("force rollback")
            except Exception:
                pass

            count = await conn.fetchval("SELECT count(*) FROM tx_test")
            assert count == 1
            await conn.execute("DROP TABLE tx_test")


class TestCreateQueriesDependency:
    def test_dependency_type(self):
        @dataclass
        class FakeQueries:
            conn: asyncpg.Connection

        from piping_bag.fastapi import create_queries_dependency

        dep = create_queries_dependency(FakeQueries)
        assert hasattr(dep, "__metadata__")
