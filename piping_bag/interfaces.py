import sys
import re

if "sqlite3" not in sys.modules:
    try:
        import pysqlite3

        sys.modules["sqlite3"] = sys.modules.pop("pysqlite3")
    except ImportError:
        print("pysqlite3 is not installed, and sqlite3 is not available.")


import asyncpg
import aiosqlite
from abc import ABC, abstractmethod


class Database(ABC):
    @abstractmethod
    async def fetch_one(self, query: str, *args): ...

    @abstractmethod
    async def fetch_many(self, query: str, *args): ...

    @abstractmethod
    async def execute(self, query: str, *args): ...

    @abstractmethod
    async def execute_many(self, query: str, args): ...


class PostgresDatabase(Database):
    def __init__(
        self, db_name: str, db_user: str, db_password: str, db_host: str, db_port: str
    ):
        self.dsn = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    async def get_connection(self):
        return await asyncpg.connect(self.dsn, statement_cache_size=0)

    async def fetch_one(self, query: str, *args):
        conn = await self.get_connection()
        result = await conn.fetchrow(query, *args)
        await conn.close()
        return result

    async def fetch_many(self, query: str, *args):
        conn = await self.get_connection()
        result = await conn.fetch(query, *args)
        await conn.close()
        return result

    async def execute(self, query: str, *args):
        conn = await self.get_connection()
        result = await conn.execute(query, *args)
        await conn.close()
        return result

    async def execute_many(self, query: str, *args):
        conn = await self.get_connection()
        result = await conn.executemany(query, *args)
        await conn.close()
        return result


class SQLiteDatabase(Database):
    def __init__(self, path: str):
        self.dsn = path

    @staticmethod
    def to_sqlite_question_marks(sql: str) -> str:
        return re.sub(r"\$(\d+)", r"?\1", sql)

    def get_connection(self):
        return aiosqlite.connect(self.dsn)

    async def fetch_one(self, query: str, *args):
        async with self.get_connection() as connection:
            connection.row_factory = aiosqlite.Row
            async with connection.cursor() as cursor:
                await cursor.execute(self.to_sqlite_question_marks(query), args)
                result = await cursor.fetchone()
                if not query.lower().startswith("select"):
                    await connection.commit()
                return result

    async def fetch_many(self, query: str, *args):
        async with self.get_connection() as connection:
            connection.row_factory = aiosqlite.Row
            async with connection.cursor() as cursor:
                await cursor.execute(
                    self.to_sqlite_question_marks(query), parameters=args
                )
                result = await cursor.fetchall()
                if not query.lower().startswith("select"):
                    await connection.commit()
                return result

    async def execute(self, query: str, *args):
        async with self.get_connection() as connection:
            connection.row_factory = aiosqlite.Row
            async with connection.cursor() as cursor:
                await cursor.execute(
                    self.to_sqlite_question_marks(query), parameters=args
                )
                if not query.lower().startswith("select"):
                    await connection.commit()

    async def execute_many(self, query: str, args):
        async with self.get_connection() as connection:
            connection.row_factory = aiosqlite.Row
            async with connection.cursor() as cursor:
                await cursor.executemany(
                    self.to_sqlite_question_marks(query), parameters=args
                )
                await connection.commit()
