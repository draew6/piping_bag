"""piping_bag — unified CLI and FastAPI integration for pgschema + SQLC.

CLI (invoked as `pb`):
    pb init      — scaffold project (db/schema.sql, db/queries/, sqlc.yaml, .env)
    pb plan      — diff desired schema vs live database
    pb apply     — apply migration
    pb sync      — plan + apply
    pb generate  — generate Python client from SQL queries (sqlc)
    pb up        — apply + generate
    pb dump      — dump live DB schema to db/schema.sql
    pb diff      — show planned changes (human-readable)
    pb studio    — launch Prisma Studio

FastAPI integration:
    import piping_bag
    from db import QueriesDep

    app = FastAPI()
    piping_bag.setup(app)
"""

from piping_bag.fastapi import (
    close_pool,
    create_queries_dependency,
    get_pool,
    init_pool,
    setup,
)

__all__ = [
    "close_pool",
    "create_queries_dependency",
    "get_pool",
    "init_pool",
    "setup",
]
