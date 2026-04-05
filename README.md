# piping_bag

Unified CLI and FastAPI integration for database migrations (pgschema) and typed query generation (sqlc).

## Install

```bash
pip install git+https://github.com/draew6/piping_bag@0.6.8.git
```

Requires `pgschema` and `sqlc` on PATH. Python 3.13+.

## Quick start

```bash
pb init              # scaffold db/, sqlc.yaml, .env
# edit .env with DATABASE_URL
# edit db/schema.sql
# write queries in db/queries/*.sql
pb up                # apply schema + generate typed Python client
```

## Usage

Write SQL queries with sqlc annotations:

```sql
-- name: GetUser :one
SELECT * FROM users WHERE id = $1;

-- name: ListUsers :many
SELECT * FROM users ORDER BY created_at DESC LIMIT $1;
```

Use in FastAPI:

```python
import piping_bag
from fastapi import FastAPI
from db import QueriesDep

app = FastAPI()
piping_bag.setup(app)

@app.get("/users")
async def list_users(queries: QueriesDep):
    return await queries.list_users(limit=100)
```

## CLI commands

| Command | Description |
|---|---|
| `pb init` | Scaffold project |
| `pb up` | Apply migrations + generate code |
| `pb plan` | Show planned migration |
| `pb apply` | Apply migration |
| `pb generate` | Generate typed Python from SQL |
| `pb diff` | Show schema diff |
| `pb dump` | Dump live DB schema |
| `pb studio` | Open Prisma Studio |
| `pb sql` | Open Harlequin SQL IDE |

## Documentation

See [CLAUDE.md](./CLAUDE.md) for detailed patterns, rules, and architecture.

## Used via fastlet

Most services use piping_bag through [fastlet](https://github.com/draew6/fastlet2). In that case, use `fastlet db <command>` instead of `pb <command>`.