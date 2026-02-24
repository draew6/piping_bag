import importlib.resources
from pathlib import Path

import typer

from piping_bag.config import (
    QUERIES_DIR,
    SCHEMA_PATH,
    PipingBagConfig,
    parse_database_url,
)
from piping_bag.runner import run

app = typer.Typer(help="piping_bag — pgschema + SQLC unified CLI")


def _load_config() -> tuple[PipingBagConfig, dict[str, str]]:
    config = PipingBagConfig()  # type: ignore[call-arg]
    pg_env = parse_database_url(config.database_url)
    return config, pg_env


def _read_template(name: str) -> str:
    templates = importlib.resources.files("piping_bag") / "templates"
    return (templates / name).read_text(encoding="utf-8")


@app.command()
def init() -> None:
    """Scaffold a new piping_bag project (db/schema.sql, db/queries/, sqlc.yaml, .env)."""
    SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
    QUERIES_DIR.mkdir(parents=True, exist_ok=True)

    files: dict[Path, str] = {
        SCHEMA_PATH: _read_template("schema.sql"),
        QUERIES_DIR / "example.sql": _read_template("example.sql"),
        Path("sqlc.yaml"): _read_template("sqlc.yaml"),
    }

    for path, content in files.items():
        if path.exists():
            typer.echo(f"  exists  {path}")
        else:
            path.write_text(content, encoding="utf-8")
            typer.echo(f"  create  {path}")

    env_path = Path(".env")
    if not env_path.exists():
        env_path.write_text(
            'DATABASE_URL="postgresql://postgres:postgres@localhost:5432/mydb"\n',
            encoding="utf-8",
        )
        typer.echo(f"  create  {env_path}")
    else:
        typer.echo(f"  exists  {env_path}")

    typer.echo("\nDone! Edit .env with your DATABASE_URL, then run: pb up")


@app.command()
def dump() -> None:
    """Dump current database schema to db/schema.sql."""
    _, pg_env = _load_config()
    run(["pgschema", "dump", "--file", str(SCHEMA_PATH)], env=pg_env)


@app.command()
def plan() -> None:
    """Plan migration (diff desired schema vs live database)."""
    _, pg_env = _load_config()
    run(["pgschema", "plan", "--file", str(SCHEMA_PATH)], env=pg_env)


@app.command()
def apply() -> None:
    """Apply planned migration to the database."""
    _, pg_env = _load_config()
    run(["pgschema", "apply", "--file", str(SCHEMA_PATH)], env=pg_env)


@app.command()
def sync() -> None:
    """Plan and apply migration in one step."""
    _, pg_env = _load_config()
    run(["pgschema", "plan", "--file", str(SCHEMA_PATH)], env=pg_env)
    run(["pgschema", "apply", "--file", str(SCHEMA_PATH)], env=pg_env)


def _write_db_init() -> None:
    """Write db/__init__.py with QueriesDep after SQLC generation."""
    db_init = Path("db/__init__.py")
    db_init.write_text(_read_template("db_init.py"), encoding="utf-8")
    typer.echo(f"  create  {db_init}")


@app.command()
def generate() -> None:
    """Generate Python client from SQL queries using SQLC."""
    _load_config()
    run(["sqlc", "generate"])
    _write_db_init()


@app.command()
def up() -> None:
    """Apply schema migration and generate Python client."""
    _, pg_env = _load_config()
    run(["pgschema", "apply", "--file", str(SCHEMA_PATH)], env=pg_env)
    run(["sqlc", "generate"])
    _write_db_init()


@app.command()
def studio() -> None:
    """Introspect database and launch Prisma Studio."""
    _load_config()
    run(["prisma", "db", "pull"])
    run(["prisma", "studio"])


@app.command()
def diff() -> None:
    """Show what pgschema would change (human-readable)."""
    _, pg_env = _load_config()
    run(["pgschema", "plan", "--file", str(SCHEMA_PATH)], env=pg_env)
