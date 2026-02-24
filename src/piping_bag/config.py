from pathlib import Path
from urllib.parse import urlparse

from pydantic_settings import BaseSettings


SCHEMA_PATH = Path("db/schema.sql")
QUERIES_DIR = Path("db/queries")


class PipingBagConfig(BaseSettings):
    database_url: str

    model_config = {"env_file": ".env"}


def parse_database_url(url: str) -> dict[str, str]:
    parsed = urlparse(url)
    env = {
        "PGHOST": parsed.hostname or "localhost",
        "PGPORT": str(parsed.port or 5432),
        "PGDATABASE": parsed.path.lstrip("/"),
        "PGUSER": parsed.username or "postgres",
    }
    if parsed.password:
        env["PGPASSWORD"] = parsed.password
    return env
