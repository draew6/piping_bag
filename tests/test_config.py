import os

import pytest

from piping_bag.config import PipingBagConfig, parse_database_url


class TestParseDatabaseUrl:
    def test_full_url(self):
        env = parse_database_url("postgresql://myuser:mypass@db.example.com:5433/mydb")
        assert env == {
            "PGHOST": "db.example.com",
            "PGPORT": "5433",
            "PGDATABASE": "mydb",
            "PGUSER": "myuser",
            "PGPASSWORD": "mypass",
        }

    def test_defaults(self):
        env = parse_database_url("postgresql:///testdb")
        assert env["PGHOST"] == "localhost"
        assert env["PGPORT"] == "5432"
        assert env["PGDATABASE"] == "testdb"
        assert env["PGUSER"] == "postgres"
        assert "PGPASSWORD" not in env

    def test_no_password(self):
        env = parse_database_url("postgresql://user@localhost:5432/db")
        assert env["PGUSER"] == "user"
        assert "PGPASSWORD" not in env

    def test_default_port(self):
        env = parse_database_url("postgresql://user:pass@host/db")
        assert env["PGPORT"] == "5432"


class TestPipingBagConfig:
    def test_loads_from_env(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test")
        config = PipingBagConfig()
        assert config.database_url == "postgresql://localhost/test"

    def test_missing_database_url_raises(self, monkeypatch):
        monkeypatch.delenv("DATABASE_URL", raising=False)
        # Also prevent reading from any .env file
        monkeypatch.chdir("/tmp")
        with pytest.raises(Exception):
            PipingBagConfig()
