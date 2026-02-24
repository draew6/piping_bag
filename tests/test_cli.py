from pathlib import Path

from typer.testing import CliRunner

from piping_bag.cli import app

runner = CliRunner()


class TestInit:
    def test_creates_all_files(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(app, ["init"])
        assert result.exit_code == 0

        assert (tmp_path / "db" / "schema.sql").exists()
        assert (tmp_path / "db" / "queries" / "example.sql").exists()
        assert (tmp_path / "sqlc.yaml").exists()
        assert (tmp_path / ".env").exists()

    def test_schema_sql_content(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        runner.invoke(app, ["init"])
        content = (tmp_path / "db" / "schema.sql").read_text()
        assert "schema" in content.lower()

    def test_example_sql_content(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        runner.invoke(app, ["init"])
        content = (tmp_path / "db" / "queries" / "example.sql").read_text()
        assert "ExampleQuery" in content

    def test_sqlc_yaml_content(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        runner.invoke(app, ["init"])
        content = (tmp_path / "sqlc.yaml").read_text()
        assert "sqlc-gen-better-python" in content
        assert "asyncpg" in content

    def test_env_file_content(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        runner.invoke(app, ["init"])
        content = (tmp_path / ".env").read_text()
        assert "DATABASE_URL" in content

    def test_does_not_overwrite_existing(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        runner.invoke(app, ["init"])

        # Modify a file
        schema = tmp_path / "db" / "schema.sql"
        schema.write_text("-- custom content")

        # Run init again
        result = runner.invoke(app, ["init"])
        assert result.exit_code == 0
        assert "exists" in result.output

        # Verify not overwritten
        assert schema.read_text() == "-- custom content"

    def test_output_messages(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(app, ["init"])
        assert "create" in result.output
        assert "pb up" in result.output
