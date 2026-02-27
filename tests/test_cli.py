from pathlib import Path

from typer.testing import CliRunner

from piping_bag.cli import (
    _discover_generated_classes,
    _generate_db_init,
    _write_db_init,
    app,
)

# Minimal sqlc-generated query class shape
_QUERY_CLASS_TEMPLATE = '''\
class {name}:
    __slots__ = ("_conn",)

    def __init__(self, conn):
        self._conn = conn
'''

_NON_QUERY_CLASS = '''\
class QueryResults:
    def __init__(self, sql, decode_hook):
        self.sql = sql
        self.decode_hook = decode_hook
'''

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


class TestWriteDbInit:
    def _setup_generated(self, tmp_path: Path, modules: dict[str, str]) -> Path:
        """Create db/generated/ with the given module files."""
        generated = tmp_path / "db" / "generated"
        generated.mkdir(parents=True)
        for name, content in modules.items():
            (generated / name).write_text(content, encoding="utf-8")
        return generated

    def test_discover_single_class(self, tmp_path):
        generated = self._setup_generated(tmp_path, {
            "metadata.py": _QUERY_CLASS_TEMPLATE.format(name="Metadata"),
        })
        classes = _discover_generated_classes(generated)
        assert classes == [("metadata", "Metadata")]

    def test_discover_multiple_files(self, tmp_path):
        generated = self._setup_generated(tmp_path, {
            "detail.py": _QUERY_CLASS_TEMPLATE.format(name="Detail"),
            "metadata.py": _QUERY_CLASS_TEMPLATE.format(name="Metadata"),
        })
        classes = _discover_generated_classes(generated)
        assert classes == [("detail", "Detail"), ("metadata", "Metadata")]

    def test_discover_skips_models_and_init(self, tmp_path):
        generated = self._setup_generated(tmp_path, {
            "__init__.py": "",
            "models.py": "class User:\n    pass\n",
            "queries.py": _QUERY_CLASS_TEMPLATE.format(name="Queries"),
        })
        classes = _discover_generated_classes(generated)
        assert classes == [("queries", "Queries")]

    def test_discover_skips_non_query_classes(self, tmp_path):
        generated = self._setup_generated(tmp_path, {
            "queries.py": _NON_QUERY_CLASS + _QUERY_CLASS_TEMPLATE.format(name="Queries"),
        })
        classes = _discover_generated_classes(generated)
        assert classes == [("queries", "Queries")]

    def test_generate_single_class(self):
        content = _generate_db_init([("queries", "Queries")])
        assert "from db.generated.queries import Queries" in content
        assert "class Queries:" in content
        assert "QueriesDep" in content
        assert "self._queries = Queries(conn)" in content

    def test_generate_multi_class_facade(self):
        content = _generate_db_init([("detail", "Detail"), ("metadata", "Metadata")])
        assert "from db.generated.detail import Detail" in content
        assert "from db.generated.metadata import Metadata" in content
        assert "self._detail = Detail(conn)" in content
        assert "self._metadata = Metadata(conn)" in content
        assert "for delegate in (self._detail, self._metadata,):" in content

    def test_write_db_init_creates_file(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        self._setup_generated(tmp_path, {
            "metadata.py": _QUERY_CLASS_TEMPLATE.format(name="Metadata"),
            "posting.py": _QUERY_CLASS_TEMPLATE.format(name="Posting"),
        })
        _write_db_init()
        db_init = tmp_path / "db" / "__init__.py"
        assert db_init.exists()
        content = db_init.read_text()
        assert "class Queries:" in content
        assert "from db.generated.metadata import Metadata" in content
        assert "from db.generated.posting import Posting" in content

    def test_write_db_init_skips_when_no_classes(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        generated = tmp_path / "db" / "generated"
        generated.mkdir(parents=True)
        (generated / "models.py").write_text("class User:\n    pass\n")
        _write_db_init()
        assert not (tmp_path / "db" / "__init__.py").exists()
