from unittest.mock import patch

import click
import pytest

from piping_bag.runner import check_binary, run


class TestCheckBinary:
    def test_existing_binary(self):
        check_binary("python3")

    def test_missing_binary_exits(self):
        with pytest.raises(click.exceptions.Exit):
            check_binary("nonexistent_binary_xyz")

    def test_missing_pgschema_shows_hint(self, capsys):
        with pytest.raises(click.exceptions.Exit):
            check_binary("pgschema")
        captured = capsys.readouterr()
        assert "pip install pgschema" in captured.err

    def test_missing_sqlc_shows_hint(self, capsys):
        with patch("piping_bag.runner.shutil.which", return_value=None):
            with pytest.raises(click.exceptions.Exit):
                check_binary("sqlc")
        captured = capsys.readouterr()
        assert "sqlc" in captured.err


class TestRun:
    def test_successful_command(self):
        result = run(["echo", "hello"])
        assert result.returncode == 0

    def test_env_vars_passed(self):
        result = run(["env"], env={"MY_TEST_VAR": "test_value"})
        assert result.returncode == 0

    def test_failed_command_exits(self):
        with pytest.raises(click.exceptions.Exit):
            run(["false"])

    def test_missing_binary_exits(self):
        with pytest.raises(click.exceptions.Exit):
            run(["nonexistent_binary_xyz", "arg1"])
