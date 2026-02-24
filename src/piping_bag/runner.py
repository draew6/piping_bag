import os
import shutil
import subprocess
from pathlib import Path

import typer


def check_binary(name: str) -> None:
    if shutil.which(name) is None:
        hints = {
            "pgschema": "Install pgschema: pip install pgschema",
            "sqlc": "Install sqlc: https://docs.sqlc.dev/en/latest/overview/install.html",
            "prisma": "Install prisma: pip install prisma",
            "harlequin": "Install harlequin: pip install harlequin[postgres]",
        }
        hint = hints.get(name, f"Install {name} and make sure it's on your PATH.")
        typer.echo(f"Error: '{name}' not found on PATH.\n{hint}", err=True)
        raise typer.Exit(1)


def run(
    cmd: list[str],
    env: dict[str, str] | None = None,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    check_binary(cmd[0])

    merged_env = {**os.environ}
    if env:
        merged_env.update(env)

    result = subprocess.run(cmd, env=merged_env, cwd=cwd, text=True)

    if result.returncode != 0:
        typer.echo(f"Command failed: {' '.join(cmd)}", err=True)
        raise typer.Exit(result.returncode)

    return result
