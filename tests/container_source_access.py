"""Image smoke check; pipe into the built image's Python as its default user.

Example (no database or network is used):
    docker run --rm -i --network none --read-only \
        --tmpfs /tmp:rw,nosuid,nodev,noexec,size=64m --entrypoint .venv/bin/python \
        first-migration - < tests/container_source_access.py

For the restrictive-umask regression, also run against an image built from a
disposable checkout with package files 0600 and directories 0700.
"""

import os
import stat
import subprocess
from pathlib import Path


def main() -> None:
    if os.geteuid() == 0:
        raise SystemExit("Run the smoke check as the configured non-root image user")
    root = Path("/app/packages")
    checked = 0
    for path in (root, *root.rglob("*")):
        info = path.stat()
        if info.st_uid != 0 or info.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
            raise SystemExit(f"Source ownership/write permissions are unsafe: {path}")
        if path.is_dir():
            if not os.access(path, os.R_OK | os.X_OK):
                raise SystemExit(f"Source directory is inaccessible: {path}")
        elif path.is_file():
            with path.open("rb") as stream:
                stream.read(1)
        checked += 1

    # Unlike `alembic heads`, offline upgrade loads both env.py and revision
    # source. A fake URL plus --network none prevents a real DB connection.
    env = os.environ | {
        "FIRST_DB_URL": "postgresql+psycopg://unused:unused@127.0.0.1:1/unused",
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    result = subprocess.run(
        [
            "/app/.venv/bin/alembic",
            "-c",
            "packages/gateway/first_gateway/database/alembic.ini",
            "upgrade",
            "head",
            "--sql",
        ],
        env=env,
        cwd="/app",
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    )
    if "CREATE TABLE first.launch_template" not in result.stdout:
        raise SystemExit("Offline SQL did not include the current launch schema")
    print(
        f"PASS: {checked} source paths accessible to non-root; offline upgrade generated"
    )


if __name__ == "__main__":
    main()
