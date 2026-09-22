"""
db_fork.py
---------------------------------------------------------------------------
Creates an isolated fork/clone of the operational Postgres database for
sandboxed agentic (natural-language) querying, so ad-hoc exploration via
the MCP-connected assistant can never touch production tables.

Usage:
    python db_fork.py create   # (re)creates the fork from a fresh dump
    python db_fork.py drop     # tears down the fork
---------------------------------------------------------------------------
"""
from __future__ import annotations

import os
import subprocess
import sys

PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_USER = os.environ.get("PG_USER", "walmart_app")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "changeme")
PROD_DB = os.environ.get("PG_DB", "walmart_ops")
FORK_DB = os.environ.get("PG_FORK_DB", "walmart_ops_fork")

ENV = {**os.environ, "PGPASSWORD": PG_PASSWORD}


def _run(cmd: list[str]) -> None:
    print(f"[db_fork] $ {' '.join(cmd)}")
    subprocess.run(cmd, check=True, env=ENV)


def create_fork() -> None:
    """Drops any existing fork, then clones PROD_DB -> FORK_DB via dump/restore."""
    _run(["dropdb", "--if-exists", "-h", PG_HOST, "-p", PG_PORT, "-U", PG_USER, FORK_DB])
    _run(["createdb", "-h", PG_HOST, "-p", PG_PORT, "-U", PG_USER, FORK_DB])
    dump_cmd = ["pg_dump", "-h", PG_HOST, "-p", PG_PORT, "-U", PG_USER, PROD_DB]
    restore_cmd = ["psql", "-h", PG_HOST, "-p", PG_PORT, "-U", PG_USER, FORK_DB]

    dump = subprocess.Popen(dump_cmd, stdout=subprocess.PIPE, env=ENV)
    subprocess.run(restore_cmd, stdin=dump.stdout, check=True, env=ENV)
    dump.wait()
    print(f"[db_fork] fork ready: {FORK_DB}  (read/write sandbox, safe to experiment on)")


def drop_fork() -> None:
    _run(["dropdb", "--if-exists", "-h", PG_HOST, "-p", PG_PORT, "-U", PG_USER, FORK_DB])
    print(f"[db_fork] fork dropped: {FORK_DB}")


if __name__ == "__main__":
    action = sys.argv[1] if len(sys.argv) > 1 else "create"
    if action == "create":
        create_fork()
    elif action == "drop":
        drop_fork()
    else:
        print("usage: python db_fork.py [create|drop]")
        sys.exit(1)
