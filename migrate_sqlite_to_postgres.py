"""
Simple table-by-table migration helper from the ELA GovCon SQLite DB to Postgres.

Usage (example):

    export DATABASE_URL="postgresql://user:pass@host:5432/govcon"
    python migrate_sqlite_to_postgres.py data/govcon.db

The script expects that the target Postgres database already has the schema
created (tables, columns, constraints). It will:

- Read all non-internal tables from the SQLite file
- Copy rows into the matching Postgres tables, preserving primary key IDs
- Disable and re-enable constraint checks per table to reduce friction

You should run this against a *test* Postgres instance first.
"""

import sys
import sqlite3
from pathlib import Path
from typing import List, Tuple, Any

import os

try:
    import psycopg2  # type: ignore
    from psycopg2.extras import execute_values  # type: ignore
except Exception as e:  # pragma: no cover
    raise RuntimeError("psycopg2 is required for Postgres migration but is not installed.") from e


def _get_sqlite_tables(conn: sqlite3.Connection) -> List[str]:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
    rows = cur.fetchall()
    return [r[0] for r in rows]


def _get_table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table});")
    rows = cur.fetchall()
    # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
    return [r[1] for r in rows]


def migrate_table(sqlite_conn: sqlite3.Connection, pg_conn, table: str) -> int:
    cols = _get_table_columns(sqlite_conn, table)
    if not cols:
        return 0
    col_list = ", ".join(cols)
    placeholders = ", ".join([f"%s" for _ in cols])

    src_cur = sqlite_conn.cursor()
    src_cur.execute(f"SELECT {col_list} FROM {table};")
    rows = src_cur.fetchall()
    if not rows:
        return 0

    with pg_conn.cursor() as cur:
        # Try to relax constraints per table to reduce migration friction.
        try:
            cur.execute(f"ALTER TABLE {table} DISABLE TRIGGER ALL;")
        except Exception:
            pass
        try:
            execute_values(
                cur,
                f"INSERT INTO {table} ({col_list}) VALUES %s",
                rows,
            )
        finally:
            try:
                cur.execute(f"ALTER TABLE {table} ENABLE TRIGGER ALL;")
            except Exception:
                pass
    pg_conn.commit()
    return len(rows)


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python migrate_sqlite_to_postgres.py path/to/govcon.db")
        sys.exit(1)

    sqlite_path = Path(sys.argv[1])
    if not sqlite_path.exists():
        print(f"SQLite file not found: {sqlite_path}")
        sys.exit(1)

    db_url = (os.getenv("DATABASE_URL") or "").strip()
    if not db_url or not (db_url.startswith("postgres://") or db_url.startswith("postgresql://")):
        print("DATABASE_URL must be set to a Postgres DSN for migration (postgres:// or postgresql://).")
        sys.exit(1)

    print(f"Opening SQLite DB at {sqlite_path} ...")
    s_conn = sqlite3.connect(str(sqlite_path))
    try:
        print(f"Connecting to Postgres at {db_url!r} ...")
        pg_conn = psycopg2.connect(db_url)
    except Exception as e:
        print(f"Failed to connect to Postgres: {e}")
        sys.exit(1)

    try:
        tables = _get_sqlite_tables(s_conn)
        print(f"Found {len(tables)} tables to migrate: {', '.join(tables)}")
        total_rows = 0
        for t in tables:
            print(f"Migrating table {t} ...", end=" ", flush=True)
            try:
                moved = migrate_table(s_conn, pg_conn, t)
                print(f"{moved} rows")
                total_rows += moved
            except Exception as e:
                print(f"ERROR: {e}")
        print(f"Done. Total rows inserted into Postgres: {total_rows}")
    finally:
        try:
            s_conn.close()
        except Exception:
            pass
        try:
            pg_conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
