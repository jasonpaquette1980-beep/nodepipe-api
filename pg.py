"""Postgres-ready data layer for Nodepipe (prepared, NOT migrated).

Set DATABASE_URL to a postgres:// URL and db.get_conn() returns a Postgres
connection instead of SQLite. Production stays on SQLite until the flip is
deliberately made (see README "Scaling path").

COVERED by the shim:
  - db.py: schema creation, migrations, api_keys/usage helpers, WAL skipped.
  - main.py: all read endpoints (SELECT/COUNT with ? placeholders, LIMIT,
    OFFSET, LOWER/LIKE, COALESCE, and date('now', ?) interval math).
  - billing.py estimate_bill (uses the db.py helpers).

NOT covered (still SQLite-only; they open sqlite3 connections directly or
rely on sqlite-isms):
  - freshness.refresh() / permits.refresh_permits() — the daily refresh
    writers (use cursor.lastrowid and PRAGMA-style access).
  - ingest.py — the bulk loaders.
  - permits.write_daily_outputs() when handed a sqlite connection.

Dialect translation is intentionally narrow: "?" -> "%s" plus the one
date('now', ?) pattern actually used. Anything else raises from psycopg
with the real SQL visible, so gaps are loud, not silent.

Rate limiting stays in-process (ratelimit.py) on both backends; move it to
Redis before running multiple API workers.
"""
from __future__ import annotations

import os
import re

# ---------------------------------------------------------------- detection
def database_url() -> str:
    return os.environ.get("DATABASE_URL", "")


def is_pg() -> bool:
    return database_url().startswith(("postgres://", "postgresql://"))


# ------------------------------------------------------------- SQL shim
_QMARK = re.compile(r"\?")
_DATE_NOW = re.compile(r"(?i)date\('now',\s*%s\)")


def translate(sql: str) -> str:
    """Translate the sqlite dialect subset this codebase uses to Postgres."""
    sql = _QMARK.sub("%s", sql)
    sql = _DATE_NOW.sub(r"(CURRENT_DATE + CAST(%s AS INTERVAL))", sql)
    return sql


# ------------------------------------------------------------------ DDL
# Same tables as db.SCHEMA, Postgres types. ids are SERIAL; the refresh
# writers are not PG-covered, so sequence gaps on conflict are a non-issue.
SCHEMA_PG = [
    """CREATE TABLE IF NOT EXISTS businesses (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL, category TEXT NOT NULL, address TEXT,
        latitude DOUBLE PRECISION, longitude DOUBLE PRECISION,
        phone TEXT, website TEXT, opening_hours TEXT,
        source TEXT NOT NULL DEFAULT 'seed',
        first_seen TEXT, last_seen TEXT,
        status TEXT NOT NULL DEFAULT 'active')""",
    """CREATE TABLE IF NOT EXISTS api_keys (
        key TEXT PRIMARY KEY, name TEXT NOT NULL,
        tier TEXT NOT NULL DEFAULT 'free',
        active INTEGER NOT NULL DEFAULT 1,
        created_at BIGINT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS usage (
        id SERIAL PRIMARY KEY, api_key TEXT NOT NULL,
        endpoint TEXT NOT NULL, timestamp BIGINT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS business_changes (
        id SERIAL PRIMARY KEY, business_key TEXT NOT NULL,
        business_id INTEGER, change_type TEXT NOT NULL,
        changed_fields TEXT, detected_on TEXT NOT NULL,
        source TEXT NOT NULL DEFAULT '')""",
    """CREATE TABLE IF NOT EXISTS snapshot_runs (
        id SERIAL PRIMARY KEY, run_date TEXT NOT NULL,
        source TEXT NOT NULL, rows_fetched INTEGER NOT NULL DEFAULT 0,
        records_new INTEGER NOT NULL DEFAULT 0,
        records_closed INTEGER NOT NULL DEFAULT 0,
        records_changed INTEGER NOT NULL DEFAULT 0,
        records_unchanged INTEGER NOT NULL DEFAULT 0, notes TEXT)""",
    """CREATE TABLE IF NOT EXISTS permits (
        id SERIAL PRIMARY KEY, dataset TEXT NOT NULL,
        external_key TEXT NOT NULL, permit_number TEXT, event_date TEXT,
        permit_type TEXT, status TEXT, description TEXT, address TEXT,
        neighbourhood TEXT, zoning TEXT,
        construction_value DOUBLE PRECISION, units_added DOUBLE PRECISION,
        latitude DOUBLE PRECISION, longitude DOUBLE PRECISION,
        first_seen TEXT, last_seen TEXT,
        record_status TEXT NOT NULL DEFAULT 'active',
        raw_json TEXT, source TEXT NOT NULL DEFAULT 'edmonton_open_data')""",
    """CREATE TABLE IF NOT EXISTS permit_changes (
        id SERIAL PRIMARY KEY, permit_id INTEGER,
        dataset TEXT NOT NULL, external_key TEXT NOT NULL,
        change_type TEXT NOT NULL, changed_fields TEXT,
        detected_on TEXT NOT NULL, summary TEXT NOT NULL,
        created_at BIGINT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS permit_snapshot_runs (
        id SERIAL PRIMARY KEY, run_date TEXT NOT NULL,
        dataset TEXT NOT NULL, rows_fetched INTEGER NOT NULL DEFAULT 0,
        new INTEGER NOT NULL DEFAULT 0,
        closed_expired INTEGER NOT NULL DEFAULT 0,
        changed INTEGER NOT NULL DEFAULT 0,
        unchanged INTEGER NOT NULL DEFAULT 0, notes TEXT)""",
]

# Same index set as db.INDEXES (Postgres supports IF NOT EXISTS here too).
INDEXES_PG = [
    "CREATE INDEX IF NOT EXISTS idx_usage_key_ts ON usage (api_key, timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_businesses_category ON businesses (category)",
    "CREATE INDEX IF NOT EXISTS idx_businesses_status ON businesses (status)",
    "CREATE INDEX IF NOT EXISTS idx_businesses_first_seen ON businesses (first_seen)",
    "CREATE INDEX IF NOT EXISTS idx_businesses_name ON businesses (name)",
    "CREATE INDEX IF NOT EXISTS idx_businesses_status_category ON businesses (status, category)",
    "CREATE INDEX IF NOT EXISTS idx_changes_detected ON business_changes (detected_on DESC)",
    "CREATE INDEX IF NOT EXISTS idx_changes_type ON business_changes (change_type)",
    "CREATE INDEX IF NOT EXISTS idx_changes_detected_type ON business_changes (detected_on DESC, change_type)",
    "CREATE INDEX IF NOT EXISTS idx_snapshot_runs_date ON snapshot_runs (run_date DESC)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_permits_identity ON permits (dataset, external_key)",
    "CREATE INDEX IF NOT EXISTS idx_permits_status ON permits (record_status)",
    "CREATE INDEX IF NOT EXISTS idx_permits_dataset_status ON permits (dataset, record_status)",
    "CREATE INDEX IF NOT EXISTS idx_permits_event_date ON permits (event_date DESC)",
    "CREATE INDEX IF NOT EXISTS idx_permits_neighbourhood ON permits (neighbourhood)",
    "CREATE INDEX IF NOT EXISTS idx_permits_value ON permits (construction_value DESC)",
    "CREATE INDEX IF NOT EXISTS idx_permit_changes_detected ON permit_changes (detected_on DESC)",
    "CREATE INDEX IF NOT EXISTS idx_permit_changes_type ON permit_changes (change_type)",
    "CREATE INDEX IF NOT EXISTS idx_permit_changes_dataset ON permit_changes (dataset)",
    "CREATE INDEX IF NOT EXISTS idx_permit_changes_detected_type ON permit_changes (detected_on DESC, change_type, dataset)",
    "CREATE INDEX IF NOT EXISTS idx_permit_snapshot_runs ON permit_snapshot_runs (run_date DESC, dataset)",
]


# ------------------------------------------------------------- wrappers
class PGCursor:
    """Thin wrapper: translates our sqlite-dialect SQL, dict rows."""

    def __init__(self, cur):
        self._cur = cur

    def execute(self, sql, params=()):
        stmt = translate(sql)
        self._cur.execute(stmt, params or None)
        return self

    def executemany(self, sql, seq):
        stmt = translate(sql)
        self._cur.executemany(stmt, seq)
        return self

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    @property
    def lastrowid(self):  # pragma: no cover - refresh paths are sqlite-only
        raise NotImplementedError(
            "Postgres path: cursor.lastrowid is unsupported; use "
            "INSERT ... RETURNING id. The refresh writers are not PG-covered."
        )


class PGConn:
    """Connection wrapper mirroring the sqlite3 bits main.py/db.py use."""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=()):
        return PGCursor(self._conn.cursor()).execute(sql, params)

    def executemany(self, sql, seq):
        return PGCursor(self._conn.cursor()).executemany(sql, seq)

    def executescript(self, sql):  # kept for API parity; PG uses lists
        for stmt in [s for s in sql.split(";") if s.strip()]:
            self.execute(stmt)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                self._conn.commit()
            else:
                self._conn.rollback()
        finally:
            self._conn.close()
        return False


def connect() -> PGConn:
    """Open a Postgres connection (dict rows). Requires psycopg."""
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as e:  # pragma: no cover
        raise RuntimeError(
            "DATABASE_URL is set but psycopg is not installed. "
            "pip install -r requirements.txt"
        ) from e
    conn = psycopg.connect(database_url(), row_factory=dict_row)
    return PGConn(conn)


def init_schema() -> None:
    """Create all tables + indexes on the Postgres backend (idempotent)."""
    conn = connect()
    try:
        for stmt in SCHEMA_PG + INDEXES_PG:
            conn.execute(stmt)
        conn.commit()
    finally:
        conn.close()
