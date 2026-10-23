"""SQLite helpers for the data/API product MVP.

Tables:
  businesses     — cleaned niche dataset rows (+ freshness tracking columns)
  api_keys       — API keys with plan tier
  usage          — one row per authenticated API call (metering source for billing)
  business_changes — snapshot-diff change events (new / closed / changed)
  snapshot_runs  — one row per refresh run (data pipeline audit trail)

Day-zero baseline: 2026-10-18 (first live ingest date). first_seen/last_seen
are backfilled to day zero for all pre-existing rows. Change history cannot
be reconstructed before day zero and is never invented.
"""
import json
import os
import sqlite3
import time
from pathlib import Path

import pg as _pg  # Postgres-ready layer (no-op unless DATABASE_URL is set)

DB_PATH = Path(__file__).parent / "data.db"

# Honest day-zero: the date of the first live (non-seed) ingest.
DAY_ZERO = "2026-10-18"

SCHEMA = """
CREATE TABLE IF NOT EXISTS businesses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    address TEXT,
    latitude REAL,
    longitude REAL,
    phone TEXT,
    website TEXT,
    opening_hours TEXT,
    source TEXT NOT NULL DEFAULT 'seed',
    first_seen TEXT,
    last_seen TEXT,
    status TEXT NOT NULL DEFAULT 'active'
);
CREATE TABLE IF NOT EXISTS api_keys (
    key TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    tier TEXT NOT NULL DEFAULT 'free',
    active INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    api_key TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    timestamp INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS business_changes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    business_key TEXT NOT NULL,
    business_id INTEGER,
    change_type TEXT NOT NULL,
    changed_fields TEXT,
    detected_on TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS snapshot_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date TEXT NOT NULL,
    source TEXT NOT NULL,
    rows_fetched INTEGER NOT NULL DEFAULT 0,
    records_new INTEGER NOT NULL DEFAULT 0,
    records_closed INTEGER NOT NULL DEFAULT 0,
    records_changed INTEGER NOT NULL DEFAULT 0,
    records_unchanged INTEGER NOT NULL DEFAULT 0,
    notes TEXT
);
CREATE TABLE IF NOT EXISTS permits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset TEXT NOT NULL,
    external_key TEXT NOT NULL,
    permit_number TEXT,
    event_date TEXT,
    permit_type TEXT,
    status TEXT,
    description TEXT,
    address TEXT,
    neighbourhood TEXT,
    zoning TEXT,
    construction_value REAL,
    units_added REAL,
    latitude REAL,
    longitude REAL,
    first_seen TEXT,
    last_seen TEXT,
    record_status TEXT NOT NULL DEFAULT 'active',
    raw_json TEXT,
    source TEXT NOT NULL DEFAULT 'edmonton_open_data'
);
CREATE TABLE IF NOT EXISTS permit_changes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    permit_id INTEGER,
    dataset TEXT NOT NULL,
    external_key TEXT NOT NULL,
    change_type TEXT NOT NULL,
    changed_fields TEXT,
    detected_on TEXT NOT NULL,
    summary TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS permit_snapshot_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date TEXT NOT NULL,
    dataset TEXT NOT NULL,
    rows_fetched INTEGER NOT NULL DEFAULT 0,
    new INTEGER NOT NULL DEFAULT 0,
    closed_expired INTEGER NOT NULL DEFAULT 0,
    changed INTEGER NOT NULL DEFAULT 0,
    unchanged INTEGER NOT NULL DEFAULT 0,
    notes TEXT
);
"""

INDEXES = """
CREATE INDEX IF NOT EXISTS idx_usage_key_ts ON usage (api_key, timestamp);
CREATE INDEX IF NOT EXISTS idx_businesses_category ON businesses (category);
CREATE INDEX IF NOT EXISTS idx_businesses_status ON businesses (status);
CREATE INDEX IF NOT EXISTS idx_businesses_first_seen ON businesses (first_seen);
CREATE INDEX IF NOT EXISTS idx_changes_detected ON business_changes (detected_on DESC);
CREATE INDEX IF NOT EXISTS idx_changes_type ON business_changes (change_type);
CREATE INDEX IF NOT EXISTS idx_snapshot_runs_date ON snapshot_runs (run_date DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_permits_identity ON permits (dataset, external_key);
CREATE INDEX IF NOT EXISTS idx_permits_status ON permits (record_status);
CREATE INDEX IF NOT EXISTS idx_permits_dataset_status ON permits (dataset, record_status);
CREATE INDEX IF NOT EXISTS idx_permits_event_date ON permits (event_date DESC);
CREATE INDEX IF NOT EXISTS idx_permits_neighbourhood ON permits (neighbourhood);
CREATE INDEX IF NOT EXISTS idx_permits_value ON permits (construction_value DESC);
CREATE INDEX IF NOT EXISTS idx_permit_changes_detected ON permit_changes (detected_on DESC);
CREATE INDEX IF NOT EXISTS idx_permit_changes_type ON permit_changes (change_type);
CREATE INDEX IF NOT EXISTS idx_permit_changes_dataset ON permit_changes (dataset);
CREATE INDEX IF NOT EXISTS idx_permit_changes_detected_type ON permit_changes (detected_on DESC, change_type, dataset);
CREATE INDEX IF NOT EXISTS idx_permit_snapshot_runs ON permit_snapshot_runs (run_date DESC, dataset);
CREATE INDEX IF NOT EXISTS idx_businesses_name ON businesses (name);
CREATE INDEX IF NOT EXISTS idx_businesses_status_category ON businesses (status, category);
CREATE INDEX IF NOT EXISTS idx_changes_detected_type ON business_changes (detected_on DESC, change_type);
"""


def get_conn(path: "str | Path | None" = None):
    """Open a DB connection.

    Default backend is SQLite (data.db). Set DATABASE_URL to a
    postgres:// URL to use Postgres instead (see pg.py for coverage).
    An explicit `path` always means a local SQLite file (used by tests).
    """
    if path is None and _pg.is_pg():
        return _pg.connect()
    conn = sqlite3.connect(path or DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _table_columns(conn, table: str) -> set:
    if isinstance(conn, _pg.PGConn):
        rows = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = %s",
            (table,),
        ).fetchall()
        return {r["column_name"] for r in rows}
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}


def migrate_freshness(conn: sqlite3.Connection) -> None:
    """Idempotent freshness migration: add tracking columns, backfill day zero.

    Backfill is honest: all rows that pre-date the freshness engine get
    first_seen/last_seen = DAY_ZERO, the date of the first live ingest.
    No change history is fabricated for earlier dates.
    """
    cols = _table_columns(conn, "businesses")
    if "first_seen" not in cols:
        conn.execute("ALTER TABLE businesses ADD COLUMN first_seen TEXT")
    if "last_seen" not in cols:
        conn.execute("ALTER TABLE businesses ADD COLUMN last_seen TEXT")
    if "status" not in cols:
        conn.execute("ALTER TABLE businesses ADD COLUMN status TEXT")
    conn.execute(
        "UPDATE businesses SET first_seen = ? WHERE first_seen IS NULL",
        (DAY_ZERO,),
    )
    conn.execute(
        "UPDATE businesses SET last_seen = ? WHERE last_seen IS NULL",
        (DAY_ZERO,),
    )
    conn.execute(
        "UPDATE businesses SET status = 'active' WHERE status IS NULL OR status = ''"
    )


def init_db(path: "str | Path | None" = None) -> None:
    if path is None and _pg.is_pg():
        # Postgres backend: dedicated DDL (SERIAL ids, same indexes).
        _pg.init_schema()
        return
    with get_conn(path) as conn:
        # WAL: readers (API) don't block on the daily refresh writer, and
        # vice versa. Persistent in the DB file; harmless to re-apply.
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.executescript(SCHEMA)
        migrate_freshness(conn)  # adds tracking columns before indexes
        conn.executescript(INDEXES)


def ensure_demo_key(path: "str | Path | None" = None) -> str:
    """Create the documented demo key on first run; return it.

    The demo key lives on the 'demo' plan (5,000 calls/month, see
    billing.TIERS) so demo/verification traffic keeps working while abuse
    stays bounded.
    """
    demo_key = "demo_edmonton_2026"
    now = int(time.time())
    with get_conn(path) as conn:
        row = conn.execute(
            "SELECT key, tier FROM api_keys WHERE key = ?", (demo_key,)
        ).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO api_keys (key, name, tier, active, created_at)"
                " VALUES (?, ?, 'demo', 1, ?)",
                (demo_key, "Demo key", now),
            )
        elif row["tier"] != "demo":
            conn.execute(
                "UPDATE api_keys SET tier = 'demo' WHERE key = ?", (demo_key,)
            )
    return demo_key


def log_usage(api_key: str, endpoint: str, path: "str | Path | None" = None) -> None:
    with get_conn(path) as conn:
        conn.execute(
            "INSERT INTO usage (api_key, endpoint, timestamp) VALUES (?, ?, ?)",
            (api_key, endpoint, int(time.time())),
        )


def key_is_valid(api_key: str, path: "str | Path | None" = None) -> bool:
    with get_conn(path) as conn:
        row = conn.execute(
            "SELECT 1 FROM api_keys WHERE key = ? AND active = 1", (api_key,)
        ).fetchone()
        return row is not None


def key_tier(api_key: str, path: "str | Path | None" = None) -> str:
    with get_conn(path) as conn:
        row = conn.execute(
            "SELECT tier FROM api_keys WHERE key = ?", (api_key,)
        ).fetchone()
        return row["tier"] if row else "free"


def monthly_usage(api_key: str, path: "str | Path | None" = None) -> int:
    """Number of calls this key made since the start of the current UTC month."""
    now = time.time()
    lt = time.gmtime(now)
    start = time.mktime(
        (lt.tm_year, lt.tm_mon, 1, 0, 0, 0, 0, 0, -1)
    )
    with get_conn(path) as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM usage WHERE api_key = ? AND timestamp >= ?",
            (api_key, int(start)),
        ).fetchone()
        return row["c"]


def business_key(name: "str | None", address: "str | None") -> str:
    """Stable identity for diffing: normalized name + address.

    Uses JSON of the pair (pipe separators are ambiguous with real
    punctuation). Matches the dedupe key used by the original ingest.
    """
    return json.dumps(
        [((name or "").strip().lower()), ((address or "").strip().lower())]
    )
