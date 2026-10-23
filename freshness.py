"""Freshness engine: snapshot-diff change tracking for the business dataset.

Pipeline model:
  refresh() -> fetch current source data (polite paging, same as ingest.py)
              -> dedupe identically to ingest.py
              -> diff against data.db by stable business_key
              -> record new / closed / changed events + one snapshot_runs row

Matching (two passes, to survive relocations):
  Pass 1 — exact key match on (name, address): unchanged, or 'changed' if
           category/coords differ.
  Pass 2 — unmatched fetched records are matched by normalized NAME against
           unmatched active DB rows (name-only key). A unique name match
           means the same business moved/renamed-address: 'changed' with the
           address (and any other differing) fields. Ambiguous names are left
           as new/closed rather than guessed.

Closed rows are kept in the table with status='closed' (never deleted):
the history stays auditable, and a closed business that reappears gets a
'new' event. Normal /v1/businesses results exclude closed rows by default.

refresh() is idempotent: re-running it against the same source data adds
no new change events and only refreshes last_seen.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import date
from pathlib import Path

from db import DB_PATH, business_key, init_db, migrate_freshness
from ingest import SOURCE_NAME, fetch_edmonton_open_data

# Fields compared for 'changed' detection (name/address are the identity).
TRACKED_FIELDS = ("category", "address", "latitude", "longitude")

# Coordinate shifts smaller than this (in degrees; 0.0009 ≈ 100 m at
# Edmonton's latitude) are city re-geocoding jitter, not a business move.
# Without this, ~70% of daily "changed" rows are 10-metre geocode wobbles.
COORD_JITTER_THRESHOLD = 0.0009


def _norm(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value).strip()


def _coord_moved(old, new) -> bool:
    """True only if the coordinate shift exceeds the jitter threshold."""
    try:
        return abs(float(old) - float(new)) > COORD_JITTER_THRESHOLD
    except (TypeError, ValueError):
        # Missing or non-numeric coordinate: fall back to plain comparison.
        return _norm(old) != _norm(new)


def _fields_differ(a: dict, b: dict) -> list[dict]:
    """Return [{field, old, new}] for tracked fields that differ."""
    diffs = []
    for field in TRACKED_FIELDS:
        if field in ("latitude", "longitude"):
            moved = _coord_moved(a.get(field), b.get(field))
        else:
            moved = _norm(a.get(field)) != _norm(b.get(field))
        if moved:
            diffs.append(
                {"field": field, "old": a.get(field), "new": b.get(field)}
            )
    return diffs


def refresh(
    db_path: "str | Path | None" = None,
    source_data: "list[dict] | None" = None,
    source_name: str = SOURCE_NAME,
    today: "str | None" = None,
) -> dict:
    """Run one snapshot-diff cycle. Returns a summary dict."""
    today = today or date.today().isoformat()
    target = db_path or DB_PATH
    init_db(target)

    records = (
        source_data if source_data is not None else fetch_edmonton_open_data()
    )
    rows_fetched = len(records)

    conn = sqlite3.connect(target)
    conn.row_factory = sqlite3.Row
    migrate_freshness(conn)

    # Current DB rows for this source, keyed by identity.
    db_rows = conn.execute(
        "SELECT * FROM businesses WHERE source = ?", (source_name,)
    ).fetchall()
    db_by_key = {business_key(r["name"], r["address"]): dict(r) for r in db_rows}

    new_n = closed_n = changed_n = unchanged_n = 0

    # ---- Write batching (scale pass): collect row operations during the
    # diff and flush with executemany in a few statements instead of one
    # execute() per row. Diff SEMANTICS are untouched — same SQL, same
    # parameters, same order of effects; only the write pattern changed.
    # New-business INSERTs stay per-row (we need cursor.lastrowid to link
    # their change events); in steady state these are rare.
    changed_updates: list = []   # full-row UPDATE for 'changed' rows
    touched_updates: list = []   # (today, id) last_seen touch, unchanged rows
    closed_updates: list = []    # (today, id) for closed rows
    new_events: list = []        # business_changes rows, in diff order

    def queue_change(bkey, business_id, change_type, changed_fields):
        new_events.append(
            (
                bkey,
                business_id,
                change_type,
                json.dumps(changed_fields) if changed_fields else None,
                today,
                source_name,
            )
        )

    # ---- Pass 1: exact (name, address) matches ---------------------------
    matched_fetched: set[str] = set()
    matched_db: set[str] = set()

    for rec in records:
        bkey = business_key(rec.get("name"), rec.get("address"))
        row = db_by_key.get(bkey)
        if row is None:
            continue
        matched_fetched.add(bkey)
        matched_db.add(bkey)
        diffs = _fields_differ(row, rec)
        if diffs:
            changed_updates.append(
                (
                    rec.get("category"), rec.get("address"),
                    rec.get("latitude"), rec.get("longitude"),
                    rec.get("phone"), rec.get("website"),
                    rec.get("opening_hours"), today, row["id"],
                )
            )
            queue_change(bkey, row["id"], "changed", diffs)
            changed_n += 1
        else:
            touched_updates.append((today, row["id"]))
            unchanged_n += 1

    # ---- Pass 2: match remaining fetched records by name alone -----------
    # Index unmatched ACTIVE db rows by normalized name.
    name_index: dict[str, list] = {}
    for bkey, row in db_by_key.items():
        if bkey in matched_db or row["status"] != "active":
            continue
        name_index.setdefault(((row["name"] or "").strip().lower()), []).append(
            (bkey, row)
        )

    for rec in records:
        bkey = business_key(rec.get("name"), rec.get("address"))
        if bkey in matched_fetched:
            continue
        candidates = name_index.get(((rec.get("name") or "").strip().lower()), [])
        if len(candidates) != 1:
            continue  # ambiguous or unknown name -> handled as new below
        old_bkey, row = candidates[0]
        matched_fetched.add(bkey)
        matched_db.add(old_bkey)
        diffs = _fields_differ(row, rec)
        changed_updates.append(
            (
                rec.get("category"), rec.get("address"),
                rec.get("latitude"), rec.get("longitude"),
                rec.get("phone"), rec.get("website"),
                rec.get("opening_hours"), today, row["id"],
            )
        )
        # Identity key changed (moved address): always a 'changed' event.
        queue_change(bkey, row["id"], "changed", diffs or [
            {"field": "address", "old": row["address"], "new": rec.get("address")}
        ])
        changed_n += 1

    # ---- Remaining fetched = new businesses -------------------------------
    for rec in records:
        bkey = business_key(rec.get("name"), rec.get("address"))
        if bkey in matched_fetched:
            continue
        cur = conn.execute(
            """INSERT INTO businesses
               (name, category, address, latitude, longitude, phone, website,
                opening_hours, source, first_seen, last_seen, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active')""",
            (
                rec.get("name"), rec.get("category"), rec.get("address"),
                rec.get("latitude"), rec.get("longitude"),
                rec.get("phone"), rec.get("website"), rec.get("opening_hours"),
                rec.get("source", source_name), today, today,
            ),
        )
        queue_change(bkey, cur.lastrowid, "new", None)
        new_n += 1

    # ---- Remaining active DB rows = closed businesses ---------------------
    for bkey, row in db_by_key.items():
        if bkey in matched_db or row["status"] != "active":
            continue
        closed_updates.append((today, row["id"]))
        queue_change(bkey, row["id"], "closed", None)
        closed_n += 1

    # ---- Flush all queued writes in a few batched statements -------------
    if changed_updates:
        conn.executemany(
            """UPDATE businesses
               SET category = ?, address = ?, latitude = ?, longitude = ?,
                   phone = ?, website = ?, opening_hours = ?,
                   last_seen = ?, status = 'active'
               WHERE id = ?""",
            changed_updates,
        )
    if touched_updates:
        conn.executemany(
            "UPDATE businesses SET last_seen = ?, status = 'active' WHERE id = ?",
            touched_updates,
        )
    if closed_updates:
        conn.executemany(
            "UPDATE businesses SET status = 'closed', last_seen = ? WHERE id = ?",
            closed_updates,
        )
    if new_events:
        conn.executemany(
            """INSERT INTO business_changes
               (business_key, business_id, change_type, changed_fields,
                detected_on, source)
               VALUES (?, ?, ?, ?, ?, ?)""",
            new_events,
        )

    conn.execute(
        """INSERT INTO snapshot_runs
           (run_date, source, rows_fetched, records_new, records_closed,
            records_changed, records_unchanged, notes)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            today, source_name, rows_fetched, new_n, closed_n, changed_n,
            unchanged_n, "daily snapshot-diff",
        ),
    )
    conn.commit()
    conn.close()

    return {
        "run_date": today,
        "source": source_name,
        "rows_fetched": rows_fetched,
        "records_new": new_n,
        "records_closed": closed_n,
        "records_changed": changed_n,
        "records_unchanged": unchanged_n,
    }
