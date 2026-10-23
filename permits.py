"""Permits change tracking for Nodepipe: a second freshness feed.

Two City of Edmonton open datasets, same pipeline as the business feed:
  building    — General Building Permits (Socrata 24uj-dj8v): ~248k issued
                permits with construction dollar value, work type, address.
  development — Development Permits (Socrata 2ccn-pwtu): ~121k permits with
                a status field (Approved / In Progress / Refused / Appealed).

Both datasets are refreshed daily by the existing refresh.py schedule.
Diff rules (per product spec):
  - strings normalized (trim, collapse whitespace, casefold) before compare;
    formatting-only noise never flags a change;
  - latitude/longitude shifts under ~100 m are city re-geocoding jitter and
    are silently updated, never flagged (same threshold as freshness.py);
  - tracked fields only — building: construction_value, address,
    job_category, work_type, building_type, units_added,
    occupancy_granted_date; development: status, description_of_development,
    address, permit_class, zoning;
  - present yesterday but gone today -> 'closed_expired';
  - new rows -> 'new'; anything else differing -> 'changed'.

Attribution: City of Edmonton — General Building Permits / Development
Permits (open data). Every permits API response carries this attribution.
"""
from __future__ import annotations

import csv
import json
import re
import sqlite3
import time
from datetime import date
from pathlib import Path

import requests

from db import DB_PATH, DAY_ZERO, get_conn, init_db

BUILDING_URL = "https://data.edmonton.ca/resource/24uj-dj8v.json"
DEVELOPMENT_URL = "https://data.edmonton.ca/resource/2ccn-pwtu.json"

# :@computed_region_* fields are NEVER fetched or compared.
COMPUTED_PREFIX = ":@computed_region_"

# Same jitter rule as the business feed (~100 m at Edmonton's latitude).
COORD_JITTER_THRESHOLD = 0.0009

FILES_DIR = Path(__file__).resolve().parent.parent
ATTRIBUTION = "City of Edmonton — General Building Permits / Development Permits (open data)"

# Fields that count as real changes, per dataset.
BUILDING_TRACKED = (
    "construction_value", "address", "job_category", "work_type",
    "building_type", "units_added", "occupancy_granted_date",
)
DEVELOPMENT_TRACKED = (
    "status", "description_of_development", "address", "permit_class",
    "zoning",
)


# ------------------------------------------------------------------ cleaning
def _norm(value) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).strip()).casefold()


def _num(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _date10(value) -> "str | None":
    if not value:
        return None
    return str(value)[:10]


def _float(v):
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def clean_building(r: dict) -> dict:
    """Clean one raw building-permit row into our normalized schema."""
    cv = _num(r.get("construction_value"))
    units = _num(r.get("units_added"))
    job_category = (r.get("job_category") or "").strip()
    job_description = (r.get("job_description") or "").strip()
    work_type = (r.get("work_type") or "").strip()
    building_type = (r.get("building_type") or "").strip()
    address = (r.get("address") or "").strip() or None
    if job_description:
        description = job_description
    elif work_type or building_type:
        description = " — ".join(p for p in (work_type, building_type) if p)
    else:
        description = job_category or "Building permit"
    return {
        "dataset": "building",
        "external_key": str(r.get("row_id") or "").strip(),
        "permit_number": (r.get("permit_number") or "").strip() or None,
        "event_date": _date10(r.get("issue_date") or r.get("permit_date")),
        "permit_type": job_category or None,
        "status": "Issued",
        "description": description,
        "address": address,
        "neighbourhood": (r.get("neighbourhood") or "").strip() or None,
        "zoning": (r.get("zoning") or "").strip() or None,
        "construction_value": cv,
        "units_added": units,
        "latitude": _float(r.get("latitude")),
        "longitude": _float(r.get("longitude")),
        "tracked": {
            "construction_value": cv,
            "address": address,
            "job_category": job_category,
            "work_type": work_type,
            "building_type": building_type,
            "units_added": units,
            "occupancy_granted_date": _date10(r.get("occupancy_granted_date")),
        },
    }


def clean_development(r: dict) -> dict:
    """Clean one raw development-permit row into our normalized schema."""
    permit_type = (r.get("permit_type") or "").strip()
    status = (r.get("status") or "").strip()
    description = (r.get("description_of_development") or "").strip()
    address = (r.get("address") or "").strip() or None
    permit_class = (r.get("permit_class") or "").strip()
    return {
        "dataset": "development",
        "external_key": str(r.get("city_file_number") or "").strip(),
        "permit_number": None,
        "event_date": _date10(r.get("permit_date")),
        "permit_type": permit_type or None,
        "status": status or None,
        "description": description or permit_type or "Development permit",
        "address": address,
        "neighbourhood": (r.get("neighbourhood") or "").strip() or None,
        "zoning": (r.get("zoning") or "").strip() or None,
        "construction_value": None,
        "units_added": None,
        "latitude": _float(r.get("latitude")),
        "longitude": _float(r.get("longitude")),
        "tracked": {
            "status": status,
            "description_of_development": description,
            "address": address,
            "permit_class": permit_class,
            "zoning": (r.get("zoning") or "").strip(),
        },
    }


def fetch_dataset(url: str, order_field: str, cleaner, page_size: int = 50000) -> list[dict]:
    """Page a Socrata resource gently; clean rows with the given cleaner."""
    records: list[dict] = []
    offset = 0
    while True:
        resp = requests.get(
            url,
            params={"$limit": page_size, "$offset": offset, "$order": order_field},
            timeout=60,
        )
        resp.raise_for_status()
        rows = resp.json()
        if not rows:
            break
        for r in rows:
            rec = cleaner(r)
            if not rec["external_key"]:
                continue
            records.append(rec)
        offset += page_size
        time.sleep(0.4)
    return records


# ------------------------------------------------------------ change summaries
def _fmt_money(value) -> str:
    try:
        return "$" + f"{float(value):,.0f}"
    except (TypeError, ValueError):
        return "an undisclosed value"


def _where(address, neighbourhood) -> str:
    loc = (address or "").strip()
    hood = (neighbourhood or "").strip()
    if loc and hood:
        return f"{loc} ({hood})"
    return loc or hood or "Edmonton"


def _short(text: str, limit: int = 120) -> str:
    text = (text or "").strip()
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def summarize_new(rec: dict) -> str:
    """Plain-English summary for a brand-new permit record."""
    if rec["dataset"] == "building":
        bits = [f"New building permit issued {rec['event_date'] or 'recently'}"]
        bits.append(f"{_fmt_money(rec['construction_value'])}")
        if rec["permit_type"]:
            bits.append(rec["permit_type"])
        bits.append("project")
        if rec["units_added"]:
            try:
                units = int(float(rec["units_added"]))
                bits.append(f"adding {units} units")
            except (TypeError, ValueError):
                pass
        bits.append(f"at {_where(rec['address'], rec['neighbourhood'])}.")
        return " ".join(bits)
    return (
        f"New development permit ({rec['permit_type'] or 'permit'}): "
        f"{_short(rec['description'])} at {_where(rec['address'], rec['neighbourhood'])}."
    )


def summarize_changed(dataset: str, diffs: list, old: dict, new: dict) -> str:
    """Plain-English summary for a changed permit record."""
    by_field = {d["field"]: d for d in diffs}
    loc = _where(new.get("address"), new.get("neighbourhood"))
    if dataset == "development" and "status" in by_field:
        return (
            f"Development permit status changed from "
            f"'{by_field['status']['old']}' to '{by_field['status']['new']}' — "
            f"{_short(new.get('description'))} at {loc}."
        )
    if dataset == "building" and "construction_value" in by_field:
        d = by_field["construction_value"]
        return (
            f"Building permit construction value changed from "
            f"{_fmt_money(d['old'])} to {_fmt_money(d['new'])} at {loc}."
        )
    parts = []
    for d in diffs[:3]:
        field = d["field"].replace("_", " ")
        parts.append(f"{field} changed from '{d['old']}' to '{d['new']}'")
    kind = "Building" if dataset == "building" else "Development"
    return f"{kind} permit change at {loc}: " + "; ".join(parts) + "."


def summarize_closed(rec: dict) -> str:
    kind = "Building" if rec["dataset"] == "building" else "Development"
    label = rec.get("permit_type") or "permit"
    return (
        f"{kind} permit expired (no longer in city feed): {label} at "
        f"{_where(rec['address'], rec['neighbourhood'])}."
    )


# ------------------------------------------------------------------- refresh
def _tracked_diff(dataset: str, old_tracked: dict, new_tracked: dict) -> list:
    """Return [{field, old, new}] for tracked fields that differ, normalized."""
    tracked = BUILDING_TRACKED if dataset == "building" else DEVELOPMENT_TRACKED
    diffs = []
    for field in tracked:
        old, new = old_tracked.get(field), new_tracked.get(field)
        if field in ("construction_value", "units_added"):
            # Numeric comparison; None vs value counts as a change.
            if _num(old) != _num(new):
                diffs.append({"field": field, "old": old, "new": new})
        elif _norm(old) != _norm(new):
            diffs.append({"field": field, "old": old, "new": new})
    return diffs


def _jitter_ok(old_lat, old_lon, new_lat, new_lon) -> bool:
    """True if the coordinate shift is within jitter (safe to update silently)."""
    try:
        return (
            abs(float(old_lat) - float(new_lat)) <= COORD_JITTER_THRESHOLD
            and abs(float(old_lon) - float(new_lon)) <= COORD_JITTER_THRESHOLD
        )
    except (TypeError, ValueError):
        return False


def _insert_permit(conn, rec: dict, today: str) -> int:
    cur = conn.execute(
        """INSERT INTO permits
           (dataset, external_key, permit_number, event_date, permit_type,
            status, description, address, neighbourhood, zoning,
            construction_value, units_added, latitude, longitude,
            first_seen, last_seen, record_status, raw_json, source)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, 'edmonton_open_data')""",
        (
            rec["dataset"], rec["external_key"], rec.get("permit_number"),
            rec.get("event_date"), rec.get("permit_type"), rec.get("status"),
            rec.get("description"), rec.get("address"), rec.get("neighbourhood"),
            rec.get("zoning"), rec.get("construction_value"), rec.get("units_added"),
            rec.get("latitude"), rec.get("longitude"),
            today, today, json.dumps(rec["tracked"]),
        ),
    )
    return cur.lastrowid


def _diff_dataset(conn, dataset: str, records: list, today: str) -> dict:
    """Diff one dataset's fetched records against the permits table."""
    # Day-zero honesty: first-ever load for a dataset becomes the baseline —
    # records are stored with first_seen=today, but NO 'new' change events
    # are fabricated for the pre-existing rows.
    baseline = (
        conn.execute(
            "SELECT COUNT(*) AS c FROM permits WHERE dataset = ?", (dataset,)
        ).fetchone()["c"]
        == 0
    )
    n_new = n_closed = n_changed = n_unchanged = 0
    db_rows = conn.execute(
        "SELECT * FROM permits WHERE dataset = ?", (dataset,)
    ).fetchall()
    db_by_key = {r["external_key"]: dict(r) for r in db_rows}
    seen: set = set()

    # ---- Write batching (scale pass): collect row operations during the
    # diff and flush with executemany. Diff SEMANTICS are untouched — same
    # SQL, same parameters; only the write pattern changed. New-permit
    # INSERTs stay per-row (we need cursor.lastrowid to link events); in
    # steady state these are rare.
    coord_touches: list = []   # (lat, lon, today, id) jitter-safe coord update
    plain_touches: list = []   # (today, id) last_seen touch
    full_updates: list = []    # full-row UPDATE for 'changed' rows
    expired_updates: list = []  # (today, id) for expired rows
    events: list = []          # permit_changes rows, in diff order

    def queue_event(permit_id, rec, change_type, diffs, summary):
        events.append(
            (
                permit_id, rec["dataset"], rec["external_key"], change_type,
                json.dumps(diffs) if diffs else None, today, summary,
                int(time.time()),
            )
        )

    for rec in records:
        key = rec["external_key"]
        seen.add(key)
        row = db_by_key.get(key)
        if row is None:
            permit_id = _insert_permit(conn, rec, today)
            if not baseline:
                queue_event(permit_id, rec, "new", None, summarize_new(rec))
                n_new += 1
            continue
        diffs = _tracked_diff(dataset, json.loads(row["raw_json"] or "{}"), rec["tracked"])
        # Silently update coordinates within jitter; keep fresh last_seen.
        new_lat, new_lon = rec.get("latitude"), rec.get("longitude")
        if _jitter_ok(row["latitude"], row["longitude"], new_lat, new_lon):
            coord_touches.append((new_lat, new_lon, today, row["id"]))
        else:
            plain_touches.append((today, row["id"]))
        if diffs:
            old_vals = {d["field"]: d["old"] for d in diffs}
            old = {**dict(row), **old_vals}
            full_updates.append(
                (
                    rec.get("permit_number"), rec.get("event_date"),
                    rec.get("permit_type"), rec.get("status"), rec.get("description"),
                    rec.get("address"), rec.get("neighbourhood"), rec.get("zoning"),
                    rec.get("construction_value"), rec.get("units_added"),
                    json.dumps(rec["tracked"]), today, row["id"],
                )
            )
            queue_event(
                row["id"], rec, "changed", diffs,
                summarize_changed(dataset, diffs, old, rec),
            )
            n_changed += 1
        else:
            n_unchanged += 1

    # Records in the DB but not in today's feed = expired.
    for key, row in db_by_key.items():
        if key in seen or row["record_status"] != "active":
            continue
        expired_updates.append((today, row["id"]))
        rec = {
            "dataset": dataset, "external_key": key,
            "permit_type": row["permit_type"], "address": row["address"],
            "neighbourhood": row["neighbourhood"],
        }
        queue_event(row["id"], rec, "closed_expired", None, summarize_closed(rec))
        n_closed += 1

    # ---- Flush all queued writes in a few batched statements -------------
    if coord_touches:
        conn.executemany(
            "UPDATE permits SET latitude = ?, longitude = ?, last_seen = ?,"
            " record_status = 'active' WHERE id = ?",
            coord_touches,
        )
    if plain_touches:
        conn.executemany(
            "UPDATE permits SET last_seen = ?, record_status = 'active' WHERE id = ?",
            plain_touches,
        )
    if full_updates:
        conn.executemany(
            """UPDATE permits SET permit_number = ?, event_date = ?,
               permit_type = ?, status = ?, description = ?, address = ?,
               neighbourhood = ?, zoning = ?, construction_value = ?,
               units_added = ?, raw_json = ?, last_seen = ?,
               record_status = 'active' WHERE id = ?""",
            full_updates,
        )
    if expired_updates:
        conn.executemany(
            "UPDATE permits SET record_status = 'expired', last_seen = ? WHERE id = ?",
            expired_updates,
        )
    if events:
        conn.executemany(
            """INSERT INTO permit_changes
               (permit_id, dataset, external_key, change_type, changed_fields,
                detected_on, summary, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            events,
        )

    conn.execute(
        """INSERT INTO permit_snapshot_runs
           (run_date, dataset, rows_fetched, new, closed_expired, changed,
            unchanged, notes)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (today, dataset, len(records), n_new, n_closed, n_changed,
         n_unchanged,
         "day-zero baseline — no change events fabricated" if baseline
         else "daily snapshot-diff"),
    )
    return {
        "dataset": dataset, "rows_fetched": len(records),
        "new": n_new, "closed_expired": n_closed,
        "changed": n_changed, "unchanged": n_unchanged,
    }


def write_daily_outputs(conn, today: str, results: list) -> tuple:
    """Write the day's permit changes CSV + digest markdown under files/.

    On the baseline day (no change events exist yet), the files honestly
    say the baseline was established instead of showing empty change lists.
    """
    rows = conn.execute(
        """SELECT pc.detected_on, pc.dataset, pc.change_type, pc.external_key,
                  p.address, p.neighbourhood, p.construction_value, pc.summary
           FROM permit_changes pc
           LEFT JOIN permits p ON p.id = pc.permit_id
           WHERE pc.detected_on = ?
           ORDER BY pc.dataset, pc.id""",
        (today,),
    ).fetchall()
    csv_path = FILES_DIR / f"permits_changes_{today}.csv"
    with open(csv_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["detected_on", "dataset", "change_type", "external_key",
                    "address", "neighbourhood", "construction_value", "summary"])
        for r in rows:
            w.writerow([r["detected_on"], r["dataset"], r["change_type"],
                        r["external_key"], r["address"], r["neighbourhood"],
                        r["construction_value"], r["summary"]])

    total_new = sum(r["new"] for r in results)
    total_closed = sum(r["closed_expired"] for r in results)
    total_changed = sum(r["changed"] for r in results)
    lines = [f"# Nodepipe permits digest — {today}", ""]
    if not rows:
        lines.append("Baseline established today: the day-zero snapshot for the")
        lines.append("permits feed. No change events yet — diffs begin with")
        lines.append("tomorrow's refresh. Nothing is backfilled or invented.")
        lines.append("")
    else:
        lines.append(f"**{total_new} new · {total_closed} expired · {total_changed} changed.**")
        lines.append("")
    for r in results:
        label = "Building" if r["dataset"] == "building" else "Development"
        lines.append(
            f"- {label} permits: fetched {r['rows_fetched']:,}, "
            f"new {r['new']}, expired {r['closed_expired']}, "
            f"changed {r['changed']}, unchanged {r['unchanged']}."
        )
    lines.append("")
    if rows:
        # Top 10 new building permits by construction value.
        top = conn.execute(
            """SELECT p.address, p.neighbourhood, p.construction_value, pc.summary
               FROM permit_changes pc JOIN permits p ON p.id = pc.permit_id
               WHERE pc.detected_on = ? AND pc.dataset = 'building'
                 AND pc.change_type = 'new'
               ORDER BY p.construction_value DESC LIMIT 10""",
            (today,),
        ).fetchall()
        if top:
            lines.append("## Top new building permits by construction value")
            lines.append("")
            for t in top:
                lines.append(f"- {t['summary']}")
            lines.append("")
        flips = conn.execute(
            """SELECT pc.summary FROM permit_changes pc
               WHERE pc.detected_on = ? AND pc.dataset = 'development'
                 AND pc.change_type = 'changed'
                 AND pc.changed_fields LIKE '%\"status\"%' LIMIT 20""",
            (today,),
        ).fetchall()
        if flips:
            lines.append("## Development-permit status flips")
            lines.append("")
            for fl in flips:
                lines.append(f"- {fl['summary']}")
            lines.append("")
    lines.append(f"Attribution: {ATTRIBUTION}.")
    digest_path = FILES_DIR / f"permits_digest_{today}.md"
    digest_path.write_text("\n".join(lines))
    return str(csv_path), str(digest_path)


def refresh_permits(db_path=None, source_data=None, today=None, write_files=True) -> dict:
    """Run one snapshot-diff cycle for both permit datasets.

    source_data (optional): {'building': [...], 'development': [...]} cleaned
    records, used by tests to inject synthetic source data. Defaults to live
    Socrata fetches. Idempotent: re-running against unchanged source data
    records zero changes and just refreshes last_seen.
    """
    today = today or date.today().isoformat()
    target = db_path or DB_PATH
    init_db(target)
    conn = sqlite3.connect(target)
    conn.row_factory = sqlite3.Row

    results = []
    configs = (
        ("building", BUILDING_URL, "row_id", clean_building),
        ("development", DEVELOPMENT_URL, "city_file_number", clean_development),
    )
    for dataset, url, order_field, cleaner in configs:
        if source_data is not None and dataset in source_data:
            records = source_data[dataset]
        else:
            records = fetch_dataset(url, order_field, cleaner)
        results.append(_diff_dataset(conn, dataset, records, today))

    files = ("", "")
    if write_files:
        files = write_daily_outputs(conn, today, results)
    conn.commit()
    conn.close()
    return {
        "run_date": today,
        "results": results,
        "csv": files[0],
        "digest": files[1],
    }
