"""Freshness engine CLI: snapshot-diff the Edmonton Open Data dataset.

Runs one refresh cycle (see freshness.refresh) and prints a summary.
Idempotent: re-running against unchanged source data records zero changes
and just refreshes last_seen. Suitable for a daily schedule.

Usage:
    .venv/bin/python refresh.py [db_path]
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from db import DAY_ZERO, get_conn
from freshness import refresh
from permits import refresh_permits


def main() -> None:
    db_path = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    print(f"Freshness baseline date (day zero): {DAY_ZERO}")
    print("Fetching current source data...")
    summary = refresh(db_path=db_path)
    print(
        "Snapshot run {run_date} [{source}]: fetched={rows_fetched} "
        "new={records_new} closed={records_closed} "
        "changed={records_changed} unchanged={records_unchanged}".format(**summary)
    )
    target = db_path or Path(__file__).parent / "data.db"
    with get_conn(target) as conn:
        active = conn.execute(
            "SELECT COUNT(*) AS c FROM businesses WHERE status = 'active'"
        ).fetchone()["c"]
        closed = conn.execute(
            "SELECT COUNT(*) AS c FROM businesses WHERE status = 'closed'"
        ).fetchone()["c"]
    print(f"Active records: {active}; closed records kept: {closed}")

    # ---- Permits feed (same daily cycle, no new infrastructure) ----------
    print("Fetching permit source data...")
    permits_summary = refresh_permits(db_path=db_path)
    for r in permits_summary["results"]:
        print(
            "Permits snapshot run {run_date} [{dataset}]: fetched={rows_fetched} "
            "new={new} closed_expired={closed_expired} "
            "changed={changed} unchanged={unchanged}".format(
                run_date=permits_summary["run_date"], **r
            )
        )
    print(f"Permits daily outputs: {permits_summary['csv']}, {permits_summary['digest']}")

    with get_conn(target) as conn:
        permits_active = conn.execute(
            "SELECT COUNT(*) AS c FROM permits WHERE record_status = 'active'"
        ).fetchone()["c"]
    print(f"Active permit records: {permits_active}")


if __name__ == "__main__":
    main()
