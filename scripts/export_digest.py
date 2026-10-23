"""Export the day's change digest as markdown for the New in Edmonton post.

Reads the refreshed data.db (businesses + permits change feeds) and writes
digest_YYYY-MM-DD.md next to it. Run after refresh.py in CI; the file is
uploaded as a workflow artifact for the daily social post.
"""
from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

DB = Path(__file__).resolve().parent.parent / "data.db"
TODAY = date.today().isoformat()


def _rows(conn, sql, params=()):
    return conn.execute(sql, params).fetchall()


def main() -> None:
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    lines = [f"# Nodepipe daily digest - {TODAY}", ""]

    # ---- businesses -------------------------------------------------------
    run = _rows(
        conn,
        "SELECT rows_fetched, records_new, records_closed, records_changed,"
        " records_unchanged FROM snapshot_runs WHERE run_date = ?"
        " ORDER BY id DESC LIMIT 1",
        (TODAY,),
    )
    if run:
        r = run[0]
        lines += [
            "## Businesses",
            f"- Fetched: {r['rows_fetched']:,}",
            f"- New: {r['records_new']}, closed: {r['records_closed']},"
            f" changed: {r['records_changed']}, unchanged: {r['records_unchanged']}",
            "",
        ]
    else:
        lines += ["## Businesses", "- No snapshot run recorded for today.", ""]

    def notable(change_type: str, limit: int = 12):
        return _rows(
            conn,
            """SELECT b.name, b.address, b.category
               FROM business_changes c
               LEFT JOIN businesses b ON b.id = c.business_id
               WHERE c.detected_on = ? AND c.change_type = ?
               ORDER BY c.id DESC LIMIT ?""",
            (TODAY, change_type, limit),
        )

    new = notable("new")
    lines.append(f"### Notable openings ({len(new)} shown)")
    for n in new:
        lines.append(f"- {n['name']} - {n['address'] or 'address n/a'}"
                     + (f" ({n['category']})" if n['category'] else ""))
    lines.append("")

    closed = notable("closed")
    lines.append(f"### Notable closures ({len(closed)} shown)")
    for n in closed:
        lines.append(f"- {n['name']} - {n['address'] or 'address n/a'}"
                     + (f" ({n['category']})" if n['category'] else ""))
    lines.append("")

    # ---- permits ----------------------------------------------------------
    pruns = _rows(
        conn,
        "SELECT dataset, rows_fetched, new, closed_expired, changed"
        " FROM permit_snapshot_runs WHERE run_date = ?",
        (TODAY,),
    )
    lines.append("## Permits")
    if pruns:
        for p in pruns:
            lines.append(
                f"- {p['dataset']}: fetched {p['rows_fetched']:,},"
                f" new {p['new']}, closed/expired {p['closed_expired']},"
                f" changed {p['changed']}"
            )
    else:
        lines.append("- No permit snapshot run recorded for today.")
    lines.append("")

    pnew = _rows(
        conn,
        "SELECT dataset, summary FROM permit_changes"
        " WHERE detected_on = ? AND change_type = 'new'"
        " ORDER BY id DESC LIMIT 8",
        (TODAY,),
    )
    lines.append(f"### Notable new permits ({len(pnew)} shown)")
    for p in pnew:
        lines.append(f"- [{p['dataset']}] {p['summary']}")
    lines.append("")

    out = DB.parent / f"digest_{TODAY}.md"
    out.write_text("\n".join(lines))
    conn.close()
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
