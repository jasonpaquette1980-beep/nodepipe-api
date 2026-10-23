"""Nodepipe — local business data, delivered by API.

Data/API product MVP: clean Edmonton local-business data, sold via API.

MVP dataset: Edmonton local businesses (OpenStreetMap, fallback to labelled seed).
Auth: X-API-Key header. Every authenticated call is metered to the usage table.
"""
from __future__ import annotations

import html

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse

from db import (
    ensure_demo_key,
    get_conn,
    init_db,
    key_is_valid,
    log_usage,
    monthly_usage,
)
from permits import ATTRIBUTION as PERMITS_ATTRIBUTION
from ratelimit import (
    RATE_LIMIT_PER_MINUTE,
    QuotaExceeded,
    RateLimited,
    check_monthly_quota,
    check_rate_limit,
    month_reset_iso,
)

PRODUCT_NAME = "Nodepipe"

app = FastAPI(title=PRODUCT_NAME)


# ------------------------------------------------------------------ startup
@app.on_event("startup")
def startup() -> None:
    init_db()
    demo_key = ensure_demo_key()
    print(f"[startup] Demo API key: {demo_key}")


# ------------------------------------------------------------------ auth
def require_key(
    request: Request, x_api_key: str | None = Header(default=None)
) -> str:
    # Abuse protection runs BEFORE auth: hammering with bad keys is still
    # rate limited (subject falls back to client IP when no key is given).
    subject = x_api_key or (request.client.host if request.client else "unknown")
    try:
        check_rate_limit(subject)
    except RateLimited as e:
        raise HTTPException(
            status_code=429,
            detail={
                "error": "rate_limit_exceeded",
                "limit": RATE_LIMIT_PER_MINUTE,
                "window": "1 minute",
                "retry_after_seconds": e.retry_after_seconds,
            },
        )
    if not x_api_key or not key_is_valid(x_api_key):
        raise HTTPException(
            status_code=401,
            detail="Missing or invalid X-API-Key header.",
        )
    return x_api_key


def enforce_quota(api_key: str = Depends(require_key)) -> str:
    """Monthly plan-quota gate. Rejected calls are NOT metered (429, no usage
    row). /v1/usage deliberately skips this so a capped customer can always
    see their plan/used/limit/remaining."""
    try:
        check_monthly_quota(api_key)
    except QuotaExceeded as e:
        raise HTTPException(
            status_code=429,
            detail={
                "error": "monthly_quota_exceeded",
                "plan": e.plan,
                "limit": e.limit,
                "used": e.used,
                "resets": month_reset_iso(),
            },
        )
    return api_key


def business_to_dict(row) -> dict:
    d = {
        "id": row["id"],
        "name": row["name"],
        "category": row["category"],
        "address": row["address"],
        "latitude": row["latitude"],
        "longitude": row["longitude"],
        "phone": row["phone"],
        "website": row["website"],
        "opening_hours": row["opening_hours"],
        "source": row["source"],
    }
    # Freshness tracking fields (present once the engine's migration has run).
    if "status" in row.keys():
        d["status"] = row["status"]
        d["first_seen"] = row["first_seen"]
        d["last_seen"] = row["last_seen"]
    return d


def parse_since(since: str | None, default_days_back: int = 7) -> str:
    """Validate YYYY-MM-DD; raise 400 on garbage. Default = N days ago."""
    from datetime import date, timedelta

    if since is None:
        return (date.today() - timedelta(days=default_days_back)).isoformat()
    try:
        parts = since.split("-")
        if len(parts) != 3:
            raise ValueError
        year, month, day = (int(p) for p in parts)
        return date(year, month, day).isoformat()
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="since must be a date in YYYY-MM-DD format.",
        )


# ------------------------------------------------------------------ public
@app.get("/health")
def health() -> dict:
    return {"status": "ok", "product": PRODUCT_NAME}


@app.get("/", response_class=HTMLResponse)
def landing() -> str:
    with get_conn() as conn:
        count = conn.execute(
            "SELECT COUNT(*) AS c FROM businesses WHERE status = 'active'"
        ).fetchone()["c"]
    demo = html.escape("demo_edmonton_2026")
    return f"""<!doctype html>
<html><head><title>{PRODUCT_NAME}</title>
<style>body{{font-family:system-ui,sans-serif;max-width:720px;margin:40px auto;
padding:0 16px;line-height:1.6}} code{{background:#f0f0f0;padding:2px 6px;
border-radius:4px}} pre{{background:#f6f6f6;padding:12px;border-radius:6px;
overflow-x:auto}}</style></head>
<body>
<h1>{PRODUCT_NAME}</h1>
<p><strong>Local business data, delivered by API.</strong></p>
<p>Clean, developer-ready <strong>Edmonton local-business data</strong>
(<code>{count}</code> records and counting) over a simple REST API.</p>
<p>One key, usage-based billing, no scraping headaches.</p>
<h2>Endpoints</h2>
<ul>
<li><code>GET /health</code> — service status (no key needed)</li>
<li><code>GET /v1/businesses</code> — list/search businesses
(<code>?category=cafe&amp;search=transcend&amp;limit=10&amp;offset=0</code>)</li>
<li><code>GET /v1/businesses/{{id}}</code> — one business</li>
<li><code>GET /v1/categories</code> — distinct categories</li>
<li><code>GET /v1/usage</code> — your calls this month (metering)</li>
<li><code>GET /v1/changes</code> — what changed in the dataset
(<code>?since=2026-10-18&amp;type=new</code>)</li>
<li><code>GET /v1/businesses/new</code> — businesses first seen since a date
(the "just opened" feed)</li>
<li><code>GET /v1/freshness</code> — last snapshot, 7/30-day change counts</li>
<li><code>GET /permits/new</code> — newly issued permits
(<code>?dataset=building&amp;since=2026-10-18</code>)</li>
<li><code>GET /permits/changes</code> — permit change events
(<code>?change_type=changed&amp;dataset=development</code>)</li>
<li><code>GET /permits/search</code> — search permits by address, description,
neighbourhood, or permit number
(<code>?q=Blatchford&amp;min_value=1000000</code>)</li>
</ul>
<p>Freshness: the dataset is snapshotted daily against Edmonton's official
business-licence data. <code>/v1/businesses</code> lists active businesses;
closed ones are kept but excluded unless
<code>?include_closed=1</code> is passed.</p>
<h2>Quick start</h2>
<pre>curl -H "X-API-Key: {demo}" \\
  "http://127.0.0.1:8000/v1/businesses?category=cafe&limit=3"</pre>
<p>Demo key: <code>{demo}</code> &mdash; send it as the
<code>X-API-Key</code> header. Full docs in the README.</p>
<footer><p><small>Data attribution: City of Edmonton &mdash; Business Licences,
General Building Permits, and Development Permits (open data).</small></p></footer>
</body></html>"""


# ------------------------------------------------------------ authenticated
@app.get("/v1/businesses")
def list_businesses(
    category: str | None = Query(default=None),
    search: str | None = Query(default=None),
    include_closed: int = Query(default=0, ge=0, le=1),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    api_key: str = Depends(enforce_quota),
) -> dict:
    log_usage(api_key, "GET /v1/businesses")
    clauses, params = [], []
    if not include_closed:
        clauses.append("status = 'active'")
    if category:
        clauses.append("LOWER(category) = LOWER(?)")
        params.append(category)
    if search:
        clauses.append("LOWER(name) LIKE '%' || LOWER(?) || '%'")
        params.append(search)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with get_conn() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) AS c FROM businesses {where}", params
        ).fetchone()["c"]
        rows = conn.execute(
            f"SELECT * FROM businesses {where} ORDER BY name LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()
    return {
        "data": [business_to_dict(r) for r in rows],
        "meta": {"total": total, "limit": limit, "offset": offset},
    }


@app.get("/v1/businesses/new")
def list_new_businesses(
    since: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    api_key: str = Depends(enforce_quota),
) -> dict:
    """Premium feed: businesses first seen on/after `since` (default: 7 days)."""
    since = parse_since(since)
    log_usage(api_key, "GET /v1/businesses/new")
    with get_conn() as conn:
        total = conn.execute(
            "SELECT COUNT(*) AS c FROM businesses "
            "WHERE status = 'active' AND first_seen >= ?",
            (since,),
        ).fetchone()["c"]
        rows = conn.execute(
            "SELECT * FROM businesses "
            "WHERE status = 'active' AND first_seen >= ? "
            "ORDER BY first_seen DESC, id DESC LIMIT ? OFFSET ?",
            (since, limit, offset),
        ).fetchall()
    return {
        "data": [business_to_dict(r) for r in rows],
        "meta": {"total": total, "since": since, "limit": limit, "offset": offset},
    }


@app.get("/v1/businesses/{business_id}")
def get_business(business_id: int, api_key: str = Depends(enforce_quota)) -> dict:
    log_usage(api_key, f"GET /v1/businesses/{business_id}")
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM businesses WHERE id = ?", (business_id,)
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Business not found.")
    return {"data": business_to_dict(row)}


@app.get("/v1/changes")
def list_changes(
    since: str | None = Query(default=None),
    type: str | None = Query(default=None, pattern="^(new|closed|changed)$"),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    api_key: str = Depends(enforce_quota),
) -> dict:
    """Change events from the freshness engine, newest first."""
    since = parse_since(since, default_days_back=30)
    log_usage(api_key, "GET /v1/changes")
    clauses, params = ["c.detected_on >= ?"], [since]
    if type:
        clauses.append("c.change_type = ?")
        params.append(type)
    with get_conn() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) AS c FROM business_changes c "
            f"WHERE {' AND '.join(clauses)}",
            params,
        ).fetchone()["c"]
        rows = conn.execute(
            f"""SELECT c.id, c.change_type, c.detected_on, c.changed_fields,
                       c.business_id, b.name, b.category, b.address,
                       b.latitude, b.longitude, b.status, b.source
                FROM business_changes c
                LEFT JOIN businesses b ON b.id = c.business_id
                WHERE {' AND '.join(clauses)}
                ORDER BY c.detected_on DESC, c.id DESC
                LIMIT ? OFFSET ?""",
            params + [limit, offset],
        ).fetchall()
    import json as _json

    data = []
    for r in rows:
        data.append(
            {
                "id": r["id"],
                "change_type": r["change_type"],
                "detected_on": r["detected_on"],
                "changed_fields": (
                    _json.loads(r["changed_fields"]) if r["changed_fields"] else None
                ),
                "business": {
                    "id": r["business_id"],
                    "name": r["name"],
                    "category": r["category"],
                    "address": r["address"],
                    "latitude": r["latitude"],
                    "longitude": r["longitude"],
                    "status": r["status"],
                    "source": r["source"],
                },
            }
        )
    return {
        "data": data,
        "meta": {"total": total, "since": since, "limit": limit, "offset": offset},
    }


@app.get("/v1/freshness")
def get_freshness(api_key: str = Depends(enforce_quota)) -> dict:
    """Freshness report: last snapshot, active counts, 7/30-day change counts."""
    log_usage(api_key, "GET /v1/freshness")
    with get_conn() as conn:
        last_run = conn.execute(
            "SELECT * FROM snapshot_runs ORDER BY run_date DESC, id DESC LIMIT 1"
        ).fetchone()
        active = conn.execute(
            "SELECT COUNT(*) AS c FROM businesses WHERE status = 'active'"
        ).fetchone()["c"]
        closed = conn.execute(
            "SELECT COUNT(*) AS c FROM businesses WHERE status = 'closed'"
        ).fetchone()["c"]
        sources = conn.execute(
            "SELECT source, COUNT(*) AS count FROM businesses "
            "WHERE status = 'active' GROUP BY source"
        ).fetchall()

        def counts(since_days: int) -> dict:
            row = conn.execute(
                """SELECT
                       SUM(CASE WHEN change_type = 'new' THEN 1 ELSE 0 END) AS n_new,
                       SUM(CASE WHEN change_type = 'closed' THEN 1 ELSE 0 END) AS n_closed,
                       SUM(CASE WHEN change_type = 'changed' THEN 1 ELSE 0 END) AS n_changed
                   FROM business_changes
                   WHERE detected_on >= date('now', ?)""",
                (f"-{since_days} days",),
            ).fetchone()
            return {
                "new": row["n_new"] or 0,
                "closed": row["n_closed"] or 0,
                "changed": row["n_changed"] or 0,
            }

    return {
        "baseline_date": "2026-10-18",
        "last_snapshot": (
            {
                "run_date": last_run["run_date"],
                "source": last_run["source"],
                "rows_fetched": last_run["rows_fetched"],
                "new": last_run["records_new"],
                "closed": last_run["records_closed"],
                "changed": last_run["records_changed"],
                "unchanged": last_run["records_unchanged"],
            }
            if last_run
            else None
        ),
        "active_records": active,
        "closed_records_kept": closed,
        "changes_last_7_days": counts(7),
        "changes_last_30_days": counts(30),
        "sources": [
            {
                "source": s["source"],
                "active_records": s["count"],
                "attribution": (
                    "City of Edmonton — Business Licences (open data, updated daily)"
                    if s["source"] == "edmonton_open_data"
                    else None
                ),
            }
            for s in sources
        ],
    }


@app.get("/v1/categories")
def list_categories(api_key: str = Depends(enforce_quota)) -> dict:
    log_usage(api_key, "GET /v1/categories")
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT category, COUNT(*) AS count FROM businesses "
            "WHERE status = 'active' "
            "GROUP BY category ORDER BY count DESC"
        ).fetchall()
    return {
        "data": [{"category": r["category"], "count": r["count"]} for r in rows]
    }


@app.get("/v1/usage")
def get_usage(api_key: str = Depends(require_key)) -> dict:
    log_usage(api_key, "GET /v1/usage")
    from billing import estimate_bill  # local import to keep startup light
    bill = estimate_bill(api_key)
    return {"api_key_suffix": api_key[-6:], "usage": bill}


# ------------------------------------------------------------ permits feed
def permit_to_dict(row) -> dict:
    return {
        "id": row["id"],
        "dataset": row["dataset"],
        "external_key": row["external_key"],
        "permit_number": row["permit_number"],
        "event_date": row["event_date"],
        "permit_type": row["permit_type"],
        "status": row["status"],
        "description": row["description"],
        "address": row["address"],
        "neighbourhood": row["neighbourhood"],
        "zoning": row["zoning"],
        "construction_value": row["construction_value"],
        "units_added": row["units_added"],
        "latitude": row["latitude"],
        "longitude": row["longitude"],
        "first_seen": row["first_seen"],
        "last_seen": row["last_seen"],
        "record_status": row["record_status"],
        "source": row["source"],
    }


def permit_change_to_dict(row) -> dict:
    import json as _json

    return {
        "id": row["id"],
        "change_type": row["change_type"],
        "dataset": row["dataset"],
        "detected_on": row["detected_on"],
        "summary": row["summary"],
        "timestamp": row["created_at"],
        "changed_fields": (
            _json.loads(row["changed_fields"]) if row["changed_fields"] else None
        ),
        "permit": {
            "id": row["permit_id"],
            "external_key": row["external_key"],
            "permit_number": row["permit_number"],
            "permit_type": row["permit_type"],
            "status": row["status"],
            "description": row["description"],
            "address": row["address"],
            "neighbourhood": row["neighbourhood"],
            "construction_value": row["construction_value"],
            "record_status": row["record_status"],
        },
    }


@app.get("/permits/new")
def permits_new(
    dataset: str | None = Query(default=None, pattern="^(building|development)$"),
    since: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    api_key: str = Depends(enforce_quota),
) -> dict:
    """Premium feed: permits first issued/seen since a date (default: 7 days)."""
    since = parse_since(since)
    log_usage(api_key, "GET /permits/new")
    clauses, params = ["c.change_type = 'new'", "c.detected_on >= ?"], [since]
    if dataset:
        clauses.append("c.dataset = ?")
        params.append(dataset)
    with get_conn() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) AS c FROM permit_changes c "
            f"WHERE {' AND '.join(clauses)}",
            params,
        ).fetchone()["c"]
        rows = conn.execute(
            f"""SELECT c.*, p.permit_number, p.permit_type, p.status,
                       p.description, p.address, p.neighbourhood,
                       p.construction_value, p.record_status
                FROM permit_changes c
                LEFT JOIN permits p ON p.id = c.permit_id
                WHERE {' AND '.join(clauses)}
                ORDER BY c.detected_on DESC, c.id DESC
                LIMIT ? OFFSET ?""",
            params + [limit, offset],
        ).fetchall()
    return {
        "data": [permit_change_to_dict(r) for r in rows],
        "meta": {
            "total": total,
            "since": since,
            "dataset": dataset,
            "limit": limit,
            "offset": offset,
            "attribution": PERMITS_ATTRIBUTION,
        },
    }


@app.get("/permits/changes")
def permits_changes(
    change_type: str | None = Query(
        default=None, pattern="^(new|closed_expired|changed)$"
    ),
    dataset: str | None = Query(default=None, pattern="^(building|development)$"),
    since: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    api_key: str = Depends(enforce_quota),
) -> dict:
    """Permit change events (new / closed_expired / changed), newest first."""
    since = parse_since(since, default_days_back=30)
    log_usage(api_key, "GET /permits/changes")
    clauses, params = ["c.detected_on >= ?"], [since]
    if change_type:
        clauses.append("c.change_type = ?")
        params.append(change_type)
    if dataset:
        clauses.append("c.dataset = ?")
        params.append(dataset)
    with get_conn() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) AS c FROM permit_changes c "
            f"WHERE {' AND '.join(clauses)}",
            params,
        ).fetchone()["c"]
        rows = conn.execute(
            f"""SELECT c.*, p.permit_number, p.permit_type, p.status,
                       p.description, p.address, p.neighbourhood,
                       p.construction_value, p.record_status
                FROM permit_changes c
                LEFT JOIN permits p ON p.id = c.permit_id
                WHERE {' AND '.join(clauses)}
                ORDER BY c.detected_on DESC, c.id DESC
                LIMIT ? OFFSET ?""",
            params + [limit, offset],
        ).fetchall()
    return {
        "data": [permit_change_to_dict(r) for r in rows],
        "meta": {
            "total": total,
            "since": since,
            "change_type": change_type,
            "dataset": dataset,
            "limit": limit,
            "offset": offset,
            "attribution": PERMITS_ATTRIBUTION,
        },
    }


@app.get("/permits/search")
def permits_search(
    q: str | None = Query(default=None),
    dataset: str | None = Query(default=None, pattern="^(building|development)$"),
    neighbourhood: str | None = Query(default=None),
    min_value: float | None = Query(default=None, ge=0),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    api_key: str = Depends(enforce_quota),
) -> dict:
    """Search permits by address, description, neighbourhood, or permit number."""
    log_usage(api_key, "GET /permits/search")
    clauses, params = ["record_status = 'active'"], []
    if dataset:
        clauses.append("dataset = ?")
        params.append(dataset)
    if neighbourhood:
        clauses.append("LOWER(neighbourhood) LIKE '%' || LOWER(?) || '%'")
        params.append(neighbourhood)
    if min_value is not None:
        clauses.append("construction_value >= ?")
        params.append(min_value)
    if q:
        clauses.append(
            "(LOWER(address) LIKE '%' || LOWER(?) || '%' "
            "OR LOWER(description) LIKE '%' || LOWER(?) || '%' "
            "OR LOWER(neighbourhood) LIKE '%' || LOWER(?) || '%' "
            "OR LOWER(COALESCE(permit_number, '')) LIKE '%' || LOWER(?) || '%' "
            "OR LOWER(external_key) LIKE '%' || LOWER(?) || '%')"
        )
        params.extend([q, q, q, q, q])
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with get_conn() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) AS c FROM permits {where}", params
        ).fetchone()["c"]
        rows = conn.execute(
            f"""SELECT * FROM permits {where}
                ORDER BY COALESCE(construction_value, 0) DESC, event_date DESC
                LIMIT ? OFFSET ?""",
            params + [limit, offset],
        ).fetchall()
    return {
        "data": [permit_to_dict(r) for r in rows],
        "meta": {
            "total": total,
            "q": q,
            "dataset": dataset,
            "neighbourhood": neighbourhood,
            "min_value": min_value,
            "limit": limit,
            "offset": offset,
            "attribution": PERMITS_ATTRIBUTION,
        },
    }
