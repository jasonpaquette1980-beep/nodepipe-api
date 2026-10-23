"""Ingest Edmonton local-business data for the data/API product MVP.

Live sources (in priority order):
  1. City of Edmonton Open Data — Business Licences (Socrata API, official
     city data: every currently-licensed business in Edmonton).
     Dataset: https://data.edmonton.ca/resource/qhi4-bdpu.json
     Attribution: City of Edmonton.
  2. OpenStreetMap via the Overpass API (public data) — chunked, small queries.

If every live source fails or returns too little, falls back to a clearly
labelled realistic seed dataset so the API still works (source='seed').

Fields the Edmonton Open Data source provides: business name, licence
category, address (or "<Home Based Business>"), neighbourhood, ward, and
latitude/longitude for most storefront records. It does NOT provide phone,
website, or opening hours — those are stored as NULL, never invented.
"""
from __future__ import annotations

import sqlite3
import sys
import time
from datetime import date

import requests

from db import DB_PATH, business_key, get_conn, init_db

# --------------------------------------------------------------------------
# Source 1: City of Edmonton Open Data — Business Licences (Socrata)
# --------------------------------------------------------------------------
EDMONTON_OPEN_DATA_URL = "https://data.edmonton.ca/resource/qhi4-bdpu.json"
# Dataset as of ingest day: "A list of businesses with a valid licence to
# operate in Edmonton." Updated by the city daily.
SOURCE_NAME = "edmonton_open_data"


def fetch_edmonton_open_data(page_size: int = 5000) -> list[dict]:
    """Page gently through the Socrata API; clean rows into our schema.

    Category: the primary (first) licence category is kept verbatim, e.g.
    "Restaurant or Food Service" — businesses can hold several licences
    (semicolon-separated); we keep the first so categories stay searchable.
    """
    records: list[dict] = []
    seen: set[tuple[str, str]] = set()
    offset = 0
    while True:
        params = {
            "$limit": page_size,
            "$offset": offset,
            "$order": "externalid",  # stable paging order
        }
        resp = requests.get(EDMONTON_OPEN_DATA_URL, params=params, timeout=60)
        resp.raise_for_status()
        rows = resp.json()
        if not rows:
            break
        for r in rows:
            name = (r.get("business_name") or "").strip().strip('"').strip()
            cat_raw = (r.get("business_licence_category") or "").strip()
            if not name or not cat_raw:
                continue
            category = cat_raw.split(";")[0].strip()
            address = (r.get("business_address") or "").strip() or None
            dedupe_key = (name.lower(), (address or "").lower())
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            lat = r.get("latitude")
            lon = r.get("longitude")
            records.append(
                {
                    "name": name,
                    "category": category,
                    "address": address,
                    "latitude": float(lat) if lat is not None else None,
                    "longitude": float(lon) if lon is not None else None,
                    "phone": None,  # not in the source dataset
                    "website": None,  # not in the source dataset
                    "opening_hours": None,  # not in the source dataset
                    "source": SOURCE_NAME,
                }
            )
        offset += page_size
        time.sleep(0.4)  # be a polite API citizen
    return records


# --------------------------------------------------------------------------
# Source 2: OpenStreetMap via Overpass (chunked, small queries)
# --------------------------------------------------------------------------
# Edmonton bounding box: south, west, north, east
EDMONTON_BBOX = (53.40, -113.70, 53.67, -113.28)

OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.nchc.org.tw/api/interpreter",
]

# Small, single-category queries time out far less than the old combined one.
OVERPASS_CHUNKED_QUERIES = [
    'nwr["amenity"="restaurant"]["name"]({s},{w},{n},{e});',
    'nwr["amenity"="cafe"]["name"]({s},{w},{n},{e});',
    'nwr["amenity"="bar"]["name"]({s},{w},{n},{e});',
    'nwr["amenity"="fast_food"]["name"]({s},{w},{n},{e});',
    'nwr["shop"]["name"]({s},{w},{n},{e});',
    'nwr["amenity"="pharmacy"]["name"]({s},{w},{n},{e});',
]


def _overpass_chunk(query_body: str, url: str) -> list[dict]:
    s, w, n, e = EDMONTON_BBOX
    query = "[out:json][timeout:45];({body});out center 300;".format(
        body=query_body.format(s=s, w=w, n=n, e=e)
    )
    resp = requests.post(url, data={"data": query}, timeout=75)
    resp.raise_for_status()
    return clean_osm(resp.json().get("elements", []))


def fetch_osm() -> list[dict]:
    """Fetch business-like nodes/ways from Overpass with small chunked queries."""
    records: list[dict] = []
    seen: set[tuple[str, float, float]] = set()
    for body in OVERPASS_CHUNKED_QUERIES:
        got_chunk = False
        for url in OVERPASS_URLS:
            try:
                chunk = _overpass_chunk(body, url)
            except Exception:
                continue
            got_chunk = True
            for rec in chunk:
                key = (
                    rec["name"].lower(),
                    round(rec["latitude"], 4),
                    round(rec["longitude"], 4),
                )
                if key in seen:
                    continue
                seen.add(key)
                records.append(rec)
            break  # chunk done, no need to try more mirrors
        if not got_chunk:
            print(f"Overpass chunk failed on all mirrors: {body[:40]}...",
                  file=sys.stderr)
        time.sleep(0.5)
    if not records:
        raise RuntimeError("All Overpass chunked queries failed or returned nothing")
    return records


def clean_osm(elements: list[dict]) -> list[dict]:
    records = []
    seen = set()
    for el in elements:
        tags = el.get("tags", {})
        name = (tags.get("name") or "").strip()
        category = (tags.get("amenity") or tags.get("shop") or "").strip()
        if not name or not category:
            continue
        lat = el.get("lat") or (el.get("center") or {}).get("lat")
        lon = el.get("lon") or (el.get("center") or {}).get("lon")
        if lat is None or lon is None:
            continue
        dedupe_key = (name.lower(), round(float(lat), 4), round(float(lon), 4))
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        addr_parts = [
            tags.get("addr:housenumber"),
            tags.get("addr:street"),
            tags.get("addr:city") or "Edmonton",
        ]
        records.append(
            {
                "name": name,
                "category": category,
                "address": " ".join(p for p in addr_parts if p) or None,
                "latitude": float(lat),
                "longitude": float(lon),
                "phone": tags.get("contact:phone") or tags.get("phone"),
                "website": tags.get("contact:website") or tags.get("website"),
                "opening_hours": tags.get("opening_hours"),
                "source": "osm",
            }
        )
    return records


# --------------------------------------------------------------------------
# Fallback: clearly-labelled seed dataset (last resort only)
# --------------------------------------------------------------------------
def seed_dataset() -> list[dict]:
    """Realistic, clearly-labelled fallback dataset (source='seed')."""
    seed = [
        ("Good Earth Coffeehouse - Jasper Ave", "cafe", "12102 Jasper Ave NW", 53.5456, -113.5284, "(780) 555-0132", "https://goodearthcafe.example", "Mo-Fr 07:00-18:00"),
        ("Transcend Coffee Roasters", "cafe", "8708 109 St NW", 53.5281, -113.5158, "(780) 555-0147", "https://transcendcoffee.example", "Mo-Su 08:00-20:00"),
        ("Iconoclast Coffee", "cafe", "10620 88 Ave NW", 53.5229, -113.5031, None, None, "Tu-Su 09:00-17:00"),
        ("District Cafe", "cafe", "10043 82 Ave NW", 53.5186, -113.4946, "(780) 555-0188", "https://districtcafe.example", "Mo-Su 08:00-22:00"),
        ("The Hat Barber Co.", "barbershop", "10556 82 Ave NW", 53.5194, -113.5003, "(780) 555-0121", "https://hatbarber.example", "Tu-Sa 09:00-18:00"),
        ("Blunt Salon", "beauty", "10178 105 St NW", 53.5420, -113.5018, "(780) 555-0165", "https://bluntsalon.example", "Mo-Sa 10:00-20:00"),
        ("Lash Loft YEG", "beauty", "8115 104 St NW", 53.5256, -113.5009, None, "https://lashloft.example", None),
        ("Hugo's Chops & Top Knot", "barbershop", "12409 102 Ave NW", 53.5495, -113.5343, "(780) 555-0119", None, "We-Sa 10:00-19:00"),
        ("Muttart Plumbing", "plumber", "9908 72 Ave NW", 53.5157, -113.4968, "(780) 555-0199", "https://muttartplumbing.example", "Mo-Fr 08:00-17:00"),
        ("Capital City Electric", "electrician", "11735 105 Ave NW", 53.5530, -113.5201, "(780) 555-0177", "https://capitalcityelectric.example", "Mo-Fr 08:00-17:00"),
        ("YEG Drain & Sewer", "plumber", "6315 104 St NW", 53.5021, -113.4998, "(780) 555-0155", None, "Mo-Su 07:00-22:00"),
        ("Northgate HVAC", "hvac", "9419 141 St NW", 53.5763, -113.5597, "(780) 555-0108", "https://northgatehvac.example", "Mo-Fr 08:00-17:00"),
        ("Rocky Mountain Roast Beef House", "restaurant", "8209 104 St NW", 53.5259, -113.4999, "(780) 555-0139", "https://rmroastbeef.example", "Mo-Su 11:00-21:00"),
        ("Padmanadi", "restaurant", "10740 101 St NW", 53.5520, -113.5011, "(780) 555-0142", "https://padmanadi.example", "We-Mo 11:00-21:00"),
        ("Bar Bricco", "restaurant", "10111 104 St NW", 53.5417, -113.5013, "(780) 555-0161", "https://barbricco.example", "Tu-Su 17:00-23:00"),
        ("Duo Ramen & Sushi", "restaurant", "10347 105 St NW", 53.5462, -113.5038, None, "https://duoramen.example", "Mo-Su 11:30-22:00"),
        ("Bistro Praha", "restaurant", "10168 100A St NW", 53.5415, -113.4991, "(780) 555-0182", "https://bistropraha.example", "Mo-Sa 11:30-23:00"),
        ("The Common", "bar", "9910 109 St NW", 53.5392, -113.5092, "(780) 555-0128", "https://thecommon.example", "We-Su 16:00-02:00"),
        ("Under the High Wheel", "bar", "8135 102 St NW", 53.5251, -113.4981, None, None, "Fr-Sa 18:00-02:00"),
        ("On the Rocks", "bar", "11740 Jasper Ave NW", 53.5448, -113.5230, "(780) 555-0112", "https://ontherocks.example", "Th-Sa 17:00-02:00"),
        ("Second Cup Whyte", "cafe", "10211 82 Ave NW", 53.5185, -113.4968, None, None, "Mo-Su 07:00-21:00"),
        ("Planet Organic Market", "grocery", "7910 104 St NW", 53.5219, -113.4995, "(780) 555-0151", "https://planetorganic.example", "Mo-Su 09:00-21:00"),
        ("H&W Produce Whyte", "grocery", "11120 82 Ave NW", 53.5184, -113.5089, "(780) 555-0173", None, "Mo-Su 09:00-20:00"),
        ("Blush Lane Organic", "grocery", "10150 109 St NW", 53.5409, -113.5087, "(780) 555-0190", "https://blushlane.example", "Mo-Su 08:00-21:00"),
        ("Italian Centre Shop", "grocery", "5028 95 St NW", 53.4921, -113.4912, "(780) 555-0134", "https://italiancentre.example", "Mo-Su 09:00-19:00"),
        ("T&T Supermarket West", "grocery", "200-9888 170 St NW", 53.5346, -113.6082, "(780) 555-0166", "https://tntsupermarket.example", "Mo-Su 09:00-21:30"),
        ("Meathead Inc.", "butcher", "12318 118 Ave NW", 53.5669, -113.5334, "(780) 555-0105", None, "Tu-Sa 09:00-18:00"),
        ("D'Amore's Fine Meats", "butcher", "9540 111 Ave NW", 53.5656, -113.4978, "(780) 555-0125", "https://damores.example", "Mo-Sa 09:00-18:00"),
        ("Scona Cycle", "bicycle_shop", "10450 82 Ave NW", 53.5188, -113.4999, "(780) 555-0140", "https://sconacycle.example", "Mo-Sa 10:00-18:00"),
        ("United Cycle", "bicycle_shop", "11746 124 St NW", 53.5652, -113.5366, "(780) 555-0185", "https://unitedcycle.example", "Mo-Sa 09:30-18:00"),
        ("The Pedalhead", "bicycle_shop", "10039 166 St NW", 53.5388, -113.6009, None, "https://pedalhead.example", "Tu-Sa 10:00-17:00"),
        ("Audreys Books", "bookstore", "10702 Jasper Ave NW", 53.5437, -113.5085, "(780) 555-0116", "https://audreysbooks.example", "Mo-Su 10:00-20:00"),
        ("Wee Book Inn Whyte", "bookstore", "8133 102 St NW", 53.5252, -113.4982, None, None, "Mo-Su 10:00-22:00"),
        ("Glass Bookshop", "bookstore", "10347 80 Ave NW", 53.5164, -113.5006, "(780) 555-0194", "https://glassbookshop.example", "Tu-Su 11:00-18:00"),
        ("Rogers Place Pro Shop", "sporting_goods", "10220 104 Ave NW", 53.5464, -113.5009, "(780) 555-0170", "https://rogersplace.example", "Mo-Su 10:00-18:00"),
        ("Totem Outfitters", "outdoor", "10159 81 Ave NW", 53.5170, -113.4956, "(780) 555-0123", "https://totemoutfitters.example", "Mo-Sa 10:00-18:00"),
        ("Valhalla Pure Outfitters", "outdoor", "10133 81 Ave NW", 53.5169, -113.4952, "(780) 555-0149", "https://valhallapure.example", "Mo-Sa 10:00-18:00"),
        ("Bentley Clothiers", "clothing", "10123 100 St NW", 53.5406, -113.4983, "(780) 555-0137", None, "Mo-Sa 10:00-18:00"),
        ("Red Pony", "clothing", "10265 123 St NW", 53.5410, -113.5316, None, "https://redpony.example", "Tu-Su 11:00-17:00"),
        ("Divine Decadence", "clothing", "10614 82 Ave NW", 53.5183, -113.5011, "(780) 555-0162", "https://divinedecadence.example", "Mo-Su 10:00-19:00"),
        ("Fox & Fable", "clothing", "10706 124 St NW", 53.5483, -113.5340, None, None, "We-Su 11:00-18:00"),
        ("Henry Singer", "clothing", "10190 104 St NW", 53.5410, -113.5018, "(780) 555-0189", "https://henrysinger.example", "Mo-Sa 10:00-18:00"),
        ("Cure Apothecary", "pharmacy", "8108 104 St NW", 53.5241, -113.4997, "(780) 555-0114", "https://cureapothecary.example", "Mo-Fr 09:00-18:00"),
        ("Whyte Ave Pharmacy", "pharmacy", "10405 82 Ave NW", 53.5186, -113.4994, "(780) 555-0130", None, "Mo-Sa 09:00-21:00"),
        ("The Growlery", "beer_store", "10420 121 St NW", 53.5405, -113.5293, "(780) 555-0158", "https://thegrowlery.example", "Mo-Su 12:00-22:00"),
        ("Keg n Cork Liquor", "liquor", "10454 82 Ave NW", 53.5188, -113.5001, "(780) 555-0179", "https://kegncork.example", "Mo-Su 10:00-23:00"),
        ("Wine & Beyond Windermere", "liquor", "6154 Currents Dr NW", 53.4658, -113.6118, "(780) 555-0102", "https://wineandbeyond.example", "Mo-Su 10:00-22:00"),
    ]
    return [
        {
            "name": n, "category": c, "address": a,
            "latitude": lat, "longitude": lon, "phone": p,
            "website": w, "opening_hours": h, "source": "seed",
        }
        for (n, c, a, lat, lon, p, w, h) in seed
    ]


# --------------------------------------------------------------------------
# Storage
# --------------------------------------------------------------------------
def store(records: list[dict], replace: bool = True) -> int:
    """Full-rebuild storage. Preserves first_seen/status for matching keys.

    Note: refresh.py (freshness engine) is the daily incremental path and the
    one that records change history. This full rebuild is the reset path —
    it keeps first_seen and active/closed status for businesses present in
    both old and new data, so history is not accidentally wiped.
    """
    init_db()
    today = date.today().isoformat()
    with get_conn() as conn:
        preserved: dict = {}
        if replace:
            for r in conn.execute(
                "SELECT name, address, first_seen, status FROM businesses"
            ):
                preserved[business_key(r["name"], r["address"])] = (
                    r["first_seen"] or today,
                    r["status"] or "active",
                )
            conn.execute("DELETE FROM businesses")
        rows = []
        for rec in records:
            fs, st = preserved.get(
                business_key(rec.get("name"), rec.get("address")),
                (today, "active"),
            )
            rows.append(
                {
                    **rec,
                    "first_seen": fs,
                    "last_seen": today,
                    "status": st,
                }
            )
        conn.executemany(
            """INSERT INTO businesses
               (name, category, address, latitude, longitude, phone, website,
                opening_hours, source, first_seen, last_seen, status)
               VALUES (:name, :category, :address, :latitude, :longitude,
                       :phone, :website, :opening_hours, :source,
                       :first_seen, :last_seen, :status)""",
            rows,
        )
        count = conn.execute("SELECT COUNT(*) AS c FROM businesses").fetchone()["c"]
    return count


# --------------------------------------------------------------------------
# Entry point — live first, seed only as a last resort
# --------------------------------------------------------------------------
def main() -> None:
    # Source 1: City of Edmonton Open Data (official, most reliable)
    try:
        records = fetch_edmonton_open_data()
        if len(records) < 1000:
            raise RuntimeError(
                f"Edmonton Open Data returned only {len(records)} usable "
                "records; need 1000+ for a credible live dataset."
            )
        count = store(records)
        print(f"LIVE INGEST (edmonton_open_data): stored {count} records.")
        return
    except Exception as e:
        print(f"Edmonton Open Data ingest failed ({e}). Trying Overpass.",
              file=sys.stderr)

    # Source 2: OpenStreetMap Overpass, small chunked queries
    try:
        records = fetch_osm()
        if len(records) < 100:
            raise RuntimeError(
                f"Overpass returned only {len(records)} usable records."
            )
        count = store(records)
        print(f"LIVE INGEST (osm): stored {count} records.")
        return
    except Exception as e:
        print(f"Overpass ingest failed ({e}). Using seed dataset.",
              file=sys.stderr)

    # Last resort: labelled seed dataset
    count = store(seed_dataset())
    print(f"SEED INGEST: stored {count} records (source=seed).")


if __name__ == "__main__":
    main()
