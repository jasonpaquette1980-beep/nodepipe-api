"""Per-key protection for Nodepipe: monthly plan quotas + per-minute rate limit.

Design notes (be honest about limits):
  - Monthly quotas come from billing.TIERS (free/growth/scale) plus the
    special 'demo' tier for the documented demo key.
  - The per-minute rate limit is an IN-PROCESS fixed window (thread-safe).
    It is correct at single-instance scale (one uvicorn worker). Before
    running multiple API workers/instances, move this to Redis and share
    the counters.
  - Quota checks run before usage is logged, so rejected (429) calls are
    NOT metered. /v1/usage is quota-exempt so a capped customer can always
    see their plan/used/limit/remaining.
"""
from __future__ import annotations

import threading
import time
from datetime import datetime, timezone

RATE_LIMIT_PER_MINUTE = 60
WINDOW_SECONDS = 60

_lock = threading.Lock()
# subject (api key, or client IP for unauthenticated hits) ->
# (window_start_epoch, count_in_window)
_windows: dict[str, tuple[float, int]] = {}


class RateLimited(Exception):
    def __init__(self, retry_after_seconds: int):
        self.retry_after_seconds = retry_after_seconds


class QuotaExceeded(Exception):
    def __init__(self, plan: str, limit: int, used: int):
        self.plan = plan
        self.limit = limit
        self.used = used


def check_rate_limit(subject: str) -> None:
    """Raise RateLimited if `subject` exceeded 60 requests in the last minute."""
    now = time.time()
    with _lock:
        start, count = _windows.get(subject, (now, 0))
        if now - start >= WINDOW_SECONDS:
            start, count = now, 0
        if count >= RATE_LIMIT_PER_MINUTE:
            retry = int(WINDOW_SECONDS - (now - start)) + 1
            raise RateLimited(retry)
        _windows[subject] = (start, count + 1)


def month_reset_iso() -> str:
    """First day of next month (UTC) — when monthly quotas reset."""
    now = datetime.now(timezone.utc)
    if now.month == 12:
        nxt = now.replace(year=now.year + 1, month=1, day=1)
    else:
        nxt = now.replace(month=now.month + 1, day=1)
    return nxt.date().isoformat()


def quota_for_tier(tier_name: str) -> int:
    from billing import TIERS  # local import: billing imports db, not this

    return TIERS.get(tier_name, TIERS["free"]).included_calls


def check_monthly_quota(api_key: str) -> dict:
    """Raise QuotaExceeded if the key used up its plan's monthly calls."""
    from db import key_tier, monthly_usage  # local import to avoid cycles

    tier = key_tier(api_key)
    limit = quota_for_tier(tier)
    used = monthly_usage(api_key)
    if used >= limit:
        raise QuotaExceeded(tier, limit, used)
    return {"plan": tier, "limit": limit, "used": used,
            "remaining": max(0, limit - used)}
