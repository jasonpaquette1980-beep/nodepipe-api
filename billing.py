"""Pricing tiers and (stub) Stripe metered-billing integration.

MVP status: the usage data for billing comes from the `usage` table in db.py.
Stripe calls are stubbed — they run fine without real Stripe keys and raise a
clear error only if you try a live Stripe action without configuration.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

from db import monthly_usage

# ---------------------------------------------------------------- pricing
@dataclass
class Tier:
    name: str
    price_cents: int          # base monthly price
    included_calls: int       # included API calls per month
    overage_per_1k_cents: int  # price per extra 1,000 calls


TIERS = {
    "free": Tier(name="free", price_cents=0, included_calls=1_000,
                 overage_per_1k_cents=0),
    "growth": Tier(name="growth", price_cents=4_900, included_calls=10_000,
                   overage_per_1k_cents=150),     # $49 base, $1.50 per 1k extra
    "scale": Tier(name="scale", price_cents=19_900, included_calls=100_000,
                  overage_per_1k_cents=100),      # $199 base, $1.00 per 1k extra
    # Demo tier: the documented demo_edmonton_2026 key. Generous but finite,
    # so abuse is bounded while all demo/verification flows keep working.
    "demo": Tier(name="demo", price_cents=0, included_calls=5_000,
                 overage_per_1k_cents=0),
}


def _month_reset_iso() -> str:
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    if now.month == 12:
        nxt = now.replace(year=now.year + 1, month=1, day=1)
    else:
        nxt = now.replace(month=now.month + 1, day=1)
    return nxt.date().isoformat()


def estimate_bill(api_key: str, tier_name: str | None = None) -> dict:
    """Estimate the current month's bill for a key from its metered usage."""
    from db import key_tier  # local import to avoid cycles at module load
    tier_name = tier_name or key_tier(api_key)
    tier = TIERS.get(tier_name, TIERS["free"])
    calls = monthly_usage(api_key)
    overage = max(0, calls - tier.included_calls)
    # charge per started 1k block
    blocks = (overage + 999) // 1000
    overage_cents = blocks * tier.overage_per_1k_cents
    return {
        "tier": tier.name,
        # Quota fields (additive; /v1/usage surfaces these for the caller).
        "plan": tier.name,
        "used": calls,
        "limit": tier.included_calls,
        "remaining": max(0, tier.included_calls - calls),
        "resets": _month_reset_iso(),
        "calls_this_month": calls,
        "included_calls": tier.included_calls,
        "overage_calls": overage,
        "base_price_cents": tier.price_cents,
        "overage_price_cents": overage_cents,
        "estimated_total_cents": tier.price_cents + overage_cents,
        "currency": "usd",
    }

# ------------------------------------------------------- Stripe integration
#
# STRIPE INTEGRATION POINT
# -----------------------
# To go live:
#   1. pip install stripe
#   2. export STRIPE_SECRET_KEY="sk_live_..."          (and STRIPE_WEBHOOK_SECRET)
#   3. Create metered products/prices in the Stripe Dashboard:
#        - "growth" base price ($49/mo) as a recurring price
#        - a metered price ($1.50 / 1k calls) attached to a Stripe Billing meter
#        - "scale"  base price ($199/mo) + metered price ($1.00 / 1k calls)
#      then put the price IDs in STRIPE_PRICE_GROWTH / STRIPE_PRICE_SCALE.
#   4. Replace the stub bodies below with real stripe.* calls and wire the
#      /v1/billing webhooks endpoint in main.py to `stripe.Webhook.construct_event`.
STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_PRICE_GROWTH = os.environ.get("STRIPE_PRICE_GROWTH", "")
STRIPE_PRICE_SCALE = os.environ.get("STRIPE_PRICE_SCALE", "")


class StripeNotConfigured(RuntimeError):
    pass


def _require_stripe() -> None:
    if not STRIPE_SECRET_KEY:
        raise StripeNotConfigured(
            "Stripe is not configured. Set STRIPE_SECRET_KEY (and price IDs) "
            "in the environment to enable live billing. "
            "Metering still works locally via the usage table."
        )


def create_checkout_session(api_key: str, tier_name: str) -> dict:
    """STUB: create a Stripe Checkout session to subscribe a customer to a tier."""
    _require_stripe()
    # Live implementation would be e.g.:
    #   import stripe
    #   stripe.api_key = STRIPE_SECRET_KEY
    #   return stripe.checkout.Session.create(...)
    raise NotImplementedError(
        "Live Stripe call intentionally not implemented in the MVP stub."
    )


def report_meter_event(api_key: str, quantity: int = 1) -> dict:
    """STUB: push a meter event to Stripe Billing (called nightly/per-call in prod)."""
    _require_stripe()
    # Live implementation would post to Stripe's Billing Meter Events API.
    raise NotImplementedError(
        "Live Stripe call intentionally not implemented in the MVP stub."
    )


def handle_webhook(payload: bytes, sig_header: str) -> dict:
    """STUB: handle Stripe webhook events (subscription created/cancelled, etc.)."""
    _require_stripe()
    # Live implementation: stripe.Webhook.construct_event(payload, sig_header, secret)
    raise NotImplementedError(
        "Live Stripe call intentionally not implemented in the MVP stub."
    )
