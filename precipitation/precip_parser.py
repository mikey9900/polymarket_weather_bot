# =============================================================
# precipitation/precip_parser.py
#
# PURPOSE:
#   Parses Polymarket precipitation bucket labels into
#   structured low/high bounds for probability calculations.
#
# EXPECTED LABEL FORMATS (Polymarket groupItemTitle):
#   "Less than 1 inch"          → low=None,  high=1.0,  unit="in"
#   "1 to 2 inches"             → low=1.0,   high=2.0,  unit="in"
#   "2 to 3 inches"             → low=2.0,   high=3.0,  unit="in"
#   "3 inches or more"          → low=3.0,   high=None, unit="in"
#   "Less than 25mm"            → low=None,  high=25.0, unit="mm"
#   "25 to 50mm"                → low=25.0,  high=50.0, unit="mm"
#   "50mm or more"              → low=50.0,  high=None, unit="mm"
# =============================================================

import re
import json as _json
from typing import Optional

MIN_LIQUIDITY = 5.0


def _get_yes_price(market: dict) -> Optional[float]:
    prices = market.get("outcomePrices")
    if not prices:
        return None
    try:
        if isinstance(prices, str):
            prices = _json.loads(prices)
        return float(prices[0])
    except Exception:
        return None


def _is_market_closed(market: dict) -> bool:
    if market.get("closed") is True:
        return True
    prices = market.get("outcomePrices")
    if prices:
        try:
            if isinstance(prices, str):
                prices = _json.loads(prices)
            yes = float(prices[0])
            if yes <= 0.01 or yes >= 0.99:
                return True
        except Exception:
            pass
    return False


def _get_liquidity(market: dict) -> float:
    for field in ("liquidityNum", "liquidity", "liquidityClob"):
        val = market.get(field)
        if val:
            try:
                f = float(val)
                if f > 0:
                    return f
            except Exception:
                pass
    return 0.0


def parse_precip_bucket(label: str) -> Optional[dict]:
    """
    Parses a precipitation bucket label into low/high bounds.

    Returns:
        {"low": float|None, "high": float|None, "unit": "in"|"mm", "label": str}
        None if the label cannot be parsed.
    """
    t    = label.strip()
    unit = "mm" if re.search(r"\bmm\b", t, re.IGNORECASE) else "in"

    # "Less than X" / "Under X" / "Below X"
    m = re.match(
        r"^(?:less\s+than|under|below)\s+([\d.]+)\s*(?:inches?|in\.?|mm)?$",
        t, re.IGNORECASE,
    )
    if m:
        return {"low": None, "high": float(m.group(1)), "unit": unit, "label": t}

    # "X or more" / "X or above" / "X inches or more" / "X+ inches"
    m = re.match(
        r"^([\d.]+)\s*(?:inches?|in\.?|mm)?\s*(?:or\s+(?:more|above|higher)|\+)$",
        t, re.IGNORECASE,
    )
    if m:
        return {"low": float(m.group(1)), "high": None, "unit": unit, "label": t}

    # "X to Y" / "X - Y" / "between X and Y"
    m = re.match(
        r"^(?:between\s+)?([\d.]+)\s*(?:inches?|in\.?|mm)?\s*(?:to|–|-|and)\s*([\d.]+)\s*(?:inches?|in\.?|mm)?$",
        t, re.IGNORECASE,
    )
    if m:
        low  = float(m.group(1))
        high = float(m.group(2))
        return {"low": low, "high": high, "unit": unit, "label": t}

    return None


def parse_precip_buckets_for_event(markets: list) -> list:
    """
    Parses all markets in a precipitation event into a sorted
    list of bucket dicts ready for probability calculation.

    Filters out closed and illiquid markets.

    Returns list of dicts:
        {
            "label":            str,
            "low":              float | None,
            "high":             float | None,
            "unit":             "in" | "mm",
            "market_yes_price": float,
            "market_slug":      str,
            "liquidity":        float,
            "event_slug":       str,   # set by run_precipitation_scanner
        }
    """
    buckets = []

    for market in markets:
        if _is_market_closed(market):
            continue

        liquidity = _get_liquidity(market)
        if liquidity < MIN_LIQUIDITY:
            continue

        yes_price = _get_yes_price(market)
        if yes_price is None:
            continue

        group_title = (market.get("groupItemTitle") or "").strip()
        if not group_title:
            continue

        bucket = parse_precip_bucket(group_title)
        if not bucket:
            continue

        bucket["market_yes_price"] = yes_price
        bucket["market_slug"]      = market.get("slug", "")
        bucket["liquidity"]        = liquidity
        bucket["event_slug"]       = ""  # filled in by caller
        buckets.append(bucket)

    # Sort by lower bound (None = bottom → sort first)
    buckets.sort(key=lambda b: (b["low"] is None, b["low"] or 0))
    return buckets
