# =============================================================
# parser/weather_parser.py
#
# PURPOSE:
#   1. Detects if a market is a temperature market
#   2. Filters out closed/resolved markets (no trading opportunity)
#   3. Parses temperature range buckets for probability comparison
#
# CLOSED MARKET FILTER:
#   When a market resolves, Polymarket sets outcomePrices to
#   ["1", "0"] (YES won) or ["0", "1"] (NO won).
#   These markets are done — no point alerting on them.
#   We also check the "closed" field directly.
#
# LIQUIDITY FILTER:
#   Markets with very low volume have wide spreads and are
#   hard to trade. We skip markets below MIN_LIQUIDITY.
# =============================================================

import re
import json as _json
from typing import Optional

# Minimum liquidity (in USD) for a market to be worth alerting on.
# Markets below this threshold are too illiquid to trade reliably.
# Adjust this based on your trading size — $500 is a reasonable default.
MIN_LIQUIDITY = 5.0

# Temperature keywords that must appear in the market text
TEMP_KEYWORDS = [
    "temperature", "daily high", "daily low", "high temp",
    "low temp", "max temp", "min temp", "highest temp",
    "°f", "°c", "fahrenheit", "celsius", "heat index",
    "high of", "low of", "exceed", "highest temperature",
]

# Forbidden keywords — reject even if a temp keyword matched
FORBIDDEN_KEYWORDS = [
    "prison", "sentence", "years in prison", "jail", "convicted",
    "trial", "verdict", "guilty",
    "percent", "%", "yield", "bond", "fed funds",
    "election", "nominee", "president", "governor", "senate",
    "token", "crypto", "nft", "bitcoin", "ethereum",
    "nba", "nhl", "nfl", "mlb", "tournament", "playoff",
    "world cup", "championship", "gta", "jesus",
]


def _get_yes_price(market: dict) -> Optional[float]:
    """
    Extracts the YES price from a market's outcomePrices field.
    Returns a float 0.0-1.0 or None.
    """
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
    """
    Returns True if the market is already resolved/closed.

    Checks two things:
    1. The "closed" field — Polymarket sets this True when resolved
    2. outcomePrices — resolved markets have prices at exactly 0 or 1
       e.g. ["1", "0"] = YES won, ["0", "1"] = NO won

    We want to skip these because:
    - You can't trade a closed market
    - The 0%/100% prices create fake huge discrepancies vs our forecast
    """

    # Direct closed field check
    if market.get("closed") is True:
        return True

    # Check if prices are locked at 0 or 1 (resolved)
    prices = market.get("outcomePrices")
    if prices:
        try:
            if isinstance(prices, str):
                prices = _json.loads(prices)
            yes = float(prices[0])
            no  = float(prices[1]) if len(prices) > 1 else None

            # If YES is exactly 0 or 1, the market has resolved
            if yes == 0.0 or yes == 1.0:
                return True

            # Also catch very close to 0/1 (sometimes API returns 0.001)
            if yes <= 0.01 or yes >= 0.99:
                return True

        except Exception:
            pass

    return False


def _get_liquidity(market: dict) -> float:
    """
    Returns the market's liquidity in USD.
    Child markets store liquidity in different fields depending on market type.
    We check multiple fields and return the first non-zero value found.
    """
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


def parse_temperature_market(market: dict) -> Optional[dict]:
    """
    Determines if a market is an active, liquid temperature market
    and extracts its data.

    Skips:
        - Non-temperature markets
        - Closed/resolved markets
        - Markets below MIN_LIQUIDITY

    Args:
        market: raw market dict from Polymarket API

    Returns:
        dict with keys: type, threshold, question, raw_title,
                        slug, label, bucket
        OR None if market should be skipped
    """

    # --- Temperature keyword check ---
    combined_text = " ".join([
        str(market.get("question", "")),
        str(market.get("groupItemTitle", "")),
        str(market.get("description", "")),
    ]).lower()

    if not any(k in combined_text for k in TEMP_KEYWORDS):
        return None

    if any(f in combined_text for f in FORBIDDEN_KEYWORDS):
        return None

    # --- Closed market filter ---
    if _is_market_closed(market):
        return None

    # --- Liquidity filter ---
    liquidity = _get_liquidity(market)
    if liquidity < MIN_LIQUIDITY:
        return None

    # --- Parse bucket ---
    group_title = market.get("groupItemTitle", "")
    bucket = parse_temperature_bucket(group_title)

    # --- Get numeric threshold ---
    threshold_value = None
    raw_threshold = market.get("groupItemThreshold")
    if raw_threshold is not None:
        try:
            threshold_value = float(raw_threshold)
        except (TypeError, ValueError):
            pass

    return {
        "type":      "temperature",
        "threshold": threshold_value,
        "question":  market.get("question"),
        "raw_title": " | ".join(filter(None, [
            market.get("question", ""),
            group_title,
        ])).strip(" |"),
        "slug":      market.get("slug"),
        "label":     group_title,
        "bucket":    bucket,
        "liquidity": liquidity,
    }


def parse_temperature_bucket(group_title: str) -> dict:
    """
    Parses a Polymarket groupItemTitle into a structured bucket dict.

    Examples:
        "47°F or below"  → {"low": None, "high": 47.5, "unit": "F"}
        "48-49°F"        → {"low": 47.5, "high": 49.5, "unit": "F"}
        "66°F or higher" → {"low": 65.5, "high": None, "unit": "F"}
        "14°C"           → {"low": 13.5, "high": 14.5, "unit": "C"}
    """

    t    = group_title.strip()
    unit = "C" if ("°C" in t or "°c" in t) else "F"

    # "X or below"
    m = re.match(r"^([\d.]+)[°]?[FC]?\s+or\s+below$", t, re.IGNORECASE)
    if m:
        val = float(m.group(1))
        return {"label": t, "low": None, "high": val + 0.5, "unit": unit}

    # "X or higher"
    m = re.match(r"^([\d.]+)[°]?[FC]?\s+or\s+higher$", t, re.IGNORECASE)
    if m:
        val = float(m.group(1))
        return {"label": t, "low": val - 0.5, "high": None, "unit": unit}

    # "X-Y°F" or "X-Y°C" range
    m = re.match(r"^([\d.]+)[\-–]([\d.]+)[°]?[FC]?$", t, re.IGNORECASE)
    if m:
        low  = float(m.group(1))
        high = float(m.group(2))
        return {"label": t, "low": low - 0.5, "high": high + 0.5, "unit": unit}

    # Single degree "X°C"
    m = re.match(r"^([\d.]+)[°]?[FC]?$", t, re.IGNORECASE)
    if m:
        val = float(m.group(1))
        return {"label": t, "low": val - 0.5, "high": val + 0.5, "unit": unit}

    return {"label": t, "low": None, "high": None, "unit": unit}


def parse_temperature_buckets_for_event(markets: list) -> list:
    """
    Parses all markets in an event into a sorted list of bucket dicts.
    Automatically filters out closed and illiquid markets.

    Args:
        markets: list of raw market dicts from one event

    Returns:
        list of bucket dicts sorted by threshold, only active liquid markets
    """

    buckets = []
    for market in markets:
        parsed = parse_temperature_market(market)
        if not parsed or not parsed.get("bucket"):
            continue

        bucket = parsed["bucket"].copy()
        bucket["market_slug"]      = market.get("slug")
        bucket["market_yes_price"] = _get_yes_price(market)
        bucket["threshold"]        = parsed.get("threshold")
        bucket["liquidity"]        = parsed.get("liquidity", 0.0)
        # event_slug is set later in run_scanner when we know the parent event
        bucket["event_slug"]       = ""
        buckets.append(bucket)

    # Sort by threshold index (0 = bottom range, 10 = top range)
    buckets.sort(key=lambda b: (b.get("threshold") or 0))
    return buckets