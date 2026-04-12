# =============================================================
# portfolio/portfolio_tracker.py
#
# PURPOSE:
#   Fetches open Polymarket positions for the configured wallet,
#   identifies weather markets, runs the latest forecast against
#   each position, and generates HOLD / BUY MORE / SELL recommendations.
#
# DATA FLOW:
#   1. Fetch positions → Polymarket Data API (no auth needed)
#   2. For each position, look up market details → Gamma API
#   3. Parse city + date from event slug
#   4. Run forecast → Open-Meteo + Visual Crossing
#   5. Compare forecast prob vs current market price vs entry price
#   6. Generate recommendation
# =============================================================

import os
import re
import requests
from datetime import date
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

POLYMARKET_WALLET = os.getenv("POLYMARKET_WALLET", "")
DATA_API          = "https://data-api.polymarket.com"
GAMMA_API         = "https://gamma-api.polymarket.com"

MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

# Keywords that identify a temperature market
WEATHER_KEYWORDS = ["temperature", "°f", "°c", "fahrenheit", "celsius"]


# =============================================================
# DATA FETCHING
# =============================================================

def fetch_positions() -> list:
    """
    Fetch all open positions for the configured wallet.
    No authentication required — public read-only endpoint.
    """
    if not POLYMARKET_WALLET:
        raise RuntimeError("POLYMARKET_WALLET not set in weather_bot.env")

    r = requests.get(
        f"{DATA_API}/positions",
        params={
            "user":           POLYMARKET_WALLET,
            "limit":          500,
            "sizeThreshold":  0.01,
            "sortBy":         "CURRENT",
            "sortDirection":  "DESC",
        },
        timeout=15,
    )
    r.raise_for_status()
    return r.json()


def get_market_details(condition_id: str) -> Optional[dict]:
    """
    Look up a market in the Gamma API by its condition ID.
    Returns the market dict (which includes event info), or None.
    """
    try:
        r = requests.get(
            f"{GAMMA_API}/markets",
            params={"conditionIds": condition_id},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        return data[0] if data else None
    except Exception as e:
        print(f"    ⚠️  Gamma API lookup failed for {condition_id}: {e}")
        return None


# =============================================================
# PARSING
# =============================================================

def parse_event_info(event_slug: str) -> Optional[dict]:
    """
    Extract city_slug and event_date from a Polymarket event slug.
    Format: highest-temperature-in-{city}-on-{month}-{day}-{year}
    """
    match = re.match(
        r"highest-temperature-in-(.+)-on-(\w+)-(\d+)-(\d+)$",
        event_slug,
    )
    if not match:
        return None

    city_slug = match.group(1)
    month_str = match.group(2).lower()
    day       = int(match.group(3))
    year      = int(match.group(4))
    month     = MONTH_MAP.get(month_str)

    if not month:
        return None

    try:
        event_date = date(year, month, day)
    except ValueError:
        return None

    return {"city_slug": city_slug, "event_date": event_date}


def is_weather_market(position: dict) -> bool:
    """Check if a position is a temperature/weather market."""
    title = (position.get("title") or "").lower()
    return any(kw in title for kw in WEATHER_KEYWORDS)


# =============================================================
# FORECAST INTEGRATION
# =============================================================

def get_forecast_prob_for_bucket(
    city_slug:    str,
    event_date:   date,
    bucket_label: str,
) -> Optional[float]:
    """
    Run forecast for a city/date and return the probability for
    a specific bucket label (e.g. "72-73°F").

    Uses Open-Meteo + Visual Crossing. Returns average of available sources.
    """
    from forecast.forecast_engine import (
        get_openmeteo_forecast_max_temp,
        get_visual_crossing_forecast_max_temp,
        CITY_COORDS,
        UNCERTAINTY_F,
        UNCERTAINTY_C,
        _normal_cdf,
    )
    from parser.weather_parser import parse_temperature_bucket

    coords = CITY_COORDS.get(city_slug)
    if not coords:
        return None

    unit = coords.get("unit", "fahrenheit")
    std  = UNCERTAINTY_F if unit == "fahrenheit" else UNCERTAINTY_C

    # Parse the bucket label into low/high bounds
    bounds = parse_temperature_bucket(bucket_label)
    if not bounds:
        return None

    low  = bounds.get("low")
    high = bounds.get("high")

    # Get temps from both sources
    om_temp = get_openmeteo_forecast_max_temp(city_slug, event_date)
    vc_temp = get_visual_crossing_forecast_max_temp(city_slug, event_date)

    probs = []
    for temp in [om_temp, vc_temp]:
        if temp is None:
            continue
        if low is None and high is None:
            continue
        elif low is None:
            p = _normal_cdf(high, temp, std)
        elif high is None:
            p = 1.0 - _normal_cdf(low, temp, std)
        else:
            p = _normal_cdf(high, temp, std) - _normal_cdf(low, temp, std)
        probs.append(max(0.0, min(1.0, p)))

    return round(sum(probs) / len(probs), 3) if probs else None


# =============================================================
# RECOMMENDATION LOGIC
# =============================================================

def get_recommendation(
    outcome:       str,
    avg_price:     float,
    cur_price:     float,
    forecast_prob: Optional[float],
) -> str:
    """
    Generate a trading recommendation based on current forecast vs position.

    Logic:
        - Edge = how much the forecast still favors your direction
        - Positive edge = forecast still backs you → hold or buy more
        - Negative edge = forecast has moved against you → consider selling
    """
    if forecast_prob is None:
        return "❓ No forecast available"

    holding_yes = outcome.lower() == "yes"

    if holding_yes:
        edge = forecast_prob - cur_price
    else:
        # Holding NO: you win if market falls, so you want forecast_prob low
        edge = cur_price - forecast_prob

    if edge >= 0.20:
        return "📈 BUY MORE — strong edge remains"
    elif edge >= 0.10:
        return "📈 BUY MORE — edge still there"
    elif edge >= 0.03:
        return "✋ HOLD — edge narrowing"
    elif edge >= -0.03:
        return "✋ HOLD — roughly fair value now"
    elif edge >= -0.10:
        return "⚠️ CONSIDER SELLING — edge gone"
    else:
        return "🚨 SELL — forecast now opposes your position"


# =============================================================
# FORMATTING
# =============================================================

def format_position(
    position:      dict,
    forecast_prob: Optional[float],
    bucket_label:  str = "",
) -> str:
    """Format a single position as a Telegram message block."""

    title      = position.get("title", "Unknown market")
    outcome    = position.get("outcome", "?")
    avg_price  = float(position.get("avgPrice") or 0)
    cur_price  = float(position.get("curPrice") or 0)
    cash_pnl   = float(position.get("cashPnl") or 0)
    pct_pnl    = float(position.get("percentPnl") or 0)
    size       = float(position.get("size") or 0)
    init_val   = float(position.get("initialValue") or 0)
    cur_val    = float(position.get("currentValue") or 0)
    end_date   = (position.get("endDate") or "")[:10]

    pnl_emoji = "📈" if cash_pnl >= 0 else "📉"
    pnl_sign  = "+" if cash_pnl >= 0 else ""

    rec = get_recommendation(outcome, avg_price, cur_price, forecast_prob)

    lines = [f"*{title}*"]
    if bucket_label:
        lines.append(f"Bucket: `{bucket_label}` | Holding: *{outcome}* | Expires: {end_date}")
    else:
        lines.append(f"Holding: *{outcome}* | Expires: {end_date}")

    price_line = f"Entry: {round(avg_price*100)}% → Now: {round(cur_price*100)}%"
    if forecast_prob is not None:
        price_line += f" | Forecast: {round(forecast_prob*100)}%"
    lines.append(price_line)

    lines.append(
        f"Size: {size:.0f} tokens  |  ${init_val:.2f} in → ${cur_val:.2f} now"
    )
    lines.append(f"{pnl_emoji} P&L: {pnl_sign}${cash_pnl:.2f} ({pnl_sign}{pct_pnl:.1f}%)")
    lines.append(f"→ *{rec}*")

    return "\n".join(lines)


# =============================================================
# MAIN
# =============================================================

def run_portfolio_check() -> list[str]:
    """
    Full portfolio check. Returns a list of Telegram messages to send.
    Splits into multiple messages so Telegram doesn't hit length limits.
    """
    print("📊 Fetching portfolio...")
    positions = fetch_positions()

    if not positions:
        return ["📭 No open positions found for your wallet."]

    weather_positions = [p for p in positions if is_weather_market(p)]
    other_count       = len(positions) - len(weather_positions)

    if not weather_positions:
        return [
            f"📭 No weather positions found.\n"
            f"({len(positions)} total open position(s), none are temperature markets)"
        ]

    messages = []
    total_pnl   = 0.0
    total_init  = 0.0

    for pos in weather_positions:
        condition_id  = pos.get("conditionId", "")
        outcome       = pos.get("outcome", "Yes")
        total_pnl    += float(pos.get("cashPnl") or 0)
        total_init   += float(pos.get("initialValue") or 0)

        forecast_prob = None
        bucket_label  = ""

        # Try to get forecast for this position
        if condition_id:
            market = get_market_details(condition_id)
            if market:
                bucket_label = market.get("groupItemTitle", "")

                # Get event info from the market's event list
                events     = market.get("events") or []
                event      = events[0] if events else {}
                event_slug = event.get("slug", "") or market.get("eventSlug", "")

                if event_slug and bucket_label:
                    info = parse_event_info(event_slug)
                    if info:
                        print(f"    🔍 Forecasting {info['city_slug']} on {info['event_date']} for {bucket_label}...")
                        forecast_prob = get_forecast_prob_for_bucket(
                            city_slug    = info["city_slug"],
                            event_date   = info["event_date"],
                            bucket_label = bucket_label,
                        )

        messages.append(format_position(pos, forecast_prob, bucket_label))

    # Summary footer
    pnl_sign  = "+" if total_pnl >= 0 else ""
    pnl_emoji = "📈" if total_pnl >= 0 else "📉"
    footer_lines = [
        f"{'─'*30}",
        f"{pnl_emoji} *Total weather P&L: {pnl_sign}${total_pnl:.2f}*",
        f"💰 Total invested: ${total_init:.2f}",
    ]
    if other_count:
        footer_lines.append(f"_(+{other_count} non-weather position(s) not shown)_")
    messages.append("\n".join(footer_lines))

    return messages
