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

# Maps Polymarket city display names → city slugs used by forecast engine
CITY_DISPLAY_TO_SLUG = {
    "new york city": "nyc",
    "los angeles":   "los-angeles",
    "chicago":       "chicago",
    "houston":       "houston",
    "dallas":        "dallas",
    "austin":        "austin",
    "san francisco": "san-francisco",
    "seattle":       "seattle",
    "denver":        "denver",
    "atlanta":       "atlanta",
    "miami":         "miami",
    "london":        "london",
    "paris":         "paris",
    "tokyo":         "tokyo",
    "toronto":       "toronto",
    "mexico city":   "mexico-city",
    "beijing":       "beijing",
    "shanghai":      "shanghai",
    "singapore":     "singapore",
    "hong kong":     "hong-kong",
    "seoul":         "seoul",
    "amsterdam":     "amsterdam",
    "madrid":        "madrid",
    "helsinki":      "helsinki",
    "warsaw":        "warsaw",
    "istanbul":      "istanbul",
    "lagos":         "lagos",
    "buenos aires":  "buenos-aires",
    "são paulo":     "sao-paulo",
    "sao paulo":     "sao-paulo",
    "jakarta":       "jakarta",
    "kuala lumpur":  "kuala-lumpur",
    "tel aviv":      "tel-aviv",
    "moscow":        "moscow",
}


# =============================================================
# URL BUILDER
# =============================================================

_MONTH_NAMES = [
    "", "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
]

def get_event_url(city_slug: str, event_date: date) -> str:
    """
    Build a Polymarket event URL from city slug + event date.
    e.g. https://polymarket.com/event/highest-temperature-in-nyc-on-april-13-2026
    """
    month = _MONTH_NAMES[event_date.month]
    slug  = f"highest-temperature-in-{city_slug}-on-{month}-{event_date.day}-{event_date.year}"
    return f"https://polymarket.com/event/{slug}"


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


def is_weather_market(position: dict) -> bool:
    """Check if a position is a temperature/weather market."""
    title = (position.get("title") or "").lower()
    return any(kw in title for kw in WEATHER_KEYWORDS)


# =============================================================
# PARSING — extract city, bucket, date directly from title
# =============================================================

def parse_position_title(title: str, end_date_str: str) -> Optional[dict]:
    """
    Parse city, bucket label, and date from a position title.

    Title format:
        "Will the highest temperature in {City} be {bucket} on {Month} {Day}?"

    Examples:
        "Will the highest temperature in Seattle be 56-57°F on April 11?"
        "Will the highest temperature in New York City be 80°F or higher on April 13?"
        "Will the highest temperature in Tokyo be 19°C on April 12?"
    """
    match = re.match(
        r"Will the highest temperature in (.+?) be (.+?) on (\w+) (\d+)\??$",
        title.strip(),
        re.IGNORECASE,
    )
    if not match:
        return None

    city_display = match.group(1).strip().lower()
    bucket_raw   = match.group(2).strip()
    month_str    = match.group(3).lower()
    day          = int(match.group(4))

    # Strip "between " prefix (Polymarket sometimes adds it)
    bucket_label = re.sub(r"^between\s+", "", bucket_raw, flags=re.IGNORECASE)

    city_slug = CITY_DISPLAY_TO_SLUG.get(city_display)
    if not city_slug:
        print(f"    ⚠️  Unknown city: '{city_display}'")
        return None

    # Get year from endDate field (format: "2026-04-13T...")
    try:
        year = int(end_date_str[:4])
    except (ValueError, TypeError):
        year = date.today().year

    month = MONTH_MAP.get(month_str)
    if not month:
        return None

    try:
        event_date = date(year, month, day)
    except ValueError:
        return None

    return {
        "city_slug":    city_slug,
        "event_date":   event_date,
        "bucket_label": bucket_label,
    }


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

    # For today's markets use WU live observation as primary source
    from forecast.forecast_engine import get_wu_forecast_max_temp, get_noaa_forecast_max_temp
    today = date.today()

    wu_temp   = get_wu_forecast_max_temp(city_slug, event_date) if event_date <= today else None
    om_temp   = get_openmeteo_forecast_max_temp(city_slug, event_date)
    vc_temp   = get_visual_crossing_forecast_max_temp(city_slug, event_date)
    noaa_temp = get_noaa_forecast_max_temp(city_slug, event_date)  # US only

    probs = []
    for temp in [wu_temp, om_temp, vc_temp, noaa_temp]:
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
    event_date:    Optional[date] = None,
) -> str:
    """
    Generate a trading recommendation based on current forecast vs position.

    For YES positions: compare forecast YES prob vs current YES token price.
    For NO positions:  compare forecast NO prob (1 - forecast) vs current NO token price.

    If market expires today or is past: market price reflects real observations,
    not forecast models — flag as resolving instead of recommending trades.
    """
    today = date.today()

    # Markets expiring today or already past
    if event_date is not None:
        if event_date < today:
            return "🔴 EXPIRED — awaiting redemption"
        if event_date == today:
            # Market price now reflects real-time weather observations
            # Use cur_price (price of the token you hold) to advise
            if cur_price <= 0.05:
                return "🚨 SELL NOW — market says you lose, salvage what's left"
            elif cur_price >= 0.90:
                return "✋ HOLD — you're winning, let it resolve"
            elif cur_price < avg_price * 0.6:
                return "⚠️ CONSIDER SELLING — resolving today and underwater"
            else:
                return "⏰ RESOLVING TODAY — uncertain, watch live weather"

    if forecast_prob is None:
        return "❓ No forecast available"

    holding_yes = outcome.lower() == "yes"

    if holding_yes:
        # Edge = how much forecast (YES prob) exceeds current YES market price
        edge = forecast_prob - cur_price
    else:
        # Holding NO tokens: curPrice IS the NO token price
        # Edge = how much forecast (NO prob) exceeds current NO token price
        forecast_no_prob = 1.0 - forecast_prob
        edge = forecast_no_prob - cur_price

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
    event_date:    Optional[date] = None,
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

    rec = get_recommendation(outcome, avg_price, cur_price, forecast_prob, event_date)

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

def run_portfolio_check() -> list[dict]:
    """
    Full portfolio check.
    Returns list of dicts: {"text": str, "url": str | None}
    Callers send each as a message, optionally with a [📊 View →] button for the URL.
    """
    print("📊 Fetching portfolio...")
    positions = fetch_positions()

    if not positions:
        return [{"text": "📭 No open positions found for your wallet.", "url": None}]

    weather_positions = [p for p in positions if is_weather_market(p)]
    other_count       = len(positions) - len(weather_positions)

    if not weather_positions:
        return [{
            "text": (
                f"📭 No weather positions found.\n"
                f"({len(positions)} total open position(s), none are temperature markets)"
            ),
            "url": None,
        }]

    results    = []
    total_pnl  = 0.0
    total_init = 0.0

    for pos in weather_positions:
        title    = pos.get("title", "")
        end_date = pos.get("endDate", "")
        total_pnl  += float(pos.get("cashPnl") or 0)
        total_init += float(pos.get("initialValue") or 0)

        forecast_prob = None
        bucket_label  = ""
        event_url     = None

        info = parse_position_title(title, end_date)
        if info:
            city_slug    = info["city_slug"]
            event_date   = info["event_date"]
            bucket_label = info["bucket_label"]
            print(f"    🔍 Forecasting {city_slug} on {event_date} for '{bucket_label}'...")
            forecast_prob = get_forecast_prob_for_bucket(city_slug, event_date, bucket_label)
            event_url     = get_event_url(city_slug, event_date)

        event_date_obj = info["event_date"] if info else None
        text = format_position(pos, forecast_prob, bucket_label, event_date_obj)
        results.append({"text": text, "url": event_url})

    # Summary footer (no URL)
    pnl_sign  = "+" if total_pnl >= 0 else ""
    pnl_emoji = "📈" if total_pnl >= 0 else "📉"
    footer_lines = [
        f"{'─'*30}",
        f"{pnl_emoji} *Total weather P&L: {pnl_sign}${total_pnl:.2f}*",
        f"💰 Total invested: ${total_init:.2f}",
    ]
    if other_count:
        footer_lines.append(f"_(+{other_count} non-weather position(s) not shown)_")
    results.append({"text": "\n".join(footer_lines), "url": None})

    return results


# =============================================================
# BACKGROUND AUTO-TRACK (silent — no Telegram messages)
# =============================================================

def run_portfolio_auto_track():
    """
    Silently fetch open positions and auto-mark matching tracked edges as bought.
    Called by background loop every 30 minutes. Sends no Telegram messages.
    """
    from tracking.scan_tracker import auto_mark_bought
    try:
        positions = fetch_positions()
    except Exception as e:
        print(f"    ⚠️  Portfolio auto-track fetch failed: {e}")
        return

    weather_positions = [p for p in positions if is_weather_market(p)]
    for pos in weather_positions:
        title     = pos.get("title", "")
        end_date  = pos.get("endDate", "")
        outcome   = pos.get("outcome", "")
        cur_price = float(pos.get("curPrice") or 0)

        info = parse_position_title(title, end_date)
        if info and outcome:
            auto_mark_bought(
                city_slug    = info["city_slug"],
                event_date   = info["event_date"],
                bucket_label = info["bucket_label"],
                outcome      = outcome,
                market_prob  = cur_price,
            )
