# =============================================================
# scanner/weather_event_scanner.py
#
# PURPOSE:
#   Fetch Polymarket daily temperature events by constructing
#   slugs directly from known cities and date ranges.
#
# SLUG PATTERN:
#   highest-temperature-in-<city>-on-<month>-<day>-<year>
#   e.g. "highest-temperature-in-san-francisco-on-april-11-2026"
# =============================================================

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

import requests

GAMMA = "https://gamma-api.polymarket.com"


# -------------------------------------------------------------
# CONFIRMED TEMPERATURE CITIES
# -------------------------------------------------------------

NORTH_AMERICA_CITIES = [
    "nyc",
    "los-angeles",
    "chicago",
    "houston",
    "dallas",
    "austin",
    "san-francisco",
    "seattle",
    "denver",
    "atlanta",
    "miami",
    "toronto",
    "mexico-city",
]

INTERNATIONAL_CITIES = [
    "london",
    "paris",
    "tokyo",
    "beijing",
    "shanghai",
    "singapore",
    "hong-kong",
    "seoul",
    "amsterdam",
    "madrid",
    "helsinki",
    "warsaw",
    "istanbul",
    "lagos",
    "buenos-aires",
    "sao-paulo",
    "jakarta",
    "kuala-lumpur",
    "tel-aviv",
    "moscow",
]

CITIES = [*NORTH_AMERICA_CITIES, *INTERNATIONAL_CITIES]

TEMPERATURE_MARKET_SCOPE_ALIASES = {
    "": "both",
    "all": "both",
    "both": "both",
    "global": "both",
    "na": "north_america",
    "northamerica": "north_america",
    "north-america": "north_america",
    "north america": "north_america",
    "north_america": "north_america",
    "domestic": "north_america",
    "international": "international",
    "intl": "international",
    "outside_north_america": "international",
}


# How many days behind today to include (grace period for recent markets)
DAYS_BEHIND = 1

# How many days ahead to look for upcoming markets
DAYS_AHEAD = 7

# Cache file stores slugs already confirmed missing so repeat scans skip them.
CACHE_FILE = os.path.join(os.path.dirname(__file__), "seen_events.json")


def normalize_temperature_market_scope(market_scope: str | None) -> str:
    raw = str(market_scope or "").strip().lower()
    return TEMPERATURE_MARKET_SCOPE_ALIASES.get(raw, "both")


def cities_for_temperature_market_scope(market_scope: str | None) -> list[str]:
    scope = normalize_temperature_market_scope(market_scope)
    if scope == "north_america":
        return list(NORTH_AMERICA_CITIES)
    if scope == "international":
        return list(INTERNATIONAL_CITIES)
    return list(CITIES)


def _market_scope_label(market_scope: str | None) -> str:
    scope = normalize_temperature_market_scope(market_scope)
    if scope == "north_america":
        return "North America"
    if scope == "international":
        return "International"
    return "Both"


# =============================================================
# CACHE HELPERS
# =============================================================

def _load_cache() -> set[str]:
    """Load seen event slugs from disk. Returns an empty set on first run."""
    if not os.path.exists(CACHE_FILE):
        return set()
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception as exc:
        print(f"  WARNING Cache unreadable ({exc}) - starting fresh")
        return set()
    return set(payload)


def _save_cache(seen: set[str]) -> None:
    """Save seen slugs once at the end of the scan."""
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as handle:
            json.dump(sorted(seen), handle, indent=2)
    except Exception as exc:
        print(f"  WARNING Could not save cache: {exc}")


def clear_cache() -> None:
    """
    Delete the cache file to force a fresh scan next time.
    Run manually:
        from scanner.weather_event_scanner import clear_cache
        clear_cache()
    """
    if os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)
        print("OK Cache cleared.")
    else:
        print("INFO No cache file found.")


# =============================================================
# SLUG BUILDER
# =============================================================

def _build_slug(city: str, dt: datetime) -> str:
    """Build the Polymarket event slug for a city and date."""
    month = dt.strftime("%B").lower()
    day = str(dt.day)
    year = str(dt.year)
    return f"highest-temperature-in-{city}-on-{month}-{day}-{year}"


# =============================================================
# MAIN FETCH FUNCTION
# =============================================================

def fetch_weather_events(limit: int = 300, *, market_scope: str = "both") -> list[dict]:
    """
    Fetch active daily temperature events by constructing slugs directly.

    Each returned item is:
        {
            "event":   { ...raw event dict from API... },
            "markets": [ ...list of child market dicts... ],
        }
    """

    scope = normalize_temperature_market_scope(market_scope)
    cities = cities_for_temperature_market_scope(scope)
    seen = _load_cache()
    results: list[dict] = []
    cache_hits = 0
    not_found = 0
    checked = 0

    today = datetime.now(timezone.utc)
    dates = [today + timedelta(days=offset) for offset in range(-DAYS_BEHIND, DAYS_AHEAD + 1)]

    total = len(cities) * len(dates)
    print(f"  SCAN Scope { _market_scope_label(scope) } | {len(cities)} cities x {len(dates)} dates = {total} slugs")

    for dt in dates:
        for city in cities:
            if len(results) >= limit:
                break

            slug = _build_slug(city, dt)
            checked += 1

            if slug in seen:
                cache_hits += 1
                continue

            try:
                response = requests.get(
                    f"{GAMMA}/events",
                    params={"slug": slug},
                    timeout=10,
                )
                response.raise_for_status()
                data = response.json()
            except Exception as exc:
                print(f"  WARNING Request failed for {slug}: {exc}")
                continue

            if not data or (isinstance(data, list) and len(data) == 0):
                not_found += 1
                seen.add(slug)
                continue

            event = data[0] if isinstance(data, list) else data
            if event.get("slug") != slug:
                not_found += 1
                seen.add(slug)
                continue

            markets = event.get("markets") or []
            end_date = (event.get("endDate") or "")[:10]
            print(f"  FOUND {event.get('title')}\n     Ends: {end_date} | {len(markets)} markets")

            # Valid events are re-fetched each scan so prices stay fresh.
            results.append({"event": event, "markets": markets})

        if len(results) >= limit:
            break

    _save_cache(seen)
    print(
        f"\nOK Done. {len(results)} events found. "
        f"({cache_hits} cache hits | {not_found} not found | {checked} slugs checked)\n"
    )
    return results
