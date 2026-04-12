# =============================================================
# scanner/weather_event_scanner.py
#
# PURPOSE:
#   Fetches Polymarket daily temperature events by constructing
#   their slugs directly from known cities and date ranges.
#
# SLUG PATTERN (confirmed):
#   highest-temperature-in-<city>-on-<month>-<day>-<year>
#   e.g. "highest-temperature-in-san-francisco-on-april-11-2026"
#
# CITIES:
#   All 33 confirmed active cities from debug_discover_cities.py
#   US cities use °F, international cities use °C.
#   Units are handled automatically by forecast_engine.py via
#   the CITY_COORDS dict — no manual unit switching needed here.
# =============================================================

import requests
import json
import os
from datetime import datetime, timezone, timedelta

GAMMA = "https://gamma-api.polymarket.com"

# -------------------------------------------------------------
# ALL 33 CONFIRMED CITIES (from debug_discover_cities.py)
# -------------------------------------------------------------
CITIES = [
    # US — Fahrenheit
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

    # International — Celsius
    "london",
    "paris",
    "tokyo",
    "toronto",
    "mexico-city",
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

# How many days behind today to include (grace period for recent markets)
DAYS_BEHIND = 1

# How many days ahead to look for upcoming markets
DAYS_AHEAD = 7

# Cache file — stores slugs we've already fetched so repeat scans skip them
CACHE_FILE = os.path.join(os.path.dirname(__file__), "seen_events.json")


# =============================================================
# CACHE HELPERS
# =============================================================

def _load_cache():
    """Loads seen event slugs from disk. Returns empty set on first run."""
    if not os.path.exists(CACHE_FILE):
        return set()
    try:
        with open(CACHE_FILE, "r") as f:
            return set(json.load(f))
    except Exception as e:
        print(f"  ⚠️  Cache unreadable ({e}) — starting fresh")
        return set()


def _save_cache(seen: set):
    """Saves seen slugs to disk once at end of scan."""
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(sorted(seen), f, indent=2)
    except Exception as e:
        print(f"  ⚠️  Could not save cache: {e}")


def clear_cache():
    """
    Deletes the cache file to force a fresh scan next time.
    Run manually:
        from scanner.weather_event_scanner import clear_cache
        clear_cache()
    """
    if os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)
        print("✅ Cache cleared.")
    else:
        print("ℹ️  No cache file found.")


# =============================================================
# SLUG BUILDER
# =============================================================

def _build_slug(city: str, dt: datetime) -> str:
    """
    Builds the Polymarket event slug for a city and date.

    Pattern: highest-temperature-in-<city>-on-<month>-<day>-<year>
    Example: highest-temperature-in-nyc-on-april-11-2026

    Args:
        city: city slug e.g. "san-francisco"
        dt:   datetime for the target date

    Returns:
        slug string
    """
    month = dt.strftime("%B").lower()  # "april"
    day   = str(dt.day)                # "11" (no leading zero)
    year  = str(dt.year)               # "2026"
    return f"highest-temperature-in-{city}-on-{month}-{day}-{year}"


# =============================================================
# MAIN FETCH FUNCTION
# =============================================================

def fetch_weather_events(limit: int = 300) -> list:
    """
    Fetches all active daily temperature events by constructing
    slugs directly for each city × date combination.

    Each returned item is:
        {
            "event":   { ...raw event dict from API... },
            "markets": [ ...list of child market dicts... ]
        }

    Args:
        limit: max number of events to return (safety cap)

    Returns:
        list of event bundle dicts
    """

    seen       = _load_cache()
    results    = []
    cache_hits = 0
    not_found  = 0
    checked    = 0

    today = datetime.now(timezone.utc)
    dates = [
        today + timedelta(days=d)
        for d in range(-DAYS_BEHIND, DAYS_AHEAD + 1)
    ]

    total = len(CITIES) * len(dates)
    print(f"  🌡️  Checking {len(CITIES)} cities × {len(dates)} dates = {total} slugs\n")

    for dt in dates:
        for city in CITIES:
            if len(results) >= limit:
                break

            slug = _build_slug(city, dt)
            checked += 1

            # Skip if already processed in a previous scan
            if slug in seen:
                cache_hits += 1
                continue

            # Fetch the event by exact slug
            try:
                r = requests.get(
                    f"{GAMMA}/events",
                    params={"slug": slug},
                    timeout=10,
                )
                r.raise_for_status()
                data = r.json()

            except Exception as e:
                print(f"  ⚠️  Request failed for {slug}: {e}")
                continue

            # Empty response = this event doesn't exist
            if not data or (isinstance(data, list) and len(data) == 0):
                not_found += 1
                seen.add(slug)  # cache: slug confirmed non-existent
                continue

            event = data[0] if isinstance(data, list) else data

            # Verify slug matches exactly (avoid partial matches)
            if event.get("slug") != slug:
                not_found += 1
                seen.add(slug)
                continue

            markets = event.get("markets") or []
            end_date = (event.get("endDate") or "")[:10]

            print(
                f"  ✅ {event.get('title')}\n"
                f"     Ends: {end_date} | {len(markets)} markets"
            )

            # Do NOT cache valid events — re-fetch each scan so prices stay fresh.
            # Only non-existent slugs are cached (above).
            results.append({
                "event":   event,
                "markets": markets,
            })

    _save_cache(seen)
    print(
        f"\n✅ Done. {len(results)} events found. "
        f"({cache_hits} cache hits | {not_found} not found | "
        f"{checked} slugs checked)\n"
    )

    return results