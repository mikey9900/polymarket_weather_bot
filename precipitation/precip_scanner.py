# =============================================================
# precipitation/precip_scanner.py
#
# PURPOSE:
#   Fetches Polymarket monthly precipitation events by constructing
#   slugs directly for known cities and the current/next month.
#
# SLUG PATTERN (confirmed):
#   precipitation-in-{city}-in-{month}
#   e.g. "precipitation-in-nyc-in-april"
#
# CITIES:
#   Tries all known cities — caches misses to avoid re-checking
#   slots that Polymarket hasn't opened yet.
# =============================================================

import requests
import json
import os
from datetime import datetime, timezone, timedelta
import calendar

GAMMA      = "https://gamma-api.polymarket.com"
CACHE_FILE = os.path.join(os.path.dirname(__file__), "seen_precip_events.json")

# All cities to try — same set as temperature scanner
CITIES = [
    "nyc", "los-angeles", "chicago", "houston", "dallas", "austin",
    "san-francisco", "seattle", "denver", "atlanta", "miami",
    "london", "paris", "tokyo", "toronto", "mexico-city", "beijing",
    "shanghai", "singapore", "hong-kong", "seoul", "amsterdam",
    "madrid", "helsinki", "warsaw", "istanbul", "lagos",
    "buenos-aires", "sao-paulo", "jakarta", "kuala-lumpur",
    "tel-aviv", "moscow",
]

_MONTH_NAMES = [
    "", "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
]


# =============================================================
# CACHE HELPERS
# =============================================================

def _load_cache() -> set:
    if not os.path.exists(CACHE_FILE):
        return set()
    try:
        with open(CACHE_FILE, "r") as f:
            return set(json.load(f))
    except Exception:
        return set()


def _save_cache(seen: set):
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(sorted(seen), f, indent=2)
    except Exception as e:
        print(f"  ⚠️  Could not save precip cache: {e}")


# =============================================================
# SLUG BUILDER
# =============================================================

def _build_slug(city: str, year: int, month: int) -> str:
    """
    e.g. city="nyc", month=4 → "precipitation-in-nyc-in-april"
    """
    return f"precipitation-in-{city}-in-{_MONTH_NAMES[month]}"


# =============================================================
# MAIN FETCH FUNCTION
# =============================================================

def fetch_precip_events() -> list:
    """
    Fetches all active monthly precipitation events.

    Tries current month + next month for all known cities.
    Caches non-existent slugs to skip on future scans.

    Returns list of dicts:
        {
            "event":      raw event dict,
            "markets":    list of child market dicts,
            "year":       int,
            "month":      int,   # 1-12
            "city_slug":  str,
        }
    """
    seen    = _load_cache()
    results = []

    today = datetime.now(timezone.utc)
    # Try current month and next month
    months_to_check = []
    for delta in (0, 1):
        # Advance by delta months
        m = today.month + delta
        y = today.year + (m - 1) // 12
        m = ((m - 1) % 12) + 1
        months_to_check.append((y, m))

    total = len(CITIES) * len(months_to_check)
    print(f"  💧 Checking {len(CITIES)} cities × {len(months_to_check)} months = {total} slugs\n")

    for year, month in months_to_check:
        for city in CITIES:
            slug = _build_slug(city, year, month)

            if slug in seen:
                continue

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

            if not data or (isinstance(data, list) and len(data) == 0):
                seen.add(slug)  # confirmed non-existent — cache it
                continue

            event = data[0] if isinstance(data, list) else data

            if event.get("slug") != slug:
                seen.add(slug)
                continue

            markets  = event.get("markets") or []
            end_date = (event.get("endDate") or "")[:10]

            print(
                f"  ✅ {event.get('title')}\n"
                f"     Ends: {end_date} | {len(markets)} markets"
            )

            results.append({
                "event":     event,
                "markets":   markets,
                "year":      year,
                "month":     month,
                "city_slug": city,
            })

    # Only cache misses persist — valid events re-fetched each scan
    _save_cache(seen)
    print(f"\n  💧 {len(results)} precipitation event(s) found.\n")
    return results
