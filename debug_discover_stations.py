# =============================================================
# debug_discover_stations.py
#
# PURPOSE:
#   Fetches the exact Weather Underground station ID that
#   Polymarket uses to resolve each city's temperature market.
#
#   This is critical — if we forecast with the wrong location,
#   our discrepancy detection will be wrong.
#
# HOW IT WORKS:
#   Fetches today's event for each city and reads the
#   resolutionSource field, which contains the WU station URL.
#   e.g. "https://www.wunderground.com/history/daily/us/ca/san-francisco/KSFO"
#   The last part of the URL is the station ID: "KSFO"
#
# HOW TO RUN:
#   python debug_discover_stations.py
# =============================================================

import requests
from datetime import datetime, timezone, timedelta

GAMMA = "https://gamma-api.polymarket.com"

CITIES = [
    "nyc", "los-angeles", "chicago", "houston", "dallas", "austin",
    "san-francisco", "seattle", "denver", "atlanta", "miami",
    "london", "paris", "tokyo", "toronto", "mexico-city",
    "beijing", "shanghai", "singapore", "hong-kong", "seoul",
    "amsterdam", "madrid", "helsinki", "warsaw", "istanbul",
    "lagos", "buenos-aires", "sao-paulo", "jakarta", "kuala-lumpur",
    "tel-aviv", "moscow",
]

today = datetime.now(timezone.utc)

print("Fetching WU station IDs for each city...\n")

results = {}

for city in CITIES:
    # Try today and the next 3 days to find an active event
    found = False
    for offset in range(4):
        dt    = today + timedelta(days=offset)
        month = dt.strftime("%B").lower()
        day   = str(dt.day)
        year  = str(dt.year)
        slug  = f"highest-temperature-in-{city}-on-{month}-{day}-{year}"

        try:
            r = requests.get(
                f"{GAMMA}/events",
                params={"slug": slug},
                timeout=10,
            )
            if r.status_code != 200:
                continue

            data = r.json()
            if not data or len(data) == 0:
                continue

            event = data[0] if isinstance(data, list) else data
            if event.get("slug") != slug:
                continue

            # Get resolutionSource from the event or first market
            res_source = event.get("resolutionSource", "")

            # If not on event, check first market
            if not res_source:
                markets = event.get("markets", [])
                if markets:
                    res_source = markets[0].get("resolutionSource", "")

            # Extract station ID from URL
            # e.g. ".../KSFO" → "KSFO"
            station_id = res_source.rstrip("/").split("/")[-1] if res_source else "UNKNOWN"

            # Also grab the full URL for reference
            results[city] = {
                "station_id":     station_id,
                "resolution_url": res_source,
                "event_title":    event.get("title", ""),
            }

            print(f"  ✅ {city:<20} → {station_id:<12} ({res_source})")
            found = True
            break

        except Exception as e:
            continue

    if not found:
        print(f"  ❌ {city:<20} → No event found")
        results[city] = {"station_id": "UNKNOWN", "resolution_url": "", "event_title": ""}

print(f"\n{'='*60}")
print("CITY → STATION MAPPING (paste into forecast_engine.py):")
print(f"{'='*60}\n")
print("CITY_TO_WU_STATION = {")
for city, info in results.items():
    sid = info["station_id"]
    print(f'    "{city}": "{sid}",')
print("}")