# =============================================================
# debug_discover_cities.py
#
# PURPOSE:
#   Discovers every city that Polymarket currently has temperature
#   markets for, by trying a broad range of city slugs.
#
# HOW IT WORKS:
#   We know the slug pattern is:
#     highest-temperature-in-<city>-on-<month>-<day>-<year>
#   We try today's date with a large list of possible city slugs
#   and see which ones return a valid event.
#
# HOW TO RUN:
#   python debug_discover_cities.py
#
# OUTPUT:
#   A list of working city slugs you can paste directly into
#   weather_event_scanner.py and forecast_engine.py
# =============================================================

import requests
from datetime import datetime, timezone

GAMMA = "https://gamma-api.polymarket.com"

# Try today and tomorrow to maximize chances of finding active markets
today = datetime.now(timezone.utc)

# Large list of candidate city slugs to try
# These follow the Polymarket URL slug format (lowercase, hyphens)
CANDIDATE_CITIES = [
    # US Cities
    "new-york", "nyc", "new-york-city",
    "los-angeles", "la",
    "chicago",
    "houston",
    "phoenix",
    "philadelphia",
    "san-antonio",
    "san-diego",
    "dallas",
    "san-jose",
    "austin",
    "jacksonville",
    "fort-worth",
    "columbus",
    "charlotte",
    "indianapolis",
    "san-francisco",
    "seattle",
    "denver",
    "nashville",
    "oklahoma-city",
    "el-paso",
    "washington-dc",
    "washington",
    "boston",
    "las-vegas",
    "portland",
    "memphis",
    "louisville",
    "baltimore",
    "milwaukee",
    "albuquerque",
    "tucson",
    "fresno",
    "sacramento",
    "mesa",
    "kansas-city",
    "atlanta",
    "omaha",
    "colorado-springs",
    "raleigh",
    "long-beach",
    "virginia-beach",
    "minneapolis",
    "tampa",
    "new-orleans",
    "miami",
    "cleveland",
    "bakersfield",
    "aurora",
    "anaheim",
    "honolulu",
    "santa-ana",
    "corpus-christi",
    "riverside",
    "lexington",
    "st-louis",
    "pittsburgh",
    "stockton",
    "cincinnati",
    "st-paul",
    "anchorage",
    "detroit",
    "orlando",

    # International
    "london",
    "paris",
    "berlin",
    "tokyo",
    "sydney",
    "toronto",
    "mexico-city",
    "beijing",
    "shanghai",
    "mumbai",
    "delhi",
    "dubai",
    "singapore",
    "hong-kong",
    "seoul",
    "amsterdam",
    "madrid",
    "rome",
    "barcelona",
    "vienna",
    "brussels",
    "zurich",
    "oslo",
    "stockholm",
    "copenhagen",
    "helsinki",
    "warsaw",
    "prague",
    "budapest",
    "bucharest",
    "istanbul",
    "athens",
    "cairo",
    "johannesburg",
    "nairobi",
    "lagos",
    "casablanca",
    "buenos-aires",
    "sao-paulo",
    "rio-de-janeiro",
    "santiago",
    "bogota",
    "lima",
    "jakarta",
    "bangkok",
    "kuala-lumpur",
    "manila",
    "karachi",
    "dhaka",
    "tehran",
    "baghdad",
    "riyadh",
    "tel-aviv",
    "moscow",
    "kyiv",
    "montreal",
    "vancouver",
    "calgary",
    "ottawa",
    "melbourne",
    "brisbane",
    "perth",
    "auckland",
]

print(f"Testing {len(CANDIDATE_CITIES)} city slugs against Polymarket API...\n")

found_cities   = []
not_found      = []

for city in CANDIDATE_CITIES:
    # Try today and tomorrow
    for offset in [0, 1]:
        from datetime import timedelta
        test_date = today + timedelta(days=offset)
        month = test_date.strftime("%B").lower()
        day   = str(test_date.day)
        year  = str(test_date.year)
        slug  = f"highest-temperature-in-{city}-on-{month}-{day}-{year}"

        try:
            r = requests.get(
                f"{GAMMA}/events",
                params={"slug": slug},
                timeout=8,
            )
            if r.status_code != 200:
                continue

            data = r.json()
            if not data or (isinstance(data, list) and len(data) == 0):
                continue

            event = data[0] if isinstance(data, list) else data
            if event.get("slug") != slug:
                continue

            # Found it!
            title    = event.get("title", "")
            end_date = event.get("endDate", "")[:10]
            markets  = event.get("markets", [])
            unit     = "°C" if any("°C" in (m.get("groupItemTitle","")) for m in markets) else "°F"

            print(f"  ✅ FOUND: {city} ({unit}) — {title}")
            found_cities.append({
                "slug": city,
                "title": title,
                "unit": "celsius" if unit == "°C" else "fahrenheit",
                "end_date": end_date,
            })
            break  # found for this city, no need to try tomorrow

        except Exception as e:
            continue

print(f"\n{'='*60}")
print(f"FOUND {len(found_cities)} CITIES:")
print(f"{'='*60}\n")

# Print in format ready to paste into weather_event_scanner.py
print("# Paste this into CITIES list in weather_event_scanner.py:")
print("CITIES = [")
for c in found_cities:
    print(f'    "{c["slug"]}",  # {c["unit"]}')
print("]\n")

# Print in format ready to paste into forecast_engine.py
print("# Paste this into CITY_COORDS in forecast_engine.py")
print("# (you'll need to add lat/lon for each new city):")
for c in found_cities:
    print(f'    "{c["slug"]}": {{"lat": ???, "lon": ???, "unit": "{c["unit"]}"}},')