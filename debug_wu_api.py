# =============================================================
# debug_wu_api.py
#
# Tests different Weather Underground / IBM Weather API endpoints
# to find which one works with your API key.
#
# HOW TO RUN:
#   python debug_wu_api.py
# =============================================================

import requests
import os
from dotenv import load_dotenv

load_dotenv()
KEY = os.getenv("WU_API_KEY")

if not KEY:
    print("❌ WU_API_KEY not found in .env")
    exit()

print(f"Testing API key: {KEY[:8]}...{KEY[-4:]}\n")

# San Francisco / KSFO as test case
LAT  = 37.6190
LON  = -122.3750
STATION = "KSFO"

TESTS = [
    # v1 geocode (what we're currently using)
    ("v1 geocode daily 5day",
     f"https://api.weather.com/v1/geocode/{LAT}/{LON}/forecast/daily/5day.json",
     {"units": "e", "language": "en-US", "format": "json", "apiKey": KEY}),

    # v3 geocode (newer endpoint)
    ("v3 geocode daily 5day",
     f"https://api.weather.com/v3/wx/forecast/daily/5day",
     {"geocode": f"{LAT},{LON}", "units": "e", "language": "en-US", "format": "json", "apiKey": KEY}),

    # v2 PWS observations (personal weather station)
    ("v2 PWS current observations",
     f"https://api.weather.com/v2/pws/observations/current",
     {"stationId": STATION, "format": "json", "units": "e", "apiKey": KEY}),

    # v2 PWS 7-day summary
    ("v2 PWS daily summary 7day",
     f"https://api.weather.com/v2/pws/dailysummary/7day",
     {"stationId": STATION, "format": "json", "units": "e", "apiKey": KEY}),

    # v3 ICAO code
    ("v3 ICAO code daily 5day",
     f"https://api.weather.com/v3/wx/forecast/daily/5day",
     {"iataCode": "SFO", "units": "e", "language": "en-US", "format": "json", "apiKey": KEY}),

    # v1 location daily (alternative)
    ("v1 location daily 7day",
     f"https://api.weather.com/v1/location/{STATION}:9:US/forecast/daily/7day.json",
     {"units": "e", "language": "en-US", "apiKey": KEY}),

    # v3 wx observations current
    ("v3 wx observations current",
     f"https://api.weather.com/v3/wx/observations/current",
     {"geocode": f"{LAT},{LON}", "units": "e", "language": "en-US", "format": "json", "apiKey": KEY}),
]

print(f"{'Test':<35} {'Status':>8}  {'Result'}")
print("-" * 80)

for name, url, params in TESTS:
    try:
        r = requests.get(url, params=params, timeout=10)
        status = r.status_code

        if status == 200:
            data = r.json()
            keys = list(data.keys())[:4]
            result = f"✅ OK — keys: {keys}"
        elif status == 401:
            result = "❌ 401 Unauthorized (wrong endpoint or plan)"
        elif status == 403:
            result = "❌ 403 Forbidden (endpoint not in your plan)"
        elif status == 404:
            result = "❌ 404 Not Found"
        else:
            result = f"❌ HTTP {status}: {r.text[:80]}"

    except Exception as e:
        status = "ERR"
        result = f"💥 {str(e)[:60]}"

    print(f"{name:<35} {str(status):>8}  {result}")

print("\nDone. Look for ✅ OK lines — those endpoints work with your key.")
print("If all fail with 401, check your key on wunderground.com/member/api-keys")