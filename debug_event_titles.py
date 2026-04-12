# =============================================================
# debug_event_titles.py
#
# PURPOSE:
#   Fetches 1000 events from Polymarket (newest first) and prints
#   ALL titles so we can see exactly what weather markets look like
#   and what keywords we need to add.
#
# HOW TO RUN:
#   python debug_event_titles.py
#
# WHAT TO LOOK FOR:
#   Any title that looks like a weather/temperature market.
#   The exact phrasing will tell us what keywords to add.
# =============================================================

import requests

GAMMA = "https://gamma-api.polymarket.com"

# Broad terms to catch anything remotely weather-related in the summary
BROAD_WEATHER = [
    "temp", "rain", "snow", "weather", "wind", "fog", "humid",
    "precip", "storm", "degree", "warm", "cold", "heat", "frost",
    "freeze", "sunny", "cloud", "thunder", "lightning", "hail",
    "drought", "flood", "celsius", "fahrenheit", "high of", "low of",
    "will it be", "climate",
]

print("Fetching 1000 events (newest first)...\n")

all_events = []
offset = 0

for page in range(10):  # 10 pages x 100 = 1000 events
    r = requests.get(
        f"{GAMMA}/events",
        params={
            "active":    True,
            "limit":     100,
            "offset":    offset,
            "order":     "startDate",
            "ascending": "false",
        },
        timeout=15,
    )
    r.raise_for_status()
    batch = r.json()

    if not batch:
        print(f"API returned empty batch at page {page + 1} — stopping")
        break

    all_events.extend(batch)
    offset += 100
    print(f"Page {page + 1}: {len(batch)} events (total so far: {len(all_events)})")

print(f"\n{'='*60}")
print(f"ALL {len(all_events)} EVENT TITLES (newest first):")
print(f"{'='*60}\n")

for e in all_events:
    date  = (e.get("endDate") or e.get("startDate") or "no date")[:10]
    title = (e.get("title") or "no title")
    print(f"{date} | {title}")

# Now filter for anything that looks remotely weather-related
print(f"\n{'='*60}")
print("POSSIBLE WEATHER EVENTS (broad keyword scan):")
print(f"{'='*60}\n")

found = []
for e in all_events:
    title = (e.get("title") or "").lower()
    if any(k in title for k in BROAD_WEATHER):
        date = (e.get("endDate") or e.get("startDate") or "no date")[:10]
        found.append((date, e.get("title", "")))

if found:
    for date, title in found:
        print(f"  {date} | {title}")
else:
    print("  ❌ Nothing found with broad weather keywords either.")
    print("  This means either:")
    print("  1. Polymarket has no active weather/temperature markets right now")
    print("  2. They use completely different terminology than expected")
    print("\n  Try looking at polymarket.com directly and searching 'temperature'")
    print("  to see what exact titles they use — then we can match them.")

print(f"\nTotal possible weather events: {len(found)} out of {len(all_events)}")