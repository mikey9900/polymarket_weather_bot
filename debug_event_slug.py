# =============================================================
# debug_event_slug.py
#
# PURPOSE:
#   Fetches a specific Polymarket event by slug and prints its
#   full structure so we can see exactly how temperature markets
#   are organized in the API.
#
# HOW TO RUN:
#   python debug_event_slug.py
# =============================================================

import requests
import json

GAMMA = "https://gamma-api.polymarket.com"

# The slug from the URL: polymarket.com/event/<slug>
SLUG = "highest-temperature-in-san-francisco-on-april-11-2026"

print(f"Fetching event by slug: {SLUG}\n")

# Try fetching the event directly by slug
r = requests.get(
    f"{GAMMA}/events",
    params={"slug": SLUG},
    timeout=15,
)
r.raise_for_status()
data = r.json()

print(f"Response type: {type(data)}")
print(f"Response length: {len(data) if isinstance(data, list) else 'N/A'}\n")

if isinstance(data, list) and len(data) > 0:
    event = data[0]
elif isinstance(data, dict):
    event = data
else:
    print("❌ No event found")
    exit()

print("=" * 60)
print("EVENT FIELDS:")
print("=" * 60)
for key, value in event.items():
    if key == "markets":
        print(f"  markets: [{len(value)} markets]")
        if value:
            print("  FIRST MARKET FIELDS:")
            for mk, mv in value[0].items():
                print(f"    {mk}: {repr(mv)[:80]}")
    else:
        print(f"  {key}: {repr(value)[:100]}")

print("\n" + "=" * 60)
print("ALL MARKET QUESTIONS IN THIS EVENT:")
print("=" * 60)
markets = event.get("markets", [])
for m in markets:
    print(f"  - {m.get('question')} | groupItemTitle: {m.get('groupItemTitle')} | threshold: {m.get('groupItemThreshold')}")

print(f"\nTotal markets in this event: {len(markets)}")

# Now let's find the pattern — what makes this event's slug predictable?
print("\n" + "=" * 60)
print("SLUG PATTERN ANALYSIS:")
print("=" * 60)
print(f"Full slug: {SLUG}")
print(f"Event title: {event.get('title')}")
print(f"Event startDate: {event.get('startDate')}")
print(f"Event endDate: {event.get('endDate')}")
print(f"Event id: {event.get('id')}")

# Check if there's a tag or category we can use
print(f"Event tags: {event.get('tags')}")
print(f"Event category: {event.get('category')}")

# Try fetching sibling events (same day, different city) to find the pattern
print("\n" + "=" * 60)
print("LOOKING FOR SIBLING TEMPERATURE EVENTS (same date pattern):")
print("=" * 60)

# Try a few city variations to find the slug pattern
test_slugs = [
    "highest-temperature-in-new-york-on-april-11-2026",
    "highest-temperature-in-nyc-on-april-11-2026",
    "highest-temperature-in-los-angeles-on-april-11-2026",
    "highest-temperature-in-chicago-on-april-11-2026",
    "highest-temperature-in-phoenix-on-april-11-2026",
    "highest-temperature-in-miami-on-april-11-2026",
]

found_siblings = []
for test_slug in test_slugs:
    r2 = requests.get(f"{GAMMA}/events", params={"slug": test_slug}, timeout=10)
    if r2.status_code == 200:
        d = r2.json()
        if d and len(d) > 0:
            e = d[0] if isinstance(d, list) else d
            print(f"  ✅ FOUND: {test_slug}")
            print(f"     title: {e.get('title')}")
            found_siblings.append(test_slug)
        else:
            print(f"  ❌ Not found: {test_slug}")
    else:
        print(f"  ❌ HTTP {r2.status_code}: {test_slug}")

print(f"\nFound {len(found_siblings)} sibling events")
print("\nConclusion: slug pattern is 'highest-temperature-in-<city>-on-<month>-<day>-<year>'")