# =============================================================
# debug_sort_params.py
#
# PURPOSE:
#   Tests different sort/order parameters on the Polymarket
#   Gamma API to find which combination returns the NEWEST
#   events first (instead of 2021 junk).
#
# HOW TO RUN:
#   python debug_sort_params.py
#
# WHAT TO LOOK FOR:
#   The test that shows dates from 2025/2026 at the top is the
#   winner — paste the params from that test into the scanner.
# =============================================================

import requests

GAMMA = "https://gamma-api.polymarket.com"

# Different parameter combinations to try
# We'll see which one puts the newest events at the top
TESTS = [
    # Label, params dict
    ("Default (no sort)",
        {"active": True, "limit": 5}),

    ("order=endDate ascending=false",
        {"active": True, "limit": 5, "order": "endDate", "ascending": "false"}),

    ("order=endDate ascending=False",
        {"active": True, "limit": 5, "order": "endDate", "ascending": False}),

    ("order=end_date_iso ascending=false",
        {"active": True, "limit": 5, "order": "end_date_iso", "ascending": "false"}),

    ("order=startDate ascending=false",
        {"active": True, "limit": 5, "order": "startDate", "ascending": "false"}),

    ("order=created_at ascending=false",
        {"active": True, "limit": 5, "order": "created_at", "ascending": "false"}),

    ("sort=endDate direction=desc",
        {"active": True, "limit": 5, "sort": "endDate", "direction": "desc"}),

    ("sort=end_date_iso direction=desc",
        {"active": True, "limit": 5, "sort": "end_date_iso", "direction": "desc"}),

    # Polymarket sometimes uses _col suffix
    ("order=end_date_iso ascending=0",
        {"active": True, "limit": 5, "order": "end_date_iso", "ascending": "0"}),

    # Try fetching from the end using a very high offset
    ("offset=50000 (try to get newest via high offset)",
        {"active": True, "limit": 5, "offset": 50000}),

    ("offset=100000",
        {"active": True, "limit": 5, "offset": 100000}),
]

for label, params in TESTS:
    print(f"\n{'='*60}")
    print(f"TEST: {label}")
    print(f"{'='*60}")
    try:
        r = requests.get(f"{GAMMA}/events", params=params, timeout=10)
        r.raise_for_status()
        data = r.json()

        if not data:
            print("  → Empty response")
            continue

        for e in data:
            date = (e.get("endDate") or e.get("startDate") or "no date")[:10]
            title = (e.get("title") or "no title")[:70]
            print(f"  {date} | {title}")

    except Exception as ex:
        print(f"  → ERROR: {ex}")

print("\n" + "="*60)
print("DONE — look for which test shows 2025/2026 dates at the top")
print("="*60)