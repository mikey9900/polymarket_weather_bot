# clear_cache.py — run this once to wipe all old caches before scanning
import os, json

paths = [
    "scanner/seen_events.json",
    "seen_events.json",
    "scanner/seen_markets.json",
    "seen_markets.json",
]

found = False
for path in paths:
    if os.path.exists(path):
        try:
            ids = json.load(open(path))
            print(f"Deleted '{path}' ({len(ids)} cached IDs)")
        except:
            print(f"Deleted '{path}' (unreadable)")
        os.remove(path)
        found = True

if not found:
    print("No cache files found — nothing to delete.")
else:
    print("✅ All caches cleared. Next scan starts fresh.")
