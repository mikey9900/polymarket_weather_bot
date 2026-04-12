import requests

GAMMA = "https://gamma-api.polymarket.com"

r = requests.get(
    f"{GAMMA}/markets",
    params={"active": True, "limit": 1000},
    timeout=15,
)
r.raise_for_status()
markets = r.json()

total = len(markets)
with_threshold = 0
with_title = 0
with_both = 0

examples = []

for m in markets:
    has_thresh = m.get("groupItemThreshold") is not None
    has_title = m.get("groupItemTitle") is not None

    if has_thresh:
        with_threshold += 1
    if has_title:
        with_title += 1
    if has_thresh and has_title:
        with_both += 1
        if "temperature" in m.get("question", "").lower():
            examples.append(
                (
                    m["question"],
                    m["groupItemTitle"],
                    m["groupItemThreshold"],
                )
            )

print(f"Total markets: {total}")
print(f"Markets with groupItemThreshold: {with_threshold}")
print(f"Markets with groupItemTitle: {with_title}")
print(f"Markets with BOTH: {with_both}")
print("\nTemperature examples:")
for e in examples[:5]:
    print(" -", e)
