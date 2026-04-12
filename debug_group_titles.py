import requests

GAMMA = "https://gamma-api.polymarket.com"

r = requests.get(
    f"{GAMMA}/markets",
    params={"active": True, "limit": 200},
    timeout=15
)
r.raise_for_status()
markets = r.json()

print("SAMPLE groupItemTitle VALUES:\n")

count = 0
for m in markets:
    title = m.get("groupItemTitle")
    if title:
        print("-", repr(title))
        count += 1
    if count >= 25:
        break