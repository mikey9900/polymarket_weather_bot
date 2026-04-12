import requests

GAMMA = "https://gamma-api.polymarket.com"

r = requests.get(
    f"{GAMMA}/markets",
    params={"active": True, "limit": 5},
    timeout=10
)
r.raise_for_status()

markets = r.json()

for i, m in enumerate(markets, start=1):
    print("="*40)
    print(f"MARKET {i}")
    for k in m.keys():
        print(k)
    print("OUTCOMES FIELD:", m.get("outcomes"))
