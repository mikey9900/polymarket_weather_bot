import requests

url = "https://gamma-api.polymarket.com/events"
params = {
    "active": True,
    "tags": "weather",
    "limit": 20,
}

r = requests.get(url, params=params)
r.raise_for_status()

events = r.json()
print(f"Found {len(events)} weather-tagged EVENTS")

for e in events:
    print("-", e["title"])