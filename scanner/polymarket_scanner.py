# =============================================================
# scanner/polymarket_scanner.py
#
# PURPOSE:
#   Fetches Polymarket markets that have a `groupItemThreshold`
#   field set. This is useful for rain/snow accumulation markets
#   where the threshold is stored directly on the market.
#
# ⚠️  IMPORTANT NOTE ABOUT TEMPERATURE MARKETS:
#   Temperature markets on Polymarket do NOT reliably use
#   `groupItemThreshold`. They live inside parent "events" and
#   the threshold is usually embedded in the market question text
#   (e.g. "Will the high exceed 100°F?").
#
#   For temperature detection, use:
#       scanner/weather_event_scanner.py  ← correct approach
#       parser/weather_parser.py          ← parses the results
#
#   This file (polymarket_scanner.py) is kept for other use cases
#   like rain/snow threshold markets that DO use groupItemThreshold.
# =============================================================

import requests

# Polymarket's public API — no authentication required
GAMMA = "https://gamma-api.polymarket.com"


def fetch_threshold_markets(limit=1000, page_size=200):
    """
    Fetches active Polymarket markets that have a groupItemThreshold value.

    `groupItemThreshold` is a field Polymarket sets on certain markets
    to indicate a numeric threshold (e.g. "2 inches of rain", "6 inches
    of snow"). Not all weather markets use this field.

    Args:
        limit (int): Maximum number of matching markets to return.
        page_size (int): How many markets to fetch per API request.
                         200 is the max Polymarket allows per page.

    Returns:
        list of dicts, each containing:
            question          – the market question text
            groupItemTitle    – short label (e.g. "2 inches")
            groupItemThreshold – the numeric threshold value
            description       – longer description (sometimes empty)
            slug              – used to build the Polymarket URL
    """

    results = []   # markets that have a threshold value
    offset = 0     # pagination cursor

    while len(results) < limit:

        # Fetch a page of markets
        r = requests.get(
            f"{GAMMA}/markets",
            params={
                "active": True,       # only open/live markets
                "limit": page_size,   # markets per page (max 200)
                "offset": offset,     # where to start this page
            },
            timeout=15,
        )
        r.raise_for_status()  # raise if API returned an error
        batch = r.json()

        # If the API returned an empty list, we've reached the end
        if not batch:
            break

        for m in batch:
            # Only keep markets that have a groupItemThreshold set
            # `is not None` check is important — threshold could be 0.0
            if m.get("groupItemThreshold") is not None:
                results.append({
                    "question":           m.get("question"),
                    "groupItemTitle":     m.get("groupItemTitle"),
                    "groupItemThreshold": m.get("groupItemThreshold"),
                    "description":        m.get("description"),
                    "slug":               m.get("slug"),
                })

                # Stop once we've collected enough
                if len(results) >= limit:
                    break

        # Advance to the next page
        offset += page_size

    print(f"fetch_threshold_markets: found {len(results)} threshold markets")
    return results