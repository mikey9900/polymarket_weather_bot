# =============================================================
# polymarket/polymarket_prices.py
#
# PURPOSE:
#   Fetches the current YES price for a Polymarket market.
#
# HOW POLYMARKET PRICING WORKS:
#   Polymarket is a prediction market. Each market has two
#   outcomes: YES and NO. The prices are probabilities expressed
#   as decimals between 0.0 and 1.0.
#
#   Example: YES price = 0.72 means the market thinks there's
#   a 72% chance the event happens.
#
#   YES price + NO price ≈ 1.0 (small spread for liquidity)
#
# NO AUTH REQUIRED:
#   The Gamma API is public — you don't need an API key.
# =============================================================

import requests
from typing import Optional

# Base URL for Polymarket's public Gamma API
GAMMA_BASE_URL = "https://gamma-api.polymarket.com"


def get_yes_price(market_url: str) -> Optional[float]:
    """
    Fetches the current YES price for a Polymarket market.

    Args:
        market_url (str): The full Polymarket URL for the market.
                          e.g. "https://polymarket.com/event/will-phoenix-high-exceed-100f"
                          The slug (last part of the URL) is extracted automatically.

    Returns:
        float between 0.0 and 1.0 representing the YES probability,
        or None if the price couldn't be fetched.
    """

    # Extract the slug from the URL
    # e.g. "https://polymarket.com/event/will-it-rain-nyc" → "will-it-rain-nyc"
    slug = market_url.rstrip("/").split("/")[-1]

    try:
        # Fetch market data from the Gamma API using the slug
        response = requests.get(
            f"{GAMMA_BASE_URL}/markets/{slug}",
            timeout=10,
        )
        response.raise_for_status()  # raises if HTTP error
        data = response.json()

        # The Gamma API returns outcomePrices as a list: [YES_price, NO_price]
        # Both are strings, so we convert to float
        outcome_prices = data.get("outcomePrices")

        # Validate we got a proper list with at least 2 values
        if not outcome_prices or len(outcome_prices) < 2:
            print(f"⚠️  No outcomePrices found for slug: {slug}")
            return None

        # Index 0 is always YES, index 1 is always NO
        yes_price = float(outcome_prices[0])
        return yes_price

    except Exception as e:
        # Don't crash the bot — just log the failure and return None
        print(f"⚠️  Polymarket price fetch failed for {slug}: {e}")
        return None