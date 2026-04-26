"""Helpers for current Polymarket contract pricing."""

from __future__ import annotations

import json
import threading
import time
from typing import Any, Optional

import requests


GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
PRICE_CACHE_TTL_SECONDS = 30.0

_price_cache_lock = threading.Lock()
_price_cache: dict[str, tuple[float | None, float]] = {}


def get_yes_price(market_url: str) -> Optional[float]:
    """Fetch the current YES price for a Polymarket child market slug or URL."""

    slug = _extract_slug(market_url)
    if not slug:
        return None

    cached = _get_cached_price(slug)
    if cached is not None:
        return cached

    try:
        response = requests.get(
            f"{GAMMA_BASE_URL}/markets",
            params={"slug": slug},
            timeout=10,
        )
        response.raise_for_status()
        market = _first_market(response.json())
        yes_price = _extract_yes_price(market)
        if yes_price is None:
            print(f"⚠️  No outcomePrices found for slug: {slug}")
        _cache_price(slug, yes_price)
        return yes_price
    except Exception as exc:
        print(f"⚠️  Polymarket price fetch failed for {slug}: {exc}")
        return None


def _extract_slug(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    return raw.rstrip("/").split("/")[-1]


def _get_cached_price(slug: str) -> Optional[float] | None:
    now = time.monotonic()
    with _price_cache_lock:
        cached = _price_cache.get(slug)
        if cached is None:
            return None
        value, expires_at = cached
        if expires_at <= now:
            _price_cache.pop(slug, None)
            return None
        return value


def _cache_price(slug: str, value: float | None) -> None:
    with _price_cache_lock:
        _price_cache[slug] = (value, time.monotonic() + PRICE_CACHE_TTL_SECONDS)


def _first_market(payload: Any) -> dict[str, Any]:
    if isinstance(payload, list):
        return payload[0] if payload else {}
    if isinstance(payload, dict):
        for key in ("data", "markets"):
            nested = payload.get(key)
            if isinstance(nested, list):
                return nested[0] if nested else {}
        return payload
    return {}


def _extract_yes_price(market: dict[str, Any]) -> Optional[float]:
    prices = market.get("outcomePrices")
    if not prices:
        return None
    try:
        if isinstance(prices, str):
            prices = json.loads(prices)
        if not isinstance(prices, list) or len(prices) < 1:
            return None
        return float(prices[0])
    except Exception:
        return None
