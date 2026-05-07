"""Order-book based fill rehearsal for shadow order intents."""

from __future__ import annotations

import json
import threading
import time
from dataclasses import replace
from typing import Any

import requests

from .models import ShadowOrderIntent
from ..models import iso_now


CLOB_BASE_URL = "https://clob.polymarket.com"
BOOK_CACHE_TTL_SECONDS = 2.0

_book_cache_lock = threading.Lock()
_book_cache: dict[str, tuple[dict[str, Any] | None, float]] = {}


def enrich_shadow_intent_with_fill_rehearsal(intent: ShadowOrderIntent) -> ShadowOrderIntent:
    """Attach public order-book context to a shadow intent without changing trading behavior."""

    rehearsal = build_shadow_fill_rehearsal(intent)
    payload = dict(intent.payload or {})
    payload["shadow_fill_rehearsal"] = rehearsal
    return replace(
        intent,
        clob_token_id=_string_or_none(rehearsal.get("clob_token_id")),
        book_best_bid=_as_float(rehearsal.get("book_best_bid")),
        book_best_ask=_as_float(rehearsal.get("book_best_ask")),
        book_spread=_as_float(rehearsal.get("book_spread")),
        book_midpoint=_as_float(rehearsal.get("book_midpoint")),
        book_depth_at_target_shares=_as_float(rehearsal.get("book_depth_at_target_shares")),
        book_depth_at_target_usd=_as_float(rehearsal.get("book_depth_at_target_usd")),
        simulated_fill_status=str(rehearsal.get("simulated_fill_status") or "unknown"),
        simulated_fill_shares=_as_float(rehearsal.get("simulated_fill_shares")),
        simulated_avg_fill_price=_as_float(rehearsal.get("simulated_avg_fill_price")),
        simulated_notional_usd=_as_float(rehearsal.get("simulated_notional_usd")),
        simulated_unfilled_shares=_as_float(rehearsal.get("simulated_unfilled_shares")),
        simulated_slippage_bps=_as_float(rehearsal.get("simulated_slippage_bps")),
        execution_checked_at=_string_or_none(rehearsal.get("execution_checked_at")),
        execution_error=_string_or_none(rehearsal.get("execution_error")),
        payload=payload,
    )


def build_shadow_fill_rehearsal(intent: ShadowOrderIntent) -> dict[str, Any]:
    checked_at = iso_now()
    token_id = _intent_clob_token_id(intent)
    if not token_id:
        return {
            "execution_checked_at": checked_at,
            "simulated_fill_status": "missing_token_id",
            "execution_error": "No CLOB token ID was available on the signal or entry shadow intent.",
        }

    try:
        book = fetch_clob_order_book(token_id)
    except Exception as exc:
        return {
            "execution_checked_at": checked_at,
            "clob_token_id": token_id,
            "simulated_fill_status": "book_error",
            "execution_error": str(exc),
        }
    if not book:
        return {
            "execution_checked_at": checked_at,
            "clob_token_id": token_id,
            "simulated_fill_status": "book_empty",
            "execution_error": "CLOB returned an empty order book.",
        }

    bids = _sorted_levels(book.get("bids"), reverse=True)
    asks = _sorted_levels(book.get("asks"), reverse=False)
    target_price = _bounded_price(intent.target_price)
    shares = max(0.0, float(intent.shares or 0.0))
    is_buy = str(intent.order_action or "").upper() == "BUY"
    executable_levels = [level for level in (asks if is_buy else bids) if _level_crosses_target(level, target_price, is_buy=is_buy)]
    fill = _simulate_fill(
        executable_levels,
        requested_shares=shares,
    )
    reference_price = _bounded_price(intent.reference_price)
    avg_fill = fill["avg_fill_price"]
    slippage_bps = None
    if avg_fill is not None and reference_price is not None and reference_price > 0:
        if is_buy:
            slippage_bps = round((avg_fill - reference_price) / reference_price * 10000.0, 4)
        else:
            slippage_bps = round((reference_price - avg_fill) / reference_price * 10000.0, 4)

    best_bid = bids[0]["price"] if bids else None
    best_ask = asks[0]["price"] if asks else None
    spread = round(best_ask - best_bid, 6) if best_bid is not None and best_ask is not None else None
    midpoint = round((best_bid + best_ask) / 2.0, 6) if best_bid is not None and best_ask is not None else None
    top_levels = {
        "bids": bids[:5],
        "asks": asks[:5],
    }
    return {
        "execution_checked_at": checked_at,
        "clob_token_id": token_id,
        "book_market": _string_or_none(book.get("market")),
        "book_asset_id": _string_or_none(book.get("asset_id")),
        "book_timestamp": _string_or_none(book.get("timestamp")),
        "book_hash": _string_or_none(book.get("hash")),
        "book_min_order_size": _as_float(book.get("min_order_size")),
        "book_tick_size": _as_float(book.get("tick_size")),
        "book_best_bid": best_bid,
        "book_best_ask": best_ask,
        "book_best_bid_size": bids[0]["size"] if bids else None,
        "book_best_ask_size": asks[0]["size"] if asks else None,
        "book_spread": spread,
        "book_midpoint": midpoint,
        "book_depth_at_target_shares": fill["available_shares"],
        "book_depth_at_target_usd": fill["available_notional_usd"],
        "simulated_fill_status": fill["status"],
        "simulated_fill_shares": fill["fill_shares"],
        "simulated_avg_fill_price": avg_fill,
        "simulated_notional_usd": fill["notional_usd"],
        "simulated_unfilled_shares": fill["unfilled_shares"],
        "simulated_slippage_bps": slippage_bps,
        "order_crosses_book": bool(executable_levels),
        "top_levels": top_levels,
    }


def fetch_clob_order_book(token_id: str) -> dict[str, Any] | None:
    token_id = str(token_id or "").strip()
    if not token_id:
        return None
    now = time.monotonic()
    with _book_cache_lock:
        cached = _book_cache.get(token_id)
        if cached is not None:
            value, expires_at = cached
            if expires_at > now:
                return value
            _book_cache.pop(token_id, None)
    response = requests.get(
        f"{CLOB_BASE_URL}/book",
        params={"token_id": token_id},
        timeout=3,
    )
    response.raise_for_status()
    payload = response.json()
    book = payload if isinstance(payload, dict) else None
    with _book_cache_lock:
        _book_cache[token_id] = (book, time.monotonic() + BOOK_CACHE_TTL_SECONDS)
    return book


def extract_clob_token_ids(payload: dict[str, Any] | None) -> list[str]:
    if not isinstance(payload, dict):
        return []
    for key in ("clob_token_ids", "clobTokenIds", "token_ids", "tokenIds"):
        parsed = _parse_token_id_list(payload.get(key))
        if parsed:
            return parsed
    yes_id = _string_or_none(payload.get("yes_token_id") or payload.get("yesTokenId"))
    no_id = _string_or_none(payload.get("no_token_id") or payload.get("noTokenId"))
    return [value for value in (yes_id, no_id) if value]


def token_id_for_outcome(payload: dict[str, Any] | None, outcome_side: str) -> str | None:
    explicit_single = _string_or_none((payload or {}).get("clob_token_id") or (payload or {}).get("clobTokenId"))
    if explicit_single:
        return explicit_single
    token_ids = extract_clob_token_ids(payload)
    outcome = str(outcome_side or "").strip().upper()
    if outcome == "YES" and token_ids:
        return token_ids[0]
    if outcome == "NO" and len(token_ids) >= 2:
        return token_ids[1]
    if outcome == "NO":
        return _string_or_none((payload or {}).get("no_token_id") or (payload or {}).get("noTokenId"))
    if outcome == "YES":
        return _string_or_none((payload or {}).get("yes_token_id") or (payload or {}).get("yesTokenId"))
    return None


def _intent_clob_token_id(intent: ShadowOrderIntent) -> str | None:
    if intent.clob_token_id:
        return str(intent.clob_token_id)
    payload = dict(intent.payload or {})
    explicit = token_id_for_outcome(payload, intent.outcome_side)
    if explicit:
        return explicit
    entry_payload = payload.get("entry_shadow_order") if isinstance(payload.get("entry_shadow_order"), dict) else None
    if entry_payload:
        explicit = token_id_for_outcome(entry_payload, intent.outcome_side)
        if explicit:
            return explicit
    return None


def _simulate_fill(levels: list[dict[str, float]], *, requested_shares: float) -> dict[str, Any]:
    remaining = max(0.0, float(requested_shares or 0.0))
    fill_shares = 0.0
    notional = 0.0
    available_shares = 0.0
    available_notional = 0.0
    for level in levels:
        size = max(0.0, float(level.get("size") or 0.0))
        price = _bounded_price(level.get("price"))
        if price is None or size <= 0:
            continue
        available_shares += size
        available_notional += size * price
        if remaining <= 0:
            continue
        take = min(remaining, size)
        fill_shares += take
        notional += take * price
        remaining -= take
    fill_shares = round(fill_shares, 6)
    notional = round(notional, 6)
    unfilled = round(max(0.0, requested_shares - fill_shares), 6)
    if fill_shares <= 0:
        status = "no_fill"
        avg_fill_price = None
    elif unfilled > 0:
        status = "partial_fill"
        avg_fill_price = round(notional / fill_shares, 6)
    else:
        status = "full_fill"
        avg_fill_price = round(notional / fill_shares, 6)
    return {
        "status": status,
        "fill_shares": fill_shares,
        "avg_fill_price": avg_fill_price,
        "notional_usd": notional,
        "unfilled_shares": unfilled,
        "available_shares": round(available_shares, 6),
        "available_notional_usd": round(available_notional, 6),
    }


def _level_crosses_target(level: dict[str, float], target_price: float | None, *, is_buy: bool) -> bool:
    if target_price is None:
        return False
    price = _bounded_price(level.get("price"))
    if price is None:
        return False
    if is_buy:
        return price <= target_price
    return price >= target_price


def _sorted_levels(levels: Any, *, reverse: bool) -> list[dict[str, float]]:
    normalized: list[dict[str, float]] = []
    if not isinstance(levels, list):
        return normalized
    for item in levels:
        if not isinstance(item, dict):
            continue
        price = _bounded_price(item.get("price"))
        size = _as_float(item.get("size"))
        if price is None or size is None or size <= 0:
            continue
        normalized.append({"price": price, "size": round(size, 6)})
    normalized.sort(key=lambda item: item["price"], reverse=reverse)
    return normalized


def _parse_token_id_list(value: Any) -> list[str]:
    raw = value
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        try:
            raw = json.loads(text)
        except json.JSONDecodeError:
            return [text]
    if not isinstance(raw, list):
        return []
    return [str(item).strip() for item in raw if str(item).strip()]


def _bounded_price(value: Any) -> float | None:
    raw = _as_float(value)
    if raw is None:
        return None
    return round(max(0.0, min(1.0, raw)), 6)


def _as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _string_or_none(value: Any) -> str | None:
    raw = str(value or "").strip()
    return raw or None
