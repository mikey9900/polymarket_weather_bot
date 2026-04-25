"""Weather-native clustering buckets for research policy."""

from __future__ import annotations


def cluster_payload(
    *,
    market_type: str,
    city_slug: str,
    source_count: int,
    edge_abs: float,
    liquidity: float,
    time_to_resolution_s: float | None,
) -> dict[str, object]:
    edge_bucket = _edge_bucket(edge_abs)
    liquidity_bucket = _liquidity_bucket(liquidity)
    time_bucket = _time_bucket(time_to_resolution_s)
    cluster_id = "|".join(
        [
            str(market_type or "unknown"),
            str(city_slug or "unknown"),
            f"src_{max(1, int(source_count or 1))}",
            f"edge_{edge_bucket}",
            f"liq_{liquidity_bucket}",
            f"time_{time_bucket}",
        ]
    )
    return {
        "cluster_id": cluster_id,
        "market_type": str(market_type or "unknown"),
        "city_slug": str(city_slug or "unknown"),
        "source_count": max(1, int(source_count or 1)),
        "edge_bucket": edge_bucket,
        "liquidity_bucket": liquidity_bucket,
        "time_bucket": time_bucket,
    }


def _edge_bucket(edge_abs: float) -> str:
    if edge_abs < 0.12:
        return "small"
    if edge_abs < 0.20:
        return "medium"
    if edge_abs < 0.30:
        return "large"
    return "huge"


def _liquidity_bucket(liquidity: float) -> str:
    if liquidity < 50:
        return "thin"
    if liquidity < 250:
        return "ok"
    if liquidity < 1000:
        return "strong"
    return "deep"


def _time_bucket(time_to_resolution_s: float | None) -> str:
    if time_to_resolution_s is None:
        return "unknown"
    hours = time_to_resolution_s / 3600.0
    if hours < 6:
        return "imminent"
    if hours < 24:
        return "day"
    if hours < 96:
        return "multi_day"
    return "long"
