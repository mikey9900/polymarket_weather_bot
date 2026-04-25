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
    source_dispersion_pct: float = 0.0,
    source_age_hours: float | None = None,
    confidence: str = "",
) -> dict[str, object]:
    agreement = agreement_bucket(source_count, confidence)
    edge = edge_bucket(edge_abs)
    liquidity_band = liquidity_bucket(liquidity)
    timing = time_bucket(time_to_resolution_s)
    dispersion = dispersion_bucket(source_dispersion_pct)
    staleness = staleness_bucket(source_age_hours)
    cluster_id = "|".join(
        [
            str(market_type or "unknown"),
            str(city_slug or "unknown"),
            f"agree_{agreement}",
            f"edge_{edge}",
            f"liq_{liquidity_band}",
            f"time_{timing}",
            f"disp_{dispersion}",
            f"stale_{staleness}",
        ]
    )
    return {
        "cluster_id": cluster_id,
        "market_type": str(market_type or "unknown"),
        "city_slug": str(city_slug or "unknown"),
        "source_count": max(1, int(source_count or 1)),
        "agreement_bucket": agreement,
        "edge_bucket": edge,
        "liquidity_bucket": liquidity_band,
        "time_bucket": timing,
        "dispersion_bucket": dispersion,
        "staleness_bucket": staleness,
        "confidence_bucket": str(confidence or "unknown"),
    }


def agreement_bucket(source_count: int, confidence: str = "") -> str:
    confidence = str(confidence or "").strip().lower()
    count = max(1, int(source_count or 1))
    if count >= 3:
        return "three_plus"
    if count == 2 or confidence == "confirmed":
        return "two_source"
    if confidence.endswith("_only"):
        return confidence
    return "single_source"


def edge_bucket(edge_abs: float) -> str:
    if edge_abs < 0.12:
        return "small"
    if edge_abs < 0.20:
        return "medium"
    if edge_abs < 0.30:
        return "large"
    return "huge"


def liquidity_bucket(liquidity: float) -> str:
    if liquidity < 50:
        return "thin"
    if liquidity < 250:
        return "ok"
    if liquidity < 1000:
        return "strong"
    return "deep"


def time_bucket(time_to_resolution_s: float | None) -> str:
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


def dispersion_bucket(source_dispersion_pct: float) -> str:
    dispersion = max(0.0, float(source_dispersion_pct or 0.0))
    if dispersion < 0.03:
        return "tight"
    if dispersion < 0.08:
        return "steady"
    if dispersion < 0.16:
        return "wide"
    return "erratic"


def staleness_bucket(source_age_hours: float | None) -> str:
    if source_age_hours is None:
        return "unknown"
    hours = max(0.0, float(source_age_hours))
    if hours <= 1.0:
        return "live"
    if hours <= 6.0:
        return "fresh"
    if hours <= 24.0:
        return "aging"
    return "stale"
