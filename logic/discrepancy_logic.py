# =============================================================
# logic/discrepancy_logic.py
#
# PURPOSE:
#   Compares forecast probabilities from weather sources to
#   Polymarket market prices. Identifies edges and classifies them
#   by confidence:
#
#   LARGE edge      - one source shows 20%+ discrepancy
#   CONFIRMED       - 2+ sources agree on direction and size
#   SMALL edge      - one source shows 10-20% discrepancy
# =============================================================

from typing import Iterable, List, Optional

SMALL_EDGE_THRESHOLD = 0.10
LARGE_EDGE_THRESHOLD = 0.20
TITLE_PREFIX = "Highest temperature in "

PROVIDER_SPECS = (
    ("wu", "WU", "wu_only", "wu_temp", "wu_prob", "WU"),
    ("om", "Open-Meteo", "om_only", "om_temp", "om_prob", "OM"),
    ("vc", "Visual Crossing", "vc_only", "vc_temp", "vc_prob", "VC"),
    ("noaa", "NOAA", "noaa_only", "noaa_temp", "noaa_prob", "NOAA"),
    ("weatherapi", "WeatherAPI", "weatherapi_only", "weatherapi_temp", "weatherapi_prob", "WA"),
)

PROVIDER_BADGES = {
    "confirmed": "2 SOURCES AGREE",
    "wu_only": "WU only",
    "om_only": "OM only",
    "vc_only": "VC only",
    "noaa_only": "NOAA only",
    "weatherapi_only": "WeatherAPI only",
}

PROVIDER_ICONS = {
    "confirmed": "[2x]",
    "wu_only": "[WU]",
    "om_only": "[OM]",
    "vc_only": "[VC]",
    "noaa_only": "[NOAA]",
    "weatherapi_only": "[WA]",
}


def _check_single_source(
    label: str,
    market_prob: float,
    forecast_prob: float,
    source: str,
) -> Optional[dict]:
    """
    Checks a single source's forecast against the market price.
    Returns a discrepancy dict if above threshold, else None.
    """
    if market_prob is None or forecast_prob is None:
        return None

    diff = forecast_prob - market_prob
    if abs(diff) < SMALL_EDGE_THRESHOLD:
        return None

    return {
        "label": label,
        "source": source,
        "market_prob": round(market_prob, 3),
        "forecast_prob": round(forecast_prob, 3),
        "discrepancy": round(diff, 3),
        "direction": "YES" if diff > 0 else "NO",
        "edge_size": "large" if abs(diff) >= LARGE_EDGE_THRESHOLD else "small",
    }


def _iter_provider_specs() -> Iterable[tuple[str, str, str, str, str, str]]:
    return PROVIDER_SPECS


def _single_source_results(label: str, market_prob: float, provider_probabilities: dict[str, Optional[dict]]) -> dict[str, dict]:
    results: dict[str, dict] = {}
    for provider_key, provider_name, *_rest in _iter_provider_specs():
        probs = provider_probabilities.get(provider_key)
        result = _check_single_source(label, market_prob, probs.get(label) if probs else None, provider_name)
        if result is not None:
            results[provider_key] = result
    return results


def _agreeing_sources(single_source_results: dict[str, dict]) -> tuple[str, str, list[dict]] | None:
    all_results = list(single_source_results.values())
    if not all_results:
        return None

    yes_votes = [result for result in all_results if result["direction"] == "YES"]
    no_votes = [result for result in all_results if result["direction"] == "NO"]

    if len(yes_votes) >= 2:
        return "confirmed", "YES", yes_votes
    if len(no_votes) >= 2:
        return "confirmed", "NO", no_votes

    for provider_key, _provider_name, confidence, *_rest in _iter_provider_specs():
        result = single_source_results.get(provider_key)
        if result is not None:
            return confidence, result["direction"], [result]
    return None


def _short_event_title(event_title: str) -> str:
    return event_title.replace(TITLE_PREFIX, "").replace("?", "")


def _temperature_parts(discrepancy: dict, *, compact: bool) -> list[str]:
    parts: list[str] = []
    for _provider_key, _provider_name, _confidence, temp_key, _prob_key, short_label in _iter_provider_specs():
        value = discrepancy.get(temp_key)
        if value is None:
            continue
        token = f"{short_label}:{value:.0f}" if compact else f"{short_label} {value:.0f}"
        parts.append(token)
    return parts


def _probability_parts(discrepancy: dict) -> list[str]:
    parts = [f"Mkt *{round(discrepancy['market_prob'] * 100)}%*"]
    for _provider_key, _provider_name, _confidence, _temp_key, prob_key, short_label in _iter_provider_specs():
        value = discrepancy.get(prob_key)
        if value is not None:
            parts.append(f"{short_label} {round(value * 100)}%")
    return parts


def _confidence_badge(discrepancy: dict) -> str:
    source_count = int(discrepancy.get("source_count", 1) or 1)
    confidence = str(discrepancy.get("confidence", "") or "")
    if source_count >= 3:
        return f"{source_count} SOURCES AGREE"
    return PROVIDER_BADGES.get(confidence, PROVIDER_BADGES["om_only"])


def _confidence_icon(confidence: str) -> str:
    return PROVIDER_ICONS.get(confidence, PROVIDER_ICONS["om_only"])


def find_discrepancies(
    event_title: str,
    city_slug: str,
    event_date,
    buckets: list,
    wu_probs: Optional[dict],
    om_probs: Optional[dict],
    wu_temp: Optional[float],
    om_temp: Optional[float],
    unit_symbol: str = "F",
    vc_probs: Optional[dict] = None,
    vc_temp: Optional[float] = None,
    noaa_probs: Optional[dict] = None,
    noaa_temp: Optional[float] = None,
    weatherapi_probs: Optional[dict] = None,
    weatherapi_temp: Optional[float] = None,
) -> List[dict]:
    """
    Compares forecast probabilities against market prices for all buckets.

    CONFIRMED = 2 or more sources agree on direction and size.
    """

    discrepancies = []

    for bucket in buckets:
        label = bucket.get("label", "")
        market_prob = bucket.get("market_yes_price")
        market_slug = bucket.get("market_slug", "")
        liquidity = bucket.get("liquidity", 0.0)

        if market_prob is None:
            continue

        provider_probabilities = {
            "wu": wu_probs,
            "om": om_probs,
            "vc": vc_probs,
            "noaa": noaa_probs,
            "weatherapi": weatherapi_probs,
        }
        single_source_results = _single_source_results(label, market_prob, provider_probabilities)
        if not single_source_results:
            continue

        agreement = _agreeing_sources(single_source_results)
        if agreement is None:
            continue
        confidence, direction, agreeing = agreement

        avg_disc = sum(result["discrepancy"] for result in agreeing) / len(agreeing)
        discrepancy_val = round(avg_disc, 3)
        edge_size = "large" if abs(avg_disc) >= LARGE_EDGE_THRESHOLD else "small"
        forecast_prob = agreeing[0]["forecast_prob"]

        discrepancies.append(
            {
                "event_title": event_title,
                "city_slug": city_slug,
                "event_date": str(event_date),
                "label": label,
                "market_prob": round(market_prob, 3),
                "forecast_prob": forecast_prob,
                "discrepancy": discrepancy_val,
                "direction": direction,
                "edge_size": edge_size,
                "confidence": confidence,
                "source_count": len(agreeing),
                "wu_temp": wu_temp,
                "om_temp": om_temp,
                "vc_temp": vc_temp,
                "noaa_temp": noaa_temp,
                "weatherapi_temp": weatherapi_temp,
                "unit": unit_symbol,
                "market_slug": market_slug,
                "liquidity": liquidity,
                "wu_prob": single_source_results.get("wu", {}).get("forecast_prob"),
                "om_prob": single_source_results.get("om", {}).get("forecast_prob"),
                "vc_prob": single_source_results.get("vc", {}).get("forecast_prob"),
                "noaa_prob": single_source_results.get("noaa", {}).get("forecast_prob"),
                "weatherapi_prob": single_source_results.get("weatherapi", {}).get("forecast_prob"),
                "event_slug": bucket.get("event_slug", ""),
            }
        )

    discrepancies.sort(key=lambda item: (-item.get("source_count", 1), -abs(item["discrepancy"])))
    return discrepancies


def format_discrepancy_message(d: dict) -> str:
    """Formats a single discrepancy into a Telegram message."""

    conf = str(d.get("confidence", "") or "")
    edge_size = d.get("edge_size", "small")
    direction = d["direction"]
    bet_emoji = "UP" if direction == "YES" else "DOWN"
    size_dot = "[L]" if edge_size == "large" else "[S]"
    badge = _confidence_badge(d)
    short = _short_event_title(d["event_title"])
    unit = d.get("unit", "F")
    temp_parts = _temperature_parts(d, compact=False)
    temp_line = ("  ".join(temp_parts) + unit) if temp_parts else ""
    diff_pct = round(d["discrepancy"] * 100)
    diff_str = f"+{diff_pct}%" if diff_pct > 0 else f"{diff_pct}%"
    prob_parts = _probability_parts(d)

    liq_str = f"${d.get('liquidity', 0):,.0f}"

    lines = [
        f"{size_dot} *{badge}*",
        f"*{short}*  `{d['label']}`",
        temp_line,
        "  ".join(prob_parts),
        f"Edge: `{diff_str}` {bet_emoji} *BET {direction}*  ${liq_str[1:]}",
    ]
    return "\n".join(line for line in lines if line)


def format_small_edge(d: dict) -> str:
    """Compact one-liner for small edges."""

    conf = str(d.get("confidence", "") or "")
    direction = d["direction"]
    bet_emoji = "UP" if direction == "YES" else "DOWN"
    pct = round(d["discrepancy"] * 100)
    sign = "+" if pct > 0 else ""
    conf_icon = _confidence_icon(conf)
    short = _short_event_title(d["event_title"])
    unit = d.get("unit", "F")
    temp_parts = _temperature_parts(d, compact=True)
    temp_str = (" ".join(temp_parts) + unit) if temp_parts else ""

    market_pct = round(d["market_prob"] * 100)
    forecast_pct = round(d["forecast_prob"] * 100)

    return (
        f"{conf_icon} *{short}* `{d['label']}`\n"
        f"   {temp_str}  {market_pct}%->{forecast_pct}% (`{sign}{pct}%`) {bet_emoji} {direction}"
    )


def summarize_discrepancies(all_discrepancies: list) -> str:
    """Summary message for Telegram - shown with filter buttons below."""

    s3p = [d for d in all_discrepancies if d.get("source_count", 1) >= 3]
    s2a = [d for d in all_discrepancies if d.get("source_count", 1) == 2]
    s1 = [d for d in all_discrepancies if d.get("source_count", 1) == 1]
    large = [d for d in all_discrepancies if d["edge_size"] == "large"]
    small = [d for d in all_discrepancies if d["edge_size"] == "small"]

    return (
        f"*Weather Edge Summary*\n"
        f"Markets flagged: *{len(all_discrepancies)}*\n"
        f"Confirmed 3+ source edges: *{len(s3p)}*\n"
        f"Confirmed 2-source edges: *{len(s2a)}*\n"
        f"Single-source edges: *{len(s1)}*\n"
        f"Large edges: *{len(large)}* | Small edges: *{len(small)}*"
    )
