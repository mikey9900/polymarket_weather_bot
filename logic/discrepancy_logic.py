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

from typing import List, Optional

SMALL_EDGE_THRESHOLD = 0.10
LARGE_EDGE_THRESHOLD = 0.20


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

        wu_result = _check_single_source(label, market_prob, wu_probs.get(label) if wu_probs else None, "WU")
        om_result = _check_single_source(label, market_prob, om_probs.get(label) if om_probs else None, "Open-Meteo")
        vc_result = _check_single_source(
            label, market_prob, vc_probs.get(label) if vc_probs else None, "Visual Crossing"
        )
        noaa_result = _check_single_source(label, market_prob, noaa_probs.get(label) if noaa_probs else None, "NOAA")
        weatherapi_result = _check_single_source(
            label,
            market_prob,
            weatherapi_probs.get(label) if weatherapi_probs else None,
            "WeatherAPI",
        )

        all_results = [
            result
            for result in (wu_result, om_result, vc_result, noaa_result, weatherapi_result)
            if result is not None
        ]
        if not all_results:
            continue

        yes_votes = [result for result in all_results if result["direction"] == "YES"]
        no_votes = [result for result in all_results if result["direction"] == "NO"]

        if len(yes_votes) >= 2:
            confidence = "confirmed"
            direction = "YES"
            agreeing = yes_votes
        elif len(no_votes) >= 2:
            confidence = "confirmed"
            direction = "NO"
            agreeing = no_votes
        elif wu_result:
            confidence = "wu_only"
            direction = wu_result["direction"]
            agreeing = [wu_result]
        elif om_result:
            confidence = "om_only"
            direction = om_result["direction"]
            agreeing = [om_result]
        elif vc_result:
            confidence = "vc_only"
            direction = vc_result["direction"]
            agreeing = [vc_result]
        elif noaa_result:
            confidence = "noaa_only"
            direction = noaa_result["direction"]
            agreeing = [noaa_result]
        elif weatherapi_result:
            confidence = "weatherapi_only"
            direction = weatherapi_result["direction"]
            agreeing = [weatherapi_result]
        else:
            continue

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
                "wu_prob": wu_result["forecast_prob"] if wu_result else None,
                "om_prob": om_result["forecast_prob"] if om_result else None,
                "vc_prob": vc_result["forecast_prob"] if vc_result else None,
                "noaa_prob": noaa_result["forecast_prob"] if noaa_result else None,
                "weatherapi_prob": weatherapi_result["forecast_prob"] if weatherapi_result else None,
                "event_slug": bucket.get("event_slug", ""),
            }
        )

    discrepancies.sort(key=lambda item: (-item.get("source_count", 1), -abs(item["discrepancy"])))
    return discrepancies


def format_discrepancy_message(d: dict) -> str:
    """Formats a single discrepancy into a Telegram message."""

    conf = d.get("confidence", "")
    src_count = d.get("source_count", 1)
    edge_size = d.get("edge_size", "small")
    direction = d["direction"]
    bet_emoji = "UP" if direction == "YES" else "DOWN"
    size_dot = "[L]" if edge_size == "large" else "[S]"

    if src_count >= 3:
        badge = f"{src_count} SOURCES AGREE"
    elif conf == "confirmed":
        badge = "2 SOURCES AGREE"
    elif conf == "wu_only":
        badge = "WU only"
    elif conf == "vc_only":
        badge = "VC only"
    elif conf == "noaa_only":
        badge = "NOAA only"
    elif conf == "weatherapi_only":
        badge = "WeatherAPI only"
    else:
        badge = "OM only"

    short = d["event_title"].replace("Highest temperature in ", "").replace("?", "")

    unit = d.get("unit", "F")
    temp_parts = []
    for label, key in (
        ("WU", "wu_temp"),
        ("OM", "om_temp"),
        ("VC", "vc_temp"),
        ("NOAA", "noaa_temp"),
        ("WA", "weatherapi_temp"),
    ):
        value = d.get(key)
        if value is not None:
            temp_parts.append(f"{label} {value:.0f}")
    temp_line = ("  ".join(temp_parts) + unit) if temp_parts else ""

    market_pct = round(d["market_prob"] * 100)
    diff_pct = round(d["discrepancy"] * 100)
    diff_str = f"+{diff_pct}%" if diff_pct > 0 else f"{diff_pct}%"

    prob_parts = [f"Mkt *{market_pct}%*"]
    for label, key in (
        ("WU", "wu_prob"),
        ("OM", "om_prob"),
        ("VC", "vc_prob"),
        ("NOAA", "noaa_prob"),
        ("WA", "weatherapi_prob"),
    ):
        value = d.get(key)
        if value is not None:
            prob_parts.append(f"{label} {round(value * 100)}%")

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

    conf = d.get("confidence", "")
    direction = d["direction"]
    bet_emoji = "UP" if direction == "YES" else "DOWN"
    pct = round(d["discrepancy"] * 100)
    sign = "+" if pct > 0 else ""

    if conf == "confirmed":
        conf_icon = "[2x]"
    elif conf == "wu_only":
        conf_icon = "[WU]"
    elif conf == "vc_only":
        conf_icon = "[VC]"
    elif conf == "weatherapi_only":
        conf_icon = "[WA]"
    elif conf == "noaa_only":
        conf_icon = "[NOAA]"
    else:
        conf_icon = "[OM]"

    short = d["event_title"].replace("Highest temperature in ", "").replace("?", "")

    unit = d.get("unit", "F")
    temp_parts = []
    for label, key in (
        ("WU", "wu_temp"),
        ("OM", "om_temp"),
        ("VC", "vc_temp"),
        ("NOAA", "noaa_temp"),
        ("WA", "weatherapi_temp"),
    ):
        value = d.get(key)
        if value is not None:
            temp_parts.append(f"{label}:{value:.0f}")
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
