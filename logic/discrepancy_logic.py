# =============================================================
# logic/discrepancy_logic.py
#
# PURPOSE:
#   Compares forecast probabilities from two sources (WU and
#   Open-Meteo) to Polymarket market prices. Identifies edges
#   and classifies them by confidence:
#
#   🔴 LARGE edge    — one source shows 20%+ discrepancy
#   ✅ CONFIRMED     — BOTH sources agree on direction and size
#   🟡 SMALL edge    — one source shows 10-20% discrepancy
#
#   Confirmed edges (both sources agree) are the strongest signal
#   since they rule out location/model differences.
# =============================================================

from typing import Optional, List

SMALL_EDGE_THRESHOLD     = 0.10   # 10%
LARGE_EDGE_THRESHOLD     = 0.20   # 20%


def _check_single_source(
    label:         str,
    market_prob:   float,
    forecast_prob: float,
    source:        str,
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
        "label":         label,
        "source":        source,
        "market_prob":   round(market_prob, 3),
        "forecast_prob": round(forecast_prob, 3),
        "discrepancy":   round(diff, 3),
        "direction":     "YES" if diff > 0 else "NO",
        "edge_size":     "large" if abs(diff) >= LARGE_EDGE_THRESHOLD else "small",
    }


def find_discrepancies(
    event_title:    str,
    city_slug:      str,
    event_date,
    buckets:        list,
    wu_probs:       Optional[dict],
    om_probs:       Optional[dict],
    wu_temp:        Optional[float],
    om_temp:        Optional[float],
    unit_symbol:    str = "°F",
) -> List[dict]:
    """
    Compares both WU and Open-Meteo forecasts against market prices
    for all buckets in one event. Returns a list of discrepancy dicts.

    A discrepancy is "confirmed" if both sources:
        - Both show a discrepancy above the small threshold
        - Both point in the same direction (both say BET YES, or both say BET NO)

    Args:
        event_title:  e.g. "Highest temperature in NYC on April 13?"
        city_slug:    e.g. "nyc"
        event_date:   date object
        buckets:      list of bucket dicts with market_yes_price
        wu_probs:     dict label→prob from WU, or None
        om_probs:     dict label→prob from Open-Meteo, or None
        wu_temp:      raw WU forecast temp (for display)
        om_temp:      raw Open-Meteo forecast temp (for display)
        unit_symbol:  "°F" or "°C"

    Returns:
        list of discrepancy dicts, sorted by confidence then size
    """

    discrepancies = []

    for bucket in buckets:
        label       = bucket.get("label", "")
        market_prob = bucket.get("market_yes_price")
        market_slug = bucket.get("market_slug", "")
        liquidity   = bucket.get("liquidity", 0.0)

        if market_prob is None:
            continue

        wu_result = _check_single_source(
            label, market_prob,
            wu_probs.get(label) if wu_probs else None,
            "WU"
        )
        om_result = _check_single_source(
            label, market_prob,
            om_probs.get(label) if om_probs else None,
            "Open-Meteo"
        )

        # Determine confidence level
        if wu_result and om_result:
            # Both sources flagged this bucket — check if they agree on direction
            if wu_result["direction"] == om_result["direction"]:
                # ✅ CONFIRMED — both sources agree, strongest signal
                confidence = "confirmed"
                # Use average discrepancy for confirmed edges
                avg_disc = (wu_result["discrepancy"] + om_result["discrepancy"]) / 2
                direction = wu_result["direction"]
                edge_size = "large" if abs(avg_disc) >= LARGE_EDGE_THRESHOLD else "small"
                discrepancy_val = round(avg_disc, 3)
                # Use WU as primary forecast (more accurate station), OM as backup
                forecast_prob = wu_result["forecast_prob"]
            else:
                # Sources disagree on direction — lower confidence, skip
                continue
        elif wu_result:
            confidence      = "wu_only"
            discrepancy_val = wu_result["discrepancy"]
            direction       = wu_result["direction"]
            edge_size       = wu_result["edge_size"]
            forecast_prob   = wu_result["forecast_prob"]
        elif om_result:
            confidence      = "om_only"
            discrepancy_val = om_result["discrepancy"]
            direction       = om_result["direction"]
            edge_size       = om_result["edge_size"]
            forecast_prob   = om_result["forecast_prob"]
        else:
            continue  # no discrepancy from either source

        discrepancies.append({
            "event_title":    event_title,
            "city_slug":      city_slug,
            "event_date":     str(event_date),
            "label":          label,
            "market_prob":    round(market_prob, 3),
            "forecast_prob":  forecast_prob,
            "discrepancy":    discrepancy_val,
            "direction":      direction,
            "edge_size":      edge_size,
            "confidence":     confidence,   # "confirmed", "wu_only", "om_only"
            "wu_temp":        wu_temp,
            "om_temp":        om_temp,
            "unit":           unit_symbol,
            "market_slug":    market_slug,
            "liquidity":      liquidity,
            # Individual source forecasts for display
            "wu_prob":        wu_result["forecast_prob"] if wu_result else None,
            "om_prob":        om_result["forecast_prob"] if om_result else None,
            # Event slug for Polymarket URL (parent event, not child market)
            "event_slug":     bucket.get("event_slug", ""),
        })

    # Sort: confirmed first, then by abs(discrepancy) descending
    def sort_key(d):
        conf_order = {"confirmed": 0, "wu_only": 1, "om_only": 2}
        return (conf_order.get(d["confidence"], 3), -abs(d["discrepancy"]))

    discrepancies.sort(key=sort_key)
    return discrepancies


def format_discrepancy_message(d: dict) -> str:
    """
    Formats a single discrepancy into a detailed Telegram message.

    Example:
        ✅ CONFIRMED — NYC on April 13
        Range: `72-73°F`
        🌡️ WU (KLGA): 73.2°F | 🌤️ OM: 72.8°F
        Market: 6% | WU: 30% | OM: 28%
        Avg discrepancy: +23% → 📈 BET YES
        💧 Liquidity: $4,521
    """

    conf      = d.get("confidence", "")
    edge_size = d.get("edge_size", "small")
    direction = d["direction"]
    bet_emoji = "📈" if direction == "YES" else "📉"

    # Confidence label
    if conf == "confirmed":
        conf_label = "✅ *CONFIRMED* —"
    elif conf == "wu_only":
        conf_label = "🌡️ *WU only* —"
    else:
        conf_label = "🌤️ *OM only* —"

    size_emoji = "🔴" if edge_size == "large" else "🟡"

    short = (
        d["event_title"]
        .replace("Highest temperature in ", "")
        .replace("?", "")
    )

    unit      = d.get("unit", "°F")
    wu_temp   = d.get("wu_temp")
    om_temp   = d.get("om_temp")
    wu_prob   = d.get("wu_prob")
    om_prob   = d.get("om_prob")

    wu_temp_str = f"{wu_temp:.1f}{unit}" if wu_temp is not None else "N/A"
    om_temp_str = f"{om_temp:.1f}{unit}" if om_temp is not None else "N/A"

    market_pct = round(d["market_prob"] * 100)
    diff_pct   = round(d["discrepancy"] * 100)
    diff_str   = f"+{diff_pct}%" if diff_pct > 0 else f"{diff_pct}%"

    liq_str = f"${d.get('liquidity', 0):,.0f}"
    # Use event slug for the URL — market slugs go to 404
    # Event slug format: highest-temperature-in-nyc-on-april-13-2026
    slug = d.get("event_slug") or d.get("market_slug", "")
    url  = f"https://polymarket.com/event/{slug}" if slug else ""

    lines = [
        f"{size_emoji} {conf_label} *{short}*",
        f"Range: `{d['label']}`",
        f"🌡️ WU: `{wu_temp_str}` | 🌤️ OM: `{om_temp_str}`",
    ]

    # Show per-source probabilities if both available
    if wu_prob is not None and om_prob is not None:
        lines.append(
            f"Market: {market_pct}% | "
            f"WU: {round(wu_prob*100)}% | "
            f"OM: {round(om_prob*100)}%"
        )
    else:
        lines.append(
            f"Market: {market_pct}% → Forecast: {round(d['forecast_prob']*100)}%"
        )

    lines.append(f"Avg edge: `{diff_str}` {bet_emoji} *BET {direction}*")
    lines.append(f"💧 Liquidity: {liq_str}")
    if url:
        lines.append(url)

    return "\n".join(lines)


def format_small_edge(d: dict) -> str:
    """Compact one-liner for small edges."""

    conf      = d.get("confidence", "")
    direction = d["direction"]
    bet_emoji = "📈" if direction == "YES" else "📉"
    pct       = round(d["discrepancy"] * 100)
    sign      = "+" if pct > 0 else ""

    conf_icon = "✅" if conf == "confirmed" else ("🌡️" if conf == "wu_only" else "🌤️")

    short = (
        d["event_title"]
        .replace("Highest temperature in ", "")
        .replace("?", "")
    )

    unit    = d.get("unit", "°F")
    wu_temp = d.get("wu_temp")
    om_temp = d.get("om_temp")

    if wu_temp is not None and om_temp is not None:
        temp_str = f"WU:{wu_temp:.0f} OM:{om_temp:.0f}{unit}"
    elif wu_temp is not None:
        temp_str = f"WU:{wu_temp:.0f}{unit}"
    elif om_temp is not None:
        temp_str = f"OM:{om_temp:.0f}{unit}"
    else:
        temp_str = ""

    market_pct   = round(d["market_prob"] * 100)
    forecast_pct = round(d["forecast_prob"] * 100)

    return (
        f"{conf_icon} *{short}* `{d['label']}`\n"
        f"   {temp_str}  "
        f"{market_pct}%→{forecast_pct}% (`{sign}{pct}%`) "
        f"{bet_emoji} {direction}"
    )


def summarize_discrepancies(all_discrepancies: list) -> str:
    """Summary message for Telegram."""

    confirmed = [d for d in all_discrepancies if d["confidence"] == "confirmed"]
    wu_only   = [d for d in all_discrepancies if d["confidence"] == "wu_only"]
    om_only   = [d for d in all_discrepancies if d["confidence"] == "om_only"]
    large     = [d for d in all_discrepancies if d["edge_size"] == "large"]
    small     = [d for d in all_discrepancies if d["edge_size"] == "small"]

    lines = ["🌡️ *Discrepancy Scan Complete*\n"]
    lines.append(f"✅ Confirmed (both sources agree): {len(confirmed)}")
    lines.append(f"🌡️ WU only: {len(wu_only)}")
    lines.append(f"🌤️ Open-Meteo only: {len(om_only)}")
    lines.append(f"🔴 Large edges (20%+): {len(large)}")
    lines.append(f"🟡 Small edges (10-20%): {len(small)}")
    lines.append(f"📊 Total flagged: {len(all_discrepancies)}\n")

    if confirmed:
        lines.append("*Top confirmed edges:*")
        for d in confirmed[:5]:
            pct  = round(d["discrepancy"] * 100)
            sign = "+" if pct > 0 else ""
            unit = d.get("unit", "°F")
            wu   = d.get("wu_temp")
            om   = d.get("om_temp")
            temp_str = f"WU:{wu:.0f} OM:{om:.0f}{unit}" if wu and om else ""
            short = d["event_title"].replace("Highest temperature in ", "").replace("?", "")
            lines.append(
                f"  • *{short}* `{d['label']}` {temp_str} "
                f"`{sign}{pct}%` → BET {d['direction']}"
            )

    return "\n".join(lines)