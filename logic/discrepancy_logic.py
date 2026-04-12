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
    vc_probs:       Optional[dict] = None,
    vc_temp:        Optional[float] = None,
) -> List[dict]:
    """
    Compares WU, Open-Meteo, and Visual Crossing forecasts against
    market prices for all buckets in one event.

    CONFIRMED = 2 or more sources agree on direction AND size.
    This means OM + VC can confirm each other even when WU is N/A.

    Returns list of discrepancy dicts sorted by confidence then size.
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
        vc_result = _check_single_source(
            label, market_prob,
            vc_probs.get(label) if vc_probs else None,
            "Visual Crossing"
        )

        # Collect all sources that flagged this bucket
        all_results = [r for r in [wu_result, om_result, vc_result] if r is not None]

        if not all_results:
            continue

        # Count votes for each direction
        yes_votes = [r for r in all_results if r["direction"] == "YES"]
        no_votes  = [r for r in all_results if r["direction"] == "NO"]

        if len(yes_votes) >= 2:
            # 2+ sources say BET YES — CONFIRMED
            confidence  = "confirmed"
            direction   = "YES"
            agreeing    = yes_votes
        elif len(no_votes) >= 2:
            # 2+ sources say BET NO — CONFIRMED
            confidence  = "confirmed"
            direction   = "NO"
            agreeing    = no_votes
        elif wu_result:
            confidence  = "wu_only"
            direction   = wu_result["direction"]
            agreeing    = [wu_result]
        elif om_result:
            confidence  = "om_only"
            direction   = om_result["direction"]
            agreeing    = [om_result]
        elif vc_result:
            confidence  = "vc_only"
            direction   = vc_result["direction"]
            agreeing    = [vc_result]
        else:
            continue

        avg_disc        = sum(r["discrepancy"] for r in agreeing) / len(agreeing)
        discrepancy_val = round(avg_disc, 3)
        edge_size       = "large" if abs(avg_disc) >= LARGE_EDGE_THRESHOLD else "small"
        forecast_prob   = agreeing[0]["forecast_prob"]

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
            "confidence":     confidence,   # "confirmed", "wu_only", "om_only", "vc_only"
            "wu_temp":        wu_temp,
            "om_temp":        om_temp,
            "vc_temp":        vc_temp,
            "unit":           unit_symbol,
            "market_slug":    market_slug,
            "liquidity":      liquidity,
            "wu_prob":        wu_result["forecast_prob"] if wu_result else None,
            "om_prob":        om_result["forecast_prob"] if om_result else None,
            "vc_prob":        vc_result["forecast_prob"] if vc_result else None,
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
    elif conf == "vc_only":
        conf_label = "🌍 *VC only* —"
    else:
        conf_label = "🌤️ *OM only* —"

    size_emoji = "🔴" if edge_size == "large" else "🟡"

    short = (
        d["event_title"]
        .replace("Highest temperature in ", "")
        .replace("?", "")
    )

    unit    = d.get("unit", "°F")
    wu_temp = d.get("wu_temp")
    om_temp = d.get("om_temp")
    vc_temp = d.get("vc_temp")
    wu_prob = d.get("wu_prob")
    om_prob = d.get("om_prob")
    vc_prob = d.get("vc_prob")

    temp_parts = []
    if wu_temp is not None:
        temp_parts.append(f"🌡️ WU: `{wu_temp:.1f}{unit}`")
    if om_temp is not None:
        temp_parts.append(f"🌤️ OM: `{om_temp:.1f}{unit}`")
    if vc_temp is not None:
        temp_parts.append(f"🌍 VC: `{vc_temp:.1f}{unit}`")

    market_pct = round(d["market_prob"] * 100)
    diff_pct   = round(d["discrepancy"] * 100)
    diff_str   = f"+{diff_pct}%" if diff_pct > 0 else f"{diff_pct}%"

    liq_str = f"${d.get('liquidity', 0):,.0f}"
    slug = d.get("event_slug") or d.get("market_slug", "")
    url  = f"https://polymarket.com/event/{slug}" if slug else ""

    prob_parts = [f"Market: {market_pct}%"]
    if wu_prob is not None:
        prob_parts.append(f"WU: {round(wu_prob*100)}%")
    if om_prob is not None:
        prob_parts.append(f"OM: {round(om_prob*100)}%")
    if vc_prob is not None:
        prob_parts.append(f"VC: {round(vc_prob*100)}%")

    lines = [
        f"{size_emoji} {conf_label} *{short}*",
        f"Range: `{d['label']}`",
        " | ".join(temp_parts) if temp_parts else "",
        " | ".join(prob_parts),
    ]
    lines = [l for l in lines if l]  # remove empty lines

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

    conf_icon = "✅" if conf == "confirmed" else ("🌡️" if conf == "wu_only" else ("🌍" if conf == "vc_only" else "🌤️"))

    short = (
        d["event_title"]
        .replace("Highest temperature in ", "")
        .replace("?", "")
    )

    unit    = d.get("unit", "°F")
    wu_temp = d.get("wu_temp")
    om_temp = d.get("om_temp")
    vc_temp = d.get("vc_temp")

    temp_parts = []
    if wu_temp is not None:
        temp_parts.append(f"WU:{wu_temp:.0f}")
    if om_temp is not None:
        temp_parts.append(f"OM:{om_temp:.0f}")
    if vc_temp is not None:
        temp_parts.append(f"VC:{vc_temp:.0f}")
    temp_str = (" ".join(temp_parts) + unit) if temp_parts else ""

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
    vc_only   = [d for d in all_discrepancies if d["confidence"] == "vc_only"]
    large     = [d for d in all_discrepancies if d["edge_size"] == "large"]
    small     = [d for d in all_discrepancies if d["edge_size"] == "small"]

    lines = ["🌡️ *Discrepancy Scan Complete*\n"]
    lines.append(f"✅ Confirmed (2+ sources agree): {len(confirmed)}")
    lines.append(f"🌡️ WU only: {len(wu_only)}")
    lines.append(f"🌤️ Open-Meteo only: {len(om_only)}")
    lines.append(f"🌍 Visual Crossing only: {len(vc_only)}")
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