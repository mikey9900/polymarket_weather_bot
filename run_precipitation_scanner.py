# =============================================================
# run_precipitation_scanner.py
#
# PURPOSE:
#   Full pipeline for monthly precipitation market scanning.
#   Mirrors run_scanner.py but for precipitation events.
#
# DATA FLOW:
#   1. Fetch active precipitation events → Gamma API
#   2. Parse bucket labels → precip_parser.py
#   3. Get observed + forecast precip → precip_forecast.py
#   4. Calculate bucket probabilities
#   5. Compare to market prices → discrepancy_logic.py
#   6. Send alerts → Telegram
# =============================================================

from alerts.telegram_alerts import send_telegram_alert, send_with_keyboard
from alerts.scan_cache import store_scan
from logic.discrepancy_logic import find_discrepancies
from precipitation.precip_scanner import fetch_precip_events
from precipitation.precip_parser import parse_precip_buckets_for_event
from precipitation.precip_forecast import (
    get_om_monthly_precip,
    get_vc_monthly_precip,
    calc_precip_bucket_probs,
)


# =============================================================
# MESSAGE FORMATTER
# =============================================================

def format_precipitation_message(d: dict) -> str:
    """
    Formats a single precipitation discrepancy as a Telegram message.
    Replaces format_discrepancy_message for precip-specific display.
    """
    conf      = d.get("confidence", "")
    src_count = d.get("source_count", 1)
    edge_size = d.get("edge_size", "small")
    direction = d["direction"]
    bet_emoji = "📈" if direction == "YES" else "📉"
    size_dot  = "🔴" if edge_size == "large" else "🟡"

    if src_count >= 2:
        badge = "✅ *2 SOURCES AGREE*"
    elif conf == "om_only":
        badge = "🌤️ *OM only*"
    elif conf == "vc_only":
        badge = "🌍 *VC only*"
    else:
        badge = "📊 *Single source*"

    # Short title: "NYC April 2026 Precipitation"
    short = (
        d.get("event_title", "")
        .replace("?", "")
        .strip()
    )

    unit     = d.get("unit", "in")
    unit_sym = '"' if unit == "in" else "mm"

    # Observed / forecast amounts (stored in wu_temp / om_temp / vc_temp slots)
    om_total = d.get("om_temp")
    vc_total = d.get("vc_temp")
    observed = d.get("wu_temp")   # reused field — stores observed_so_far

    total_parts = []
    if om_total is not None:
        total_parts.append(f"🌤️ OM: {om_total:.2f}{unit_sym}")
    if vc_total is not None:
        total_parts.append(f"🌍 VC: {vc_total:.2f}{unit_sym}")
    total_line = "  ".join(total_parts)

    obs_line = f"💧 Observed so far: {observed:.2f}{unit_sym}" if observed is not None else ""

    market_pct = round(d["market_prob"] * 100)
    diff_pct   = round(d["discrepancy"] * 100)
    diff_str   = f"+{diff_pct}%" if diff_pct > 0 else f"{diff_pct}%"

    prob_parts = [f"Mkt *{market_pct}%*"]
    if d.get("om_prob")   is not None: prob_parts.append(f"OM {round(d['om_prob']*100)}%")
    if d.get("vc_prob")   is not None: prob_parts.append(f"VC {round(d['vc_prob']*100)}%")

    liq_str = f"${d.get('liquidity', 0):,.0f}"

    lines = [
        f"{size_dot} {badge}",
        f"*{short}*  `{d['label']}`",
        obs_line,
        total_line,
        "  ".join(prob_parts),
        f"Edge: `{diff_str}` {bet_emoji} *BET {direction}*  💧 {liq_str}",
    ]
    return "\n".join(l for l in lines if l)


def _precip_event_url(d: dict) -> str:
    slug = d.get("event_slug", "")
    return f"https://polymarket.com/event/{slug}" if slug else ""


# =============================================================
# MAIN SCANNER
# =============================================================

def run_precipitation_scan() -> list[dict]:
    """
    Full precipitation scan pipeline.
    Returns list of dicts: {"text": str, "url": str | None}
    Same format as run_portfolio_check — callers send each as a message.
    """
    print("💧 Starting monthly precipitation scan...")

    bundles = fetch_precip_events()
    if not bundles:
        return [{"text": "💧 No active precipitation markets found.", "url": None}]

    all_discrepancies = []
    events_ok         = 0
    events_flagged    = 0
    events_skipped    = 0

    for bundle in bundles:
        event     = bundle["event"]
        markets   = bundle["markets"]
        year      = bundle["year"]
        month     = bundle["month"]
        city_slug = bundle["city_slug"]
        title     = event.get("title", "Unknown")
        event_slug = event.get("slug", "")

        print(f"📅 {title}")

        buckets = parse_precip_buckets_for_event(markets)
        if not buckets:
            print(f"   ⚠️  No active liquid buckets — skipping\n")
            events_skipped += 1
            continue

        # Stamp event slug onto every bucket
        for b in buckets:
            b["event_slug"] = event_slug

        # ── Fetch forecasts ───────────────────────────────────
        print(f"   🌤️  Fetching Open-Meteo precip for {city_slug} {year}-{month:02d}...")
        om_data = get_om_monthly_precip(city_slug, year, month)

        print(f"   🌍  Fetching Visual Crossing precip for {city_slug} {year}-{month:02d}...")
        vc_data = get_vc_monthly_precip(city_slug, year, month)

        if not om_data and not vc_data:
            print(f"   ⚠️  All forecasts failed — skipping\n")
            events_skipped += 1
            continue

        unit = (om_data or vc_data)["unit"]
        unit_sym = '"' if unit == "in" else "mm"

        # Log what we got
        if om_data:
            print(f"   🌤️  OM: obs={om_data['observed']:.2f}{unit_sym}  "
                  f"fcast={om_data['forecast']:.2f}{unit_sym}  "
                  f"total={om_data['total_projected']:.2f}{unit_sym}")
        if vc_data:
            print(f"   🌍  VC: obs={vc_data['observed']:.2f}{unit_sym}  "
                  f"fcast={vc_data['forecast']:.2f}{unit_sym}  "
                  f"total={vc_data['total_projected']:.2f}{unit_sym}")

        # ── Calculate bucket probabilities ────────────────────
        # Use OM observed as the canonical observed amount (both sources
        # should agree on past data; OM archive is generally reliable)
        observed = (om_data or vc_data)["observed"]

        om_probs = None
        if om_data:
            om_probs = calc_precip_bucket_probs(
                observed, om_data["forecast"], buckets, unit
            )

        vc_probs = None
        if vc_data:
            vc_probs = calc_precip_bucket_probs(
                observed, vc_data["forecast"], buckets, unit
            )

        if not om_probs and not vc_probs:
            print(f"   ⚠️  Probability calculation failed — skipping\n")
            events_skipped += 1
            continue

        # Print comparison table
        print(f"   {'Bucket':<20} {'Mkt':>6} {'OM':>6} {'VC':>6} {'OMΔ':>7} {'VCΔ':>7}  {'Liq':>7}")
        print(f"   {'-'*68}")

        for bucket in buckets:
            label   = bucket["label"]
            mkt     = bucket.get("market_yes_price")
            om_p    = om_probs.get(label)  if om_probs else None
            vc_p    = vc_probs.get(label)  if vc_probs else None
            liq     = bucket.get("liquidity", 0)

            if mkt is None:
                continue

            def fmt(v):
                return f"{v*100:.0f}%" if v is not None else "  N/A"

            def fmt_d(v):
                if v is None: return "  N/A"
                return f"+{v*100:.0f}%" if v >= 0 else f"{v*100:.0f}%"

            om_diff = (om_p - mkt) if om_p is not None else None
            vc_diff = (vc_p - mkt) if vc_p is not None else None

            print(f"   {label:<20} {fmt(mkt):>6} {fmt(om_p):>6} {fmt(vc_p):>6} "
                  f"{fmt_d(om_diff):>7} {fmt_d(vc_diff):>7}  ${liq:>5,.0f}")

        # ── Find discrepancies ────────────────────────────────
        # Reuse temperature discrepancy logic — pass precip totals
        # in the wu/om/vc temp slots for display purposes.
        event_discreps = find_discrepancies(
            event_title  = title,
            city_slug    = city_slug,
            event_date   = f"{year}-{month:02d}-01",  # month start as event date
            buckets      = buckets,
            wu_probs     = None,
            om_probs     = om_probs,
            wu_temp      = observed,                    # observed so far (display)
            om_temp      = om_data["total_projected"] if om_data else None,
            unit_symbol  = unit_sym,
            vc_probs     = vc_probs,
            vc_temp      = vc_data["total_projected"] if vc_data else None,
            noaa_probs   = None,
            noaa_temp    = None,
        )

        if event_discreps:
            events_flagged += 1
            all_discrepancies.extend(event_discreps)
            print(f"   ⚡ {len(event_discreps)} edge(s) found\n")
        else:
            events_ok += 1
            print(f"   ✅ No significant discrepancies\n")

    # ── Summary + alerts ─────────────────────────────────────
    print(f"{'='*52}")
    print(f"Processed: {events_ok + events_flagged} | Flagged: {events_flagged} | Skipped: {events_skipped}")
    print(f"Total edges: {len(all_discrepancies)}")

    if not all_discrepancies:
        return [{
            "text": (
                f"💧 *Precipitation Scan Complete*\n\n"
                f"📅 Events scanned: {events_ok + events_flagged + events_skipped}\n"
                f"✅ No significant discrepancies found."
            ),
            "url": None,
        }]

    # Cache for filter buttons
    scan_id = store_scan(all_discrepancies)

    s2a = sum(1 for d in all_discrepancies if d.get("source_count", 1) >= 2 and d["edge_size"] == "large")
    s1  = sum(1 for d in all_discrepancies if d.get("source_count", 1) == 1 and d["edge_size"] == "large")

    large = [d for d in all_discrepancies if d["edge_size"] == "large"]
    small = [d for d in all_discrepancies if d["edge_size"] == "small"]

    summary_lines = [
        "💧 *Precipitation Scan Complete*",
        "─────────────────────────",
        f"✅ 2 sources agree:    *{s2a}*",
        f"🟡 1 source only:      *{s1}*",
        "─────────────────────────",
        f"🔴 Large edges 20%+:   *{len(large)}*",
        f"🟡 Small edges 10-20%: *{len(small)}*",
        f"📊 Total flagged:      *{len(all_discrepancies)}*",
    ]
    summary_text = "\n".join(summary_lines)

    results = [{"text": summary_text, "url": None, "scan_id": scan_id, "edge_counts": (s2a, s1)}]

    for d in all_discrepancies:
        if d["edge_size"] != "large":
            continue
        text = format_precipitation_message(d)
        url  = _precip_event_url(d)
        results.append({"text": text, "url": url})

    return results
