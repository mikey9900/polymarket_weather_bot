# =============================================================
# run_scanner.py
#
# PURPOSE:
#   Full pipeline using BOTH Weather Underground and Open-Meteo.
#
# CONFIDENCE LEVELS:
#   ✅ CONFIRMED  — both WU and Open-Meteo agree on direction
#   🌡️ WU only   — only WU shows a discrepancy
#   🌤️ OM only   — only Open-Meteo shows a discrepancy
#
# TELEGRAM STRATEGY:
#   1. Summary message always sent
#   2. Confirmed large edges — one message each (highest priority)
#   3. Confirmed small edges — batched
#   4. Single-source edges — batched separately (lower confidence)
# =============================================================

from datetime import date

from scanner.weather_event_scanner import fetch_weather_events
from parser.weather_parser import parse_temperature_buckets_for_event
from forecast.forecast_engine import get_both_bucket_probabilities, CITY_COORDS
from logic.discrepancy_logic import (
    find_discrepancies,
    format_discrepancy_message,
    summarize_discrepancies,
)
from alerts.telegram_alerts import send_telegram_alert, send_with_keyboard
from alerts.scan_cache import store_scan
from tracking.scan_tracker import log_edge, check_resolutions


def run_weather_scan(limit: int = 300):
    """
    Runs the full dual-source temperature market scan.
    """

    print("🔍 Starting dual-source temperature + discrepancy scan...")

    # Check if any previously tracked edges have resolved
    check_resolutions()

    # ----------------------------------------------------------
    # STEP 1: Fetch temperature events
    # ----------------------------------------------------------
    event_bundles = fetch_weather_events(limit=limit)
    total_events  = len(event_bundles)
    print(f"📦 {total_events} events found. Running forecasts...\n")

    all_discrepancies = []
    events_ok      = 0
    events_flagged = 0
    events_skipped = 0

    # ----------------------------------------------------------
    # STEP 2-4: Process each event
    # ----------------------------------------------------------
    for bundle in event_bundles:
        event        = bundle["event"]
        markets      = bundle["markets"]
        event_title  = event.get("title", "Unknown")
        end_date_str = (event.get("endDate") or "")[:10]
        city_slug    = event.get("seriesSlug", "").replace("-daily-weather", "")

        print(f"📅 {event_title}")

        try:
            event_date = date.fromisoformat(end_date_str)
        except Exception:
            print(f"   ⚠️  Bad date — skipping\n")
            events_skipped += 1
            continue

        buckets = parse_temperature_buckets_for_event(markets)
        if not buckets:
            print(f"   ⚠️  No active liquid buckets — skipping\n")
            events_skipped += 1
            continue

        # Stamp the parent event slug onto every bucket so we can
        # build correct Polymarket URLs in alerts
        # e.g. "highest-temperature-in-nyc-on-april-13-2026"
        event_slug = event.get("slug", "")
        for bucket in buckets:
            bucket["event_slug"] = event_slug

        # Fetch from all three sources
        forecast_data = get_both_bucket_probabilities(city_slug, event_date, buckets)
        wu_probs   = forecast_data.get("wu")
        om_probs   = forecast_data.get("openmeteo")
        vc_probs   = forecast_data.get("vc")
        noaa_probs = forecast_data.get("noaa")
        wu_temp    = forecast_data.get("wu_temp")
        om_temp    = forecast_data.get("om_temp")
        vc_temp    = forecast_data.get("vc_temp")
        noaa_temp  = forecast_data.get("noaa_temp")
        unit_sym   = forecast_data.get("unit", "°F")

        if wu_probs is None and om_probs is None and vc_probs is None and noaa_probs is None:
            print(f"   ⚠️  All forecasts failed — skipping\n")
            events_skipped += 1
            continue

        # Determine unit for city
        coords   = CITY_COORDS.get(city_slug, {})
        unit_name = coords.get("unit", "fahrenheit")

        # Print comparison table
        has_noaa = noaa_probs is not None
        if has_noaa:
            print(f"   {'Bucket':<22} {'Mkt':>5} {'WU':>5} {'OM':>5} {'VC':>5} {'NOAA':>5} {'WUΔ':>6} {'OMΔ':>6} {'VCΔ':>6} {'NAΔ':>6}  {'Liq':>7}")
            print(f"   {'-'*88}")
        else:
            print(f"   {'Bucket':<22} {'Mkt':>6} {'WU':>6} {'OM':>6} {'VC':>6} {'WUΔ':>7} {'OMΔ':>7} {'VCΔ':>7}  {'Liq':>7}")
            print(f"   {'-'*80}")

        for bucket in buckets:
            label     = bucket.get("label", "")
            mkt       = bucket.get("market_yes_price")
            wu_p      = wu_probs.get(label)   if wu_probs   else None
            om_p      = om_probs.get(label)   if om_probs   else None
            vc_p      = vc_probs.get(label)   if vc_probs   else None
            noaa_p    = noaa_probs.get(label) if noaa_probs else None
            liquidity = bucket.get("liquidity", 0)

            if mkt is None:
                continue

            wu_diff   = wu_p   - mkt if wu_p   is not None else None
            om_diff   = om_p   - mkt if om_p   is not None else None
            vc_diff   = vc_p   - mkt if vc_p   is not None else None
            noaa_diff = noaa_p - mkt if noaa_p is not None else None

            def fmt_pct(v):
                return f"{v*100:.0f}%" if v is not None else " N/A"

            def fmt_diff(v):
                if v is None:
                    return "  N/A"
                return f"+{v*100:.0f}%" if v >= 0 else f"{v*100:.0f}%"

            def flag(v):
                if v is None: return ""
                if abs(v) >= 0.20: return "🔴"
                if abs(v) >= 0.10: return "🟡"
                return ""

            all_diffs = [d for d in [wu_diff, om_diff, vc_diff, noaa_diff] if d is not None and abs(d) >= 0.10]
            yes_votes = sum(1 for d in all_diffs if d > 0)
            no_votes  = sum(1 for d in all_diffs if d < 0)
            confirmed = " ✅" if yes_votes >= 2 or no_votes >= 2 else ""

            if has_noaa:
                print(
                    f"   {label:<22} "
                    f"{fmt_pct(mkt):>5} {fmt_pct(wu_p):>5} {fmt_pct(om_p):>5} "
                    f"{fmt_pct(vc_p):>5} {fmt_pct(noaa_p):>5} "
                    f"{fmt_diff(wu_diff):>6}{flag(wu_diff)} "
                    f"{fmt_diff(om_diff):>6}{flag(om_diff)} "
                    f"{fmt_diff(vc_diff):>6}{flag(vc_diff)} "
                    f"{fmt_diff(noaa_diff):>6}{flag(noaa_diff)}"
                    f"{confirmed}  ${liquidity:>5,.0f}"
                )
            else:
                print(
                    f"   {label:<22} "
                    f"{fmt_pct(mkt):>6} {fmt_pct(wu_p):>6} {fmt_pct(om_p):>6} {fmt_pct(vc_p):>6} "
                    f"{fmt_diff(wu_diff):>7}{flag(wu_diff)} "
                    f"{fmt_diff(om_diff):>7}{flag(om_diff)} "
                    f"{fmt_diff(vc_diff):>7}{flag(vc_diff)}"
                    f"{confirmed}  ${liquidity:>5,.0f}"
                )

        # Find discrepancies
        event_discreps = find_discrepancies(
            event_title  = event_title,
            city_slug    = city_slug,
            event_date   = event_date,
            buckets      = buckets,
            wu_probs     = wu_probs,
            om_probs     = om_probs,
            wu_temp      = wu_temp,
            om_temp      = om_temp,
            unit_symbol  = unit_sym,
            vc_probs     = vc_probs,
            vc_temp      = vc_temp,
            noaa_probs   = noaa_probs,
            noaa_temp    = noaa_temp,
        )

        if event_discreps:
            events_flagged += 1
            all_discrepancies.extend(event_discreps)
            confirmed_count = sum(1 for d in event_discreps if d["confidence"] == "confirmed")
            print(f"   ⚡ {len(event_discreps)} edge(s) found ({confirmed_count} confirmed)\n")
            # Log every edge for performance tracking
            for d in event_discreps:
                log_edge(d)
        else:
            events_ok += 1
            print(f"   ✅ No significant discrepancies\n")

    # ----------------------------------------------------------
    # STEP 5: Cache results + send ONE summary with filter buttons
    # ----------------------------------------------------------
    print(f"{'='*52}")
    print(f"Processed: {events_ok + events_flagged} | Flagged: {events_flagged} | Skipped: {events_skipped}")
    print(f"Total edges: {len(all_discrepancies)}")

    if not all_discrepancies:
        send_telegram_alert(
            f"🌡️ *Temperature Scan Complete*\n\n"
            f"📅 Events scanned: {total_events}\n"
            f"✅ No significant discrepancies found.\n"
            f"Markets look fairly priced across all sources."
        )
        return

    # Cache the full list — buttons reference this scan_id
    scan_id = store_scan(all_discrepancies)

    s3p = sum(1 for d in all_discrepancies if d.get("source_count", 1) >= 3 and d["edge_size"] == "large")
    s2a = sum(1 for d in all_discrepancies if d.get("source_count", 1) == 2 and d["edge_size"] == "large")
    s1  = sum(1 for d in all_discrepancies if d.get("source_count", 1) == 1 and d["edge_size"] == "large")

    summary_text = summarize_discrepancies(all_discrepancies)

    keyboard = [
        [
            {"text": f"🔥 3+ Agree ({s3p})",  "callback_data": f"f:{scan_id}:3p"},
            {"text": f"✅ 2 Agree ({s2a})",    "callback_data": f"f:{scan_id}:2a"},
            {"text": f"🟡 1 Source ({s1})",    "callback_data": f"f:{scan_id}:1s"},
        ],
    ]

    send_with_keyboard(summary_text, keyboard)
    print(f"✅ Done. Scan {scan_id} cached with {len(all_discrepancies)} edges. Filter buttons sent.")