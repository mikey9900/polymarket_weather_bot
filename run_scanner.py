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
    format_small_edge,
    summarize_discrepancies,
)
from alerts.telegram_alerts import send_telegram_alert

SMALL_EDGES_PER_MESSAGE = 5


def _send_batched(items: list, header: str, format_fn, batch_size: int):
    """Sends a list of items to Telegram in batches."""
    total = (len(items) + batch_size - 1) // batch_size
    for i in range(0, len(items), batch_size):
        batch     = items[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        suffix    = f" ({batch_num}/{total})" if total > 1 else ""
        lines     = [f"{header}{suffix}\n"]
        for item in batch:
            lines.append(format_fn(item))
            lines.append("")
        send_telegram_alert("\n".join(lines))


def run_weather_scan(limit: int = 300):
    """
    Runs the full dual-source temperature market scan.
    """

    print("🔍 Starting dual-source temperature + discrepancy scan...")

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
        wu_probs  = forecast_data.get("wu")
        om_probs  = forecast_data.get("openmeteo")
        vc_probs  = forecast_data.get("vc")
        wu_temp   = forecast_data.get("wu_temp")
        om_temp   = forecast_data.get("om_temp")
        vc_temp   = forecast_data.get("vc_temp")
        unit_sym  = forecast_data.get("unit", "°F")

        if wu_probs is None and om_probs is None and vc_probs is None:
            print(f"   ⚠️  All forecasts failed — skipping\n")
            events_skipped += 1
            continue

        # Determine unit for city
        coords   = CITY_COORDS.get(city_slug, {})
        unit_name = coords.get("unit", "fahrenheit")

        # Print comparison table
        print(f"   {'Bucket':<22} {'Mkt':>6} {'WU':>6} {'OM':>6} {'VC':>6} {'WUΔ':>7} {'OMΔ':>7} {'VCΔ':>7}  {'Liq':>7}")
        print(f"   {'-'*80}")

        for bucket in buckets:
            label     = bucket.get("label", "")
            mkt       = bucket.get("market_yes_price")
            wu_p      = wu_probs.get(label) if wu_probs else None
            om_p      = om_probs.get(label) if om_probs else None
            vc_p      = vc_probs.get(label) if vc_probs else None
            liquidity = bucket.get("liquidity", 0)

            if mkt is None:
                continue

            wu_diff = wu_p - mkt if wu_p is not None else None
            om_diff = om_p - mkt if om_p is not None else None
            vc_diff = vc_p - mkt if vc_p is not None else None

            def fmt_pct(v):
                return f"{v*100:.0f}%" if v is not None else "  N/A"

            def fmt_diff(v):
                if v is None:
                    return "   N/A"
                return f"+{v*100:.0f}%" if v >= 0 else f"{v*100:.0f}%"

            def flag(v):
                if v is None:
                    return ""
                if abs(v) >= 0.20:
                    return "🔴"
                if abs(v) >= 0.10:
                    return "🟡"
                return ""

            # Show ✅ if 2+ sources agree on a discrepancy
            diffs = [d for d in [wu_diff, om_diff, vc_diff] if d is not None and abs(d) >= 0.10]
            yes_votes = sum(1 for d in diffs if d > 0)
            no_votes  = sum(1 for d in diffs if d < 0)
            confirmed = " ✅" if yes_votes >= 2 or no_votes >= 2 else ""

            print(
                f"   {label:<22} "
                f"{fmt_pct(mkt):>6} "
                f"{fmt_pct(wu_p):>6} "
                f"{fmt_pct(om_p):>6} "
                f"{fmt_pct(vc_p):>6} "
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
        )

        if event_discreps:
            events_flagged += 1
            all_discrepancies.extend(event_discreps)
            confirmed_count = sum(1 for d in event_discreps if d["confidence"] == "confirmed")
            print(f"   ⚡ {len(event_discreps)} edge(s) found ({confirmed_count} confirmed)\n")
        else:
            events_ok += 1
            print(f"   ✅ No significant discrepancies\n")

    # ----------------------------------------------------------
    # STEP 5: Send Telegram alerts
    # ----------------------------------------------------------
    print(f"{'='*52}")
    print(f"Processed: {events_ok + events_flagged} | Flagged: {events_flagged} | Skipped: {events_skipped}")
    print(f"Total edges: {len(all_discrepancies)}")

    confirmed  = [d for d in all_discrepancies if d["confidence"] == "confirmed"]
    single_src = [d for d in all_discrepancies if d["confidence"] != "confirmed"]
    large      = [d for d in all_discrepancies if d["edge_size"] == "large"]

    if not all_discrepancies:
        send_telegram_alert(
            f"🌡️ *Temperature Scan Complete*\n\n"
            f"📅 Events scanned: {total_events}\n"
            f"✅ No significant discrepancies found.\n"
            f"Markets are aligned with both WU and Open-Meteo forecasts."
        )
        return

    # 1. Summary
    send_telegram_alert(summarize_discrepancies(all_discrepancies))

    # 2. Confirmed large edges — one message each (best opportunities)
    conf_large = [d for d in confirmed if d["edge_size"] == "large"]
    if conf_large:
        send_telegram_alert(f"✅🔴 *CONFIRMED LARGE EDGES* — {len(conf_large)} found")
        for d in conf_large:
            send_telegram_alert(format_discrepancy_message(d))

    # 3. Confirmed small edges — batched
    conf_small = [d for d in confirmed if d["edge_size"] == "small"]
    if conf_small:
        _send_batched(
            items      = conf_small,
            header     = f"✅🟡 *CONFIRMED SMALL EDGES* — {len(conf_small)} found",
            format_fn  = format_small_edge,
            batch_size = SMALL_EDGES_PER_MESSAGE,
        )

    # 4. Single-source large edges
    ss_large = [d for d in single_src if d["edge_size"] == "large"]
    if ss_large:
        send_telegram_alert(f"🔴 *SINGLE-SOURCE LARGE EDGES* — {len(ss_large)} found")
        for d in ss_large:
            send_telegram_alert(format_discrepancy_message(d))

    # 5. Single-source small edges — batched
    ss_small = [d for d in single_src if d["edge_size"] == "small"]
    if ss_small:
        _send_batched(
            items      = ss_small,
            header     = f"🟡 *SINGLE-SOURCE SMALL EDGES* — {len(ss_small)} found",
            format_fn  = format_small_edge,
            batch_size = SMALL_EDGES_PER_MESSAGE,
        )

    print(f"✅ Done. {len(confirmed)} confirmed + {len(single_src)} single-source edges sent.")