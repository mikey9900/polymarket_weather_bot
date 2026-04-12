# =============================================================
# tracking/scan_tracker.py
#
# PURPOSE:
#   Logs every edge found during a scan so we can track whether
#   the bot's predictions were correct after markets resolve.
#
# STORAGE:
#   /config/weather_bot_edges.json — persists on HA config volume,
#   survives add-on restarts and rebuilds.
#
# WORKFLOW:
#   1. run_scanner.py calls log_edge() for every discrepancy found
#   2. After a market's event_date passes, check_resolutions() queries
#      the Gamma API to see if the market resolved YES or NO
#   3. /stats command shows win rates and accuracy by confidence tier
# =============================================================

import json
import os
import requests
from datetime import date, datetime
from typing import Optional

TRACKING_FILE = "/config/weather_bot_edges.json"
GAMMA_API     = "https://gamma-api.polymarket.com"


# =============================================================
# FILE I/O
# =============================================================

def _load() -> list:
    if not os.path.exists(TRACKING_FILE):
        return []
    try:
        with open(TRACKING_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return []


def _save(edges: list):
    with open(TRACKING_FILE, "w") as f:
        json.dump(edges, f, indent=2, default=str)


# =============================================================
# LOGGING
# =============================================================

def log_edge(d: dict):
    """
    Log a single discrepancy from run_scanner to the tracking file.
    Skips duplicates (same event_date + label + direction within 12h).

    Called automatically after every scan for each edge found.
    """
    edges = _load()

    scan_time  = datetime.now().isoformat(timespec="seconds")
    entry_id   = f"{d.get('city_slug')}_{d.get('event_date')}_{d.get('label')}_{d.get('direction')}"

    # Avoid duplicate entries for the same edge within 12 hours
    recent_ids = {
        e["id"] for e in edges
        if e.get("scan_time", "") >= scan_time[:10]  # same day
    }
    if entry_id in recent_ids:
        return

    edges.append({
        "id":           entry_id,
        "scan_time":    scan_time,
        "event_title":  d.get("event_title", ""),
        "city_slug":    d.get("city_slug", ""),
        "event_date":   str(d.get("event_date", "")),
        "label":        d.get("label", ""),
        "direction":    d.get("direction", ""),       # "YES" or "NO" — what we think the edge is
        "confidence":   d.get("confidence", ""),      # "confirmed", "wu_only", "om_only", "vc_only"
        "edge_size":    d.get("edge_size", ""),       # "large" or "small"
        "market_price": round(d.get("market_prob", 0), 3),
        "wu_prob":      d.get("wu_prob"),
        "om_prob":      d.get("om_prob"),
        "vc_prob":      d.get("vc_prob"),
        "edge":         round(d.get("discrepancy", 0), 3),
        "liquidity":    d.get("liquidity", 0),
        "event_slug":   d.get("event_slug", ""),
        "market_slug":  d.get("market_slug", ""),
        # Filled in later by check_resolutions()
        "resolved":     False,
        "resolution":   None,   # "YES" or "NO"
        "result":       None,   # "WIN" or "LOSS"
    })

    _save(edges)


# =============================================================
# TRADE LOGGING (user clicked "Mark Bought" button)
# =============================================================

def log_trade(scan_id: str, edge_index: int):
    """
    Mark an edge as bought in the tracking file when the user
    clicks the "Mark Bought" inline button on Telegram.

    Matches by the edge's ID (city_slug + event_date + label + direction)
    and stamps bought=True + buy_time on the existing tracking entry.
    """
    from alerts.scan_cache import get_edge
    edge_data = get_edge(scan_id, edge_index)
    if not edge_data:
        print(f"    ⚠️  log_trade: no edge found for {scan_id}[{edge_index}]")
        return

    edges    = _load()
    entry_id = (
        f"{edge_data.get('city_slug')}_{edge_data.get('event_date')}"
        f"_{edge_data.get('label')}_{edge_data.get('direction')}"
    )
    buy_time = datetime.now().isoformat(timespec="seconds")

    matched = False
    for e in edges:
        if e.get("id") == entry_id:
            e["bought"]    = True
            e["buy_price"] = edge_data.get("market_prob")
            e["buy_time"]  = buy_time
            matched = True
            break

    if not matched:
        # Edge not in tracker yet (e.g., scan was run before tracker was added) — insert it
        edges.append({
            "id":           entry_id,
            "scan_time":    buy_time,
            "event_title":  edge_data.get("event_title", ""),
            "city_slug":    edge_data.get("city_slug", ""),
            "event_date":   str(edge_data.get("event_date", "")),
            "label":        edge_data.get("label", ""),
            "direction":    edge_data.get("direction", ""),
            "confidence":   edge_data.get("confidence", ""),
            "edge_size":    edge_data.get("edge_size", ""),
            "market_price": round(edge_data.get("market_prob", 0), 3),
            "wu_prob":      edge_data.get("wu_prob"),
            "om_prob":      edge_data.get("om_prob"),
            "vc_prob":      edge_data.get("vc_prob"),
            "edge":         round(edge_data.get("discrepancy", 0), 3),
            "liquidity":    edge_data.get("liquidity", 0),
            "event_slug":   edge_data.get("event_slug", ""),
            "market_slug":  edge_data.get("market_slug", ""),
            "bought":       True,
            "buy_price":    edge_data.get("market_prob"),
            "buy_time":     buy_time,
            "resolved":     False,
            "resolution":   None,
            "result":       None,
        })

    _save(edges)
    print(f"    💰 Trade logged: {entry_id}")


# =============================================================
# RESOLUTION CHECKING
# =============================================================

def _get_resolution(market_slug: str) -> Optional[str]:
    """
    Query Gamma API to check if a market has resolved.
    Returns "YES", "NO", or None if still open.
    """
    if not market_slug:
        return None
    try:
        r = requests.get(
            f"{GAMMA_API}/markets",
            params={"slug": market_slug},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        if not data:
            return None
        market = data[0]

        # Market resolves when closed=True and resolutionPrice is set
        if not market.get("closed"):
            return None

        res_price = market.get("resolutionPrice")
        if res_price is None:
            return None

        return "YES" if float(res_price) >= 0.5 else "NO"

    except Exception:
        return None


def check_resolutions():
    """
    Check all unresolved edges whose event_date has passed.
    Updates tracking file with resolution outcomes.

    Called automatically at the start of each scan.
    """
    edges   = _load()
    today   = date.today().isoformat()
    updated = 0

    for edge in edges:
        if edge.get("resolved"):
            continue
        if edge.get("event_date", "9999-99-99") >= today:
            continue  # market hasn't closed yet

        resolution = _get_resolution(edge.get("market_slug", ""))
        if resolution is None:
            continue

        edge["resolved"]   = True
        edge["resolution"] = resolution
        # WIN = market resolved in the direction we said to bet
        edge["result"]     = "WIN" if resolution == edge["direction"] else "LOSS"
        updated += 1

    if updated:
        _save(edges)
        print(f"    📊 Updated {updated} resolved edge(s) in tracking file")

    return updated


# =============================================================
# STATS
# =============================================================

def get_stats() -> str:
    """
    Generate a performance summary for the /stats Telegram command.
    Shows win rates broken down by confidence tier and edge size.
    """
    edges    = _load()
    resolved = [e for e in edges if e.get("resolved")]
    pending  = [e for e in edges if not e.get("resolved")]

    if not edges:
        return (
            "📊 *Scan Performance Tracker*\n\n"
            "No edges logged yet. Run /scan to start tracking."
        )

    def win_rate(subset: list) -> str:
        wins   = sum(1 for e in subset if e.get("result") == "WIN")
        total  = len(subset)
        if total == 0:
            return "—"
        return f"{wins}/{total} ({round(wins/total*100)}%)"

    def avg_edge(subset: list) -> str:
        vals = [abs(e.get("edge", 0)) for e in subset]
        if not vals:
            return "—"
        return f"{round(sum(vals)/len(vals)*100)}%"

    confirmed = [e for e in resolved if e.get("confidence") == "confirmed"]
    single    = [e for e in resolved if e.get("confidence") != "confirmed"]
    large     = [e for e in resolved if e.get("edge_size") == "large"]
    small     = [e for e in resolved if e.get("edge_size") == "small"]

    # Recent results
    recent = sorted(resolved, key=lambda e: e.get("scan_time", ""), reverse=True)[:5]

    lines = [
        "📊 *Scan Performance Tracker*\n",
        f"Total edges logged: {len(edges)}",
        f"Resolved: {len(resolved)} | Pending: {len(pending)}\n",
        "*Win rate by confidence:*",
        f"  ✅ Confirmed: {win_rate(confirmed)} (avg edge: {avg_edge(confirmed)})",
        f"  🌤️ Single source: {win_rate(single)} (avg edge: {avg_edge(single)})\n",
        "*Win rate by edge size:*",
        f"  🔴 Large (20%+): {win_rate(large)}",
        f"  🟡 Small (10-20%): {win_rate(small)}\n",
        f"*Overall: {win_rate(resolved)}*",
    ]

    if recent:
        lines.append("\n*Recent results:*")
        for e in recent:
            result_icon = "✅" if e.get("result") == "WIN" else "❌"
            short = e.get("event_title", "").replace("Highest temperature in ", "").replace("?", "")
            edge_pct = round(abs(e.get("edge", 0)) * 100)
            lines.append(
                f"  {result_icon} {short} `{e.get('label','')}` "
                f"BET {e.get('direction','')} | edge: {edge_pct}%"
            )

    return "\n".join(lines)
