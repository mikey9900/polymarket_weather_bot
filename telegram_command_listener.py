# =============================================================
# telegram_command_listener.py
#
# PURPOSE:
#   Runs continuously and listens for commands sent to your
#   Telegram bot. Also auto-scans on a schedule.
#
# COMMANDS:
#   /scan     → run a full scan immediately
#   /status   → check if a scan is running + time until next auto-scan
#
# AUTO-SCAN:
#   Runs automatically every AUTO_SCAN_HOURS hours.
# =============================================================

print("🔥 telegram_command_listener.py LOADED")

import time
import requests
import os
import threading
from datetime import datetime, timedelta
from dotenv import load_dotenv
from run_scanner import run_weather_scan
from portfolio.portfolio_tracker import run_portfolio_check
from tracking.scan_tracker import get_stats, log_trade
from alerts.scan_cache import get_scan, get_edge
from alerts.telegram_alerts import send_with_keyboard, answer_callback
from logic.discrepancy_logic import format_discrepancy_message

# =============================================================
# CONFIG
# =============================================================

AUTO_SCAN_HOURS = 4  # how often to auto-scan (change this as needed)
SCAN_LIMIT = 300     # scan all possible events every time

# =============================================================
# SETUP
# =============================================================

scan_lock = threading.Lock()
load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not BOT_TOKEN or not CHAT_ID:
    raise RuntimeError(
        "❌ TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not found.\n"
        "   Make sure your .env file exists and has both values set."
    )

BASE_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"

# Track when the next auto-scan is scheduled
next_auto_scan = datetime.now() + timedelta(hours=AUTO_SCAN_HOURS)


# =============================================================
# TELEGRAM HELPER FUNCTIONS
# =============================================================

def send_message(text: str):
    requests.post(
        f"{BASE_URL}/sendMessage",
        json={"chat_id": CHAT_ID, "text": text},
        timeout=10,
    )


def get_updates(offset=None):
    r = requests.get(
        f"{BASE_URL}/getUpdates",
        params={"timeout": 30, "offset": offset},
        timeout=35,
    )
    r.raise_for_status()
    return r.json()


# =============================================================
# CALLBACK HANDLERS (inline button presses)
# =============================================================

def _edge_url(d: dict) -> str:
    slug = d.get("event_slug") or d.get("market_slug", "")
    return f"https://polymarket.com/event/{slug}" if slug else ""


def _send_edge_with_buttons(d: dict, scan_id: str, index: int):
    """Send a single edge message with Mark Bought + View on Polymarket buttons."""
    text = format_discrepancy_message(d)
    url  = _edge_url(d)

    keyboard = [[
        {"text": "💰 Mark Bought", "callback_data": f"b:{scan_id}:{index}"},
    ]]
    if url:
        keyboard[0].append({"text": "📊 View →", "url": url})

    send_with_keyboard(text, keyboard)


def _handle_filter(scan_id: str, filt: str):
    """Send edges from a cached scan filtered by button type."""
    edges = get_scan(scan_id)
    if edges is None:
        send_message("⚠️ Scan results expired. Please run /scan again.")
        return

    if filt == "3p":
        subset = [(i, d) for i, d in enumerate(edges) if d.get("source_count", 1) >= 3]
        label  = "🔥 3+ Sources Agree"
    elif filt == "2a":
        subset = [(i, d) for i, d in enumerate(edges) if d.get("source_count", 1) == 2]
        label  = "✅ 2 Sources Agree"
    elif filt == "1s":
        subset = [(i, d) for i, d in enumerate(edges) if d.get("source_count", 1) == 1]
        label  = "🟡 Single Source"
    elif filt == "lg":
        subset = [(i, d) for i, d in enumerate(edges) if d.get("edge_size") == "large"]
        label  = "🔴 Large Edges (20%+)"
    else:
        send_message(f"❓ Unknown filter: {filt}")
        return

    if not subset:
        send_message(f"{label}\n\nNo edges match this filter.")
        return

    send_message(f"{label} — {len(subset)} edge(s)")
    for index, d in subset:
        _send_edge_with_buttons(d, scan_id, index)


def _handle_buy(scan_id: str, edge_index: int):
    """Log a trade when user clicks Mark Bought."""
    log_trade(scan_id, edge_index)


# =============================================================
# SCAN RUNNER
# =============================================================

def run_scan_with_lock(triggered_by: str = "manual"):
    """
    Starts a full scan in a background thread.
    triggered_by: "manual" or "auto" — shown in the Telegram message.
    """
    global next_auto_scan

    if scan_lock.locked():
        if triggered_by == "manual":
            send_message("⛔ A scan is already running. Please wait until it finishes.")
        return

    def _run():
        with scan_lock:
            try:
                label = "🤖 Auto-scan" if triggered_by == "auto" else "🚀 Manual scan"
                send_message(f"{label} starting…")
                run_weather_scan(limit=SCAN_LIMIT)
            except Exception as e:
                send_message(f"❌ Scan crashed:\n{e}")
                import traceback
                traceback.print_exc()
            finally:
                send_message("✅ Scan finished.")

    threading.Thread(target=_run, daemon=True).start()


# =============================================================
# AUTO-SCAN SCHEDULER
# =============================================================

def auto_scan_loop():
    """
    Runs in a background thread. Triggers a scan every AUTO_SCAN_HOURS hours.
    """
    global next_auto_scan
    while True:
        now = datetime.now()
        if now >= next_auto_scan:
            next_auto_scan = now + timedelta(hours=AUTO_SCAN_HOURS)
            run_scan_with_lock(triggered_by="auto")
        time.sleep(60)  # check every minute


# =============================================================
# MAIN LOOP
# =============================================================

def main():
    global next_auto_scan
    print("✅ Telegram listener started — waiting for commands...")

    # Start the auto-scan background thread
    threading.Thread(target=auto_scan_loop, daemon=True).start()

    send_message(
        f"✅ Weather scanner bot online\n\n"
        f"Auto-scan every {AUTO_SCAN_HOURS}h\n"
        f"Next auto-scan: {next_auto_scan.strftime('%H:%M')}\n\n"
        "Commands:\n"
        "/scan       – run a full scan now\n"
        "/portfolio  – check open positions + recommendations\n"
        "/stats      – win rate + edge performance tracker\n"
        "/status     – scan status + next auto-scan time"
    )

    offset = None

    while True:
        try:
            updates = get_updates(offset)

            for update in updates.get("result", []):
                offset = update["update_id"] + 1

                # ── Inline button callbacks ──────────────────────────────
                callback = update.get("callback_query")
                if callback:
                    cb_id   = callback["id"]
                    cb_data = callback.get("data", "")

                    if cb_data.startswith("f:"):
                        _, scan_id, filt = cb_data.split(":", 2)
                        answer_callback(cb_id, "Loading…")
                        threading.Thread(
                            target=_handle_filter, args=(scan_id, filt), daemon=True
                        ).start()

                    elif cb_data.startswith("b:"):
                        _, scan_id, idx_str = cb_data.split(":", 2)
                        answer_callback(cb_id, "✅ Marked as bought!")
                        _handle_buy(scan_id, int(idx_str))

                    else:
                        answer_callback(cb_id)
                    continue

                # ── Text commands ────────────────────────────────────────
                message = update.get("message")
                if not message:
                    continue

                text = message.get("text", "").strip()

                if text == "/scan":
                    run_scan_with_lock(triggered_by="manual")

                elif text == "/portfolio":
                    def _run_portfolio():
                        try:
                            send_message("📊 Checking your positions…")
                            items = run_portfolio_check()
                            for item in items:
                                msg = item["text"]
                                url = item.get("url")
                                if url:
                                    send_with_keyboard(msg, [[{"text": "📊 View on Polymarket →", "url": url}]])
                                else:
                                    send_message(msg)
                        except Exception as e:
                            send_message(f"❌ Portfolio check failed:\n{e}")
                            import traceback
                            traceback.print_exc()
                    threading.Thread(target=_run_portfolio, daemon=True).start()

                elif text == "/stats":
                    try:
                        send_message(get_stats())
                    except Exception as e:
                        send_message(f"❌ Stats failed:\n{e}")

                elif text == "/status":
                    running = scan_lock.locked()
                    mins_until = max(0, int((next_auto_scan - datetime.now()).total_seconds() / 60))
                    send_message(
                        f"✅ Bot is running\n"
                        f"🔒 Scan in progress: {'YES — please wait' if running else 'NO — ready'}\n"
                        f"⏰ Next auto-scan in: {mins_until} min"
                    )

                else:
                    if text.startswith("/"):
                        send_message(
                            f"❓ Unknown command: {text}\n\n"
                            "Try:\n/scan\n/portfolio\n/stats\n/status"
                        )

            time.sleep(2)

        except Exception as e:
            print(f"⚠️  Listener error: {e}")
            time.sleep(5)
            offset = None


if __name__ == "__main__":
    main()
