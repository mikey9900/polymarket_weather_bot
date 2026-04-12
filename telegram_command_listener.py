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
        "/scan    – run a full scan now\n"
        "/status  – check scan status + next auto-scan time"
    )

    offset = None

    while True:
        try:
            updates = get_updates(offset)

            for update in updates.get("result", []):
                offset = update["update_id"] + 1

                message = update.get("message")
                if not message:
                    continue

                text = message.get("text", "").strip()

                if text == "/scan":
                    run_scan_with_lock(triggered_by="manual")

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
                            "Try:\n/scan\n/status"
                        )

            time.sleep(2)

        except Exception as e:
            print(f"⚠️  Listener error: {e}")
            time.sleep(5)
            offset = None


if __name__ == "__main__":
    main()
