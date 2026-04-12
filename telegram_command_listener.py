# =============================================================
# telegram_command_listener.py
#
# PURPOSE:
#   Runs continuously and listens for commands sent to your
#   Telegram bot. When you send /scan, it triggers a weather scan.
#
# HOW TELEGRAM BOTS WORK (quick overview):
#   - Your bot has a token (like a password) from @BotFather
#   - You send messages to your bot in Telegram
#   - This script "polls" Telegram every few seconds asking
#     "any new messages?" via the getUpdates API
#   - When it sees a command like /scan, it reacts
#
# COMMANDS:
#   /scan     → scan up to 50 weather events (fast, default)
#   /scan200  → scan up to 200 weather events (thorough)
#   /status   → check if a scan is currently running
# =============================================================

print("🔥 telegram_command_listener.py LOADED")

import time
import requests
import os
import threading
from dotenv import load_dotenv
from run_scanner import run_weather_scan

# =============================================================
# SETUP
# =============================================================

# A "lock" is a thread safety tool. It prevents two scans from
# running at the same time if the user sends /scan twice quickly.
scan_lock = threading.Lock()

# Load .env file so we can read credentials
load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Fail immediately with a clear message if credentials are missing
if not BOT_TOKEN or not CHAT_ID:
    raise RuntimeError(
        "❌ TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not found.\n"
        "   Make sure your .env file exists and has both values set."
    )

# Base URL for all Telegram Bot API calls
BASE_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"


# =============================================================
# TELEGRAM HELPER FUNCTIONS
# =============================================================

def send_message(text: str):
    """
    Sends a plain text message to your Telegram chat.
    Used internally by this listener (not for scan results —
    that's handled by telegram_alerts.py).
    """
    requests.post(
        f"{BASE_URL}/sendMessage",
        json={"chat_id": CHAT_ID, "text": text},
        timeout=10,
    )


def get_updates(offset=None):
    """
    Asks Telegram: "Give me any new messages since `offset`."

    `offset` is the ID of the last message we processed + 1.
    Passing it back tells Telegram to not re-send old messages.

    `timeout=30` means Telegram will hold the connection open
    for up to 30 seconds if there are no new messages — this
    is called "long polling" and is more efficient than hammering
    the API every second.
    """
    r = requests.get(
        f"{BASE_URL}/getUpdates",
        params={"timeout": 30, "offset": offset},
        timeout=35,  # slightly longer than the long-poll timeout
    )
    r.raise_for_status()
    return r.json()


# =============================================================
# SCAN RUNNER (with lock to prevent duplicate scans)
# =============================================================

def run_scan_with_lock(limit: int):
    """
    Starts a scan in a background thread, protected by a lock.

    Args:
        limit (int): Max number of weather EVENTS to scan.

    Why a background thread?
        The scan can take many seconds. If we ran it directly in
        the main loop, we couldn't receive any new Telegram messages
        while it was running. Threading lets both happen at once.

    Why a lock?
        Without it, sending /scan twice quickly would start two
        simultaneous scans hitting the API at the same time.
    """
    # If a scan is already running, reject the new request
    if scan_lock.locked():
        send_message("⛔ A scan is already running. Please wait until it finishes.")
        return

    def _run():
        # `with scan_lock` acquires the lock on entry, releases on exit
        with scan_lock:
            try:
                send_message(f"🚀 Starting weather scan (up to {limit} events)…")
                run_weather_scan(limit=limit)
            except Exception as e:
                # If the scan crashes, report it to Telegram and print traceback
                send_message(f"❌ Scan crashed:\n{e}")
                import traceback
                traceback.print_exc()
            finally:
                # `finally` always runs, even if an exception was raised
                send_message("✅ Scan finished.")

    # Start the scan in a daemon thread (daemon = auto-killed when main script exits)
    threading.Thread(target=_run, daemon=True).start()


# =============================================================
# MAIN LOOP
# =============================================================

def main():
    """
    The main polling loop. Runs forever, checking for new
    Telegram messages every few seconds.
    """
    print("✅ Telegram listener started — waiting for commands...")

    # Send a startup message so you know the bot is online
    send_message(
        "✅ Weather scanner bot online\n\n"
        "Commands:\n"
        "/scan     – scan ~50 weather events (fast)\n"
        "/scan200  – scan ~200 weather events (thorough)\n"
        "/status   – check if a scan is running"
    )

    # `offset` tracks which Telegram updates we've already handled
    # Start at None = get all recent unread messages first
    offset = None

    while True:
        try:
            # Ask Telegram for any new messages
            updates = get_updates(offset)

            # Loop through each new message/command
            for update in updates.get("result", []):
                # Advance offset so this update won't be returned again
                offset = update["update_id"] + 1

                # Extract the message object (could be None for non-message updates)
                message = update.get("message")
                if not message:
                    continue

                # Get the text the user sent, stripped of whitespace
                text = message.get("text", "").strip()

                # --- Handle commands ---

                if text == "/scan":
                    # Fast scan: up to 50 weather events
                    run_scan_with_lock(limit=50)

                elif text == "/scan200":
                    # Thorough scan: up to 200 weather events
                    run_scan_with_lock(limit=200)

                elif text == "/status":
                    # Report whether a scan is currently in progress
                    running = scan_lock.locked()
                    send_message(
                        f"✅ Bot is running\n"
                        f"🔒 Scan in progress: {'YES — please wait' if running else 'NO — ready to scan'}"
                    )

                else:
                    # Unknown command — remind user of valid ones
                    if text.startswith("/"):
                        send_message(
                            f"❓ Unknown command: {text}\n\n"
                            "Try:\n/scan\n/scan200\n/status"
                        )

            # Small sleep between polls to avoid hammering the API
            time.sleep(2)

        except Exception as e:
            # If anything goes wrong (network error, etc.), log it and retry
            print(f"⚠️  Listener error: {e}")
            send_message(
                f"⚠️ Listener error:\n{e}\n\nRestarting in 5 seconds…"
            )
            time.sleep(5)
            offset = None  # reset offset to avoid missing messages


# =============================================================
# ENTRY POINT
# =============================================================

# This block only runs when you execute this file directly:
#   python telegram_command_listener.py
# It does NOT run if this file is imported by another module.
if __name__ == "__main__":
    main()