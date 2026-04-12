# alerts/telegram_alerts.py

import os
import requests
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")


def send_telegram_alert(message: str) -> None:
    """
    Sends a message to your Telegram chat.
    Credentials are loaded from .env — never hardcoded.
    """

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️  Telegram credentials missing from .env — skipping alert.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id":                  TELEGRAM_CHAT_ID,
        "text":                     message,
        "parse_mode":               "Markdown",
        "disable_web_page_preview": True,
    }

    try:
        r = requests.post(url, json=payload, timeout=10)
        if not r.ok:
            print(f"⚠️  Telegram alert failed: {r.status_code} {r.text[:200]}")
    except Exception as e:
        print(f"⚠️  Telegram alert failed: {e}")