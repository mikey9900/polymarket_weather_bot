# alerts/telegram_alerts.py

import os
import requests
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")


def _base_url():
    return f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"


def send_telegram_alert(message: str) -> None:
    """Send a plain text message to your Telegram chat."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️  Telegram credentials missing — skipping alert.")
        return
    try:
        r = requests.post(
            f"{_base_url()}/sendMessage",
            json={
                "chat_id":                  TELEGRAM_CHAT_ID,
                "text":                     message,
                "parse_mode":               "Markdown",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        if not r.ok:
            print(f"⚠️  Telegram alert failed: {r.status_code} {r.text[:200]}")
    except Exception as e:
        print(f"⚠️  Telegram alert failed: {e}")


def send_with_keyboard(text: str, keyboard: list) -> dict:
    """
    Send a message with an inline keyboard.

    keyboard format:
        [
            [{"text": "Button 1", "callback_data": "cb1"}, {"text": "URL btn", "url": "https://..."}],
            [{"text": "Row 2 btn", "callback_data": "cb2"}],
        ]

    Returns the Telegram message object on success, or {}.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return {}
    try:
        r = requests.post(
            f"{_base_url()}/sendMessage",
            json={
                "chat_id":                  TELEGRAM_CHAT_ID,
                "text":                     text,
                "parse_mode":               "Markdown",
                "disable_web_page_preview": True,
                "reply_markup":             {"inline_keyboard": keyboard},
            },
            timeout=10,
        )
        if r.ok:
            return r.json().get("result", {})
        print(f"⚠️  send_with_keyboard failed: {r.status_code} {r.text[:200]}")
    except Exception as e:
        print(f"⚠️  send_with_keyboard failed: {e}")
    return {}


def answer_callback(callback_query_id: str, text: str = "", alert: bool = False) -> None:
    """
    Acknowledge a Telegram callback query (required within 10 seconds).
    Set alert=True to show a pop-up notification instead of a toast.
    """
    if not TELEGRAM_BOT_TOKEN:
        return
    try:
        requests.post(
            f"{_base_url()}/answerCallbackQuery",
            json={
                "callback_query_id": callback_query_id,
                "text":              text,
                "show_alert":        alert,
            },
            timeout=10,
        )
    except Exception:
        pass
