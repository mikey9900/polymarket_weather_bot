"""Minimal Telegram transport."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import requests


class TelegramClient:
    def __init__(self, bot_token: str = "", chat_id: str = ""):
        self.bot_token = bot_token.strip()
        self.chat_id = chat_id.strip()

    @classmethod
    def from_env_or_options(cls, options_path: str | Path = "/data/options.json") -> "TelegramClient":
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        file_path = Path(options_path)
        if file_path.exists():
            try:
                payload = json.loads(file_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                payload = {}
            if isinstance(payload, dict):
                token = token or str(payload.get("telegram_bot_token") or "").strip()
                chat_id = chat_id or str(payload.get("telegram_chat_id") or "").strip()
        return cls(token, chat_id)

    @property
    def available(self) -> bool:
        return bool(self.bot_token and self.chat_id)

    def send_message(self, text: str) -> dict[str, Any]:
        if not self.available:
            return {}
        try:
            response = requests.post(
                f"https://api.telegram.org/bot{self.bot_token}/sendMessage",
                json={
                    "chat_id": self.chat_id,
                    "text": text,
                    "parse_mode": "Markdown",
                    "disable_web_page_preview": True,
                },
                timeout=15,
            )
            if response.ok:
                return response.json().get("result", {})
        except Exception:
            return {}
        return {}

    def send_with_keyboard(self, text: str, keyboard: list[list[dict[str, str]]]) -> dict[str, Any]:
        if not self.available:
            return {}
        try:
            response = requests.post(
                f"https://api.telegram.org/bot{self.bot_token}/sendMessage",
                json={
                    "chat_id": self.chat_id,
                    "text": text,
                    "parse_mode": "Markdown",
                    "disable_web_page_preview": True,
                    "reply_markup": {"inline_keyboard": keyboard},
                },
                timeout=15,
            )
            if response.ok:
                return response.json().get("result", {})
        except Exception:
            return {}
        return {}

    def answer_callback(self, callback_query_id: str, text: str = "", *, alert: bool = False) -> None:
        if not self.bot_token:
            return
        try:
            requests.post(
                f"https://api.telegram.org/bot{self.bot_token}/answerCallbackQuery",
                json={
                    "callback_query_id": callback_query_id,
                    "text": text,
                    "show_alert": alert,
                },
                timeout=10,
            )
        except Exception:
            return
