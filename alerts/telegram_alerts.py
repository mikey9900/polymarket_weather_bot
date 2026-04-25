"""Compatibility wrapper for the new Telegram transport."""

from __future__ import annotations

from weather_bot.telegram_client import TelegramClient


_CLIENT = TelegramClient.from_env_or_options()


def send_telegram_alert(message: str) -> None:
    _CLIENT.send_message(message)


def send_with_keyboard(text: str, keyboard: list) -> dict:
    return _CLIENT.send_with_keyboard(text, keyboard)


def answer_callback(callback_query_id: str, text: str = "", alert: bool = False) -> None:
    _CLIENT.answer_callback(callback_query_id, text, alert=alert)
