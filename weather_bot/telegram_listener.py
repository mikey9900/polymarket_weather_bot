"""Optional Telegram backup listener for the weather platform."""

from __future__ import annotations

import threading
import time
from typing import Any

import requests

from .control_plane import ControlRequest
from .messages import format_scan_summary, format_signal_message, format_status_message


class TelegramBackupService:
    def __init__(self, app, *, poll_timeout_seconds: int = 30) -> None:
        self.app = app
        self.telegram = app.telegram
        self.poll_timeout_seconds = max(5, int(poll_timeout_seconds))
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._offset: int | None = None

    @property
    def enabled(self) -> bool:
        return bool(self.app.config.alerts.telegram_enabled and self.telegram.available)

    @property
    def base_url(self) -> str:
        return f"https://api.telegram.org/bot{self.telegram.bot_token}"

    def start(self) -> bool:
        if not self.enabled:
            return False
        if self._thread is not None and self._thread.is_alive():
            return True
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="weather-telegram-backup", daemon=True)
        self._thread.start()
        return True

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
        self._thread = None

    def run_forever(self, *, require_credentials: bool = False) -> None:
        if require_credentials and not self.telegram.available:
            raise RuntimeError("Telegram credentials are required to run telegram backup controls.")
        if not self.enabled:
            if require_credentials:
                raise RuntimeError("Telegram backup controls are disabled in config.")
            return
        self._stop.clear()
        self._announce_online()
        self._poll_loop()

    def _run(self) -> None:
        try:
            self._announce_online()
            self._poll_loop()
        except Exception:
            return

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            try:
                updates = self._get_updates(offset=self._offset)
                for update in updates.get("result", []):
                    self._offset = int(update["update_id"]) + 1
                    if "callback_query" in update:
                        callback = update["callback_query"]
                        self.telegram.answer_callback(callback["id"])
                        self._handle_callback(callback.get("data", ""))
                        continue
                    message = update.get("message") or {}
                    text = message.get("text", "")
                    if text:
                        self._handle_command(text)
            except Exception as exc:
                self.telegram.send_message(f"Telegram listener error: {exc}")
                if self._stop.wait(5.0):
                    return

    def _get_updates(self, offset: int | None = None) -> dict[str, Any]:
        response = requests.get(
            f"{self.base_url}/getUpdates",
            params={"timeout": self.poll_timeout_seconds, "offset": offset},
            timeout=self.poll_timeout_seconds + 5,
        )
        response.raise_for_status()
        return response.json()

    def _announce_online(self) -> None:
        self.telegram.send_message("Weather bot online. Home Assistant is primary; Telegram is running as backup control.")
        self._send_command_footer()

    def _send_command_footer(self) -> None:
        keyboard = [
            [
                {"text": "Temp Scan", "callback_data": "cmd:scan"},
                {"text": "Precip Scan", "callback_data": "cmd:precip"},
                {"text": "Status", "callback_data": "cmd:status"},
            ],
            [
                {"text": "Resume", "callback_data": "cmd:start"},
                {"text": "Pause", "callback_data": "cmd:stop"},
                {"text": "Paper Auto", "callback_data": "cmd:paperauto"},
            ],
            [
                {"text": "Research", "callback_data": "cmd:research"},
                {"text": "Tune", "callback_data": "cmd:tune"},
            ],
        ]
        self.telegram.send_with_keyboard("Controls", keyboard)

    def _send_temperature_results(self) -> None:
        batch, results = self.app.runtime.run_temperature_scan(send_alerts=False)
        self.telegram.send_message(
            format_scan_summary(
                batch,
                accepted_count=sum(1 for item in results if item.decision.accepted),
                opened_count=sum(1 for item in results if item.position),
            )
        )
        for result in results[:10]:
            if result.position is not None or result.decision.accepted:
                self.telegram.send_message(format_signal_message(result.signal))
        self._send_command_footer()

    def _send_precipitation_results(self) -> None:
        batch, results = self.app.runtime.run_precipitation_scan(send_alerts=False)
        self.telegram.send_message(
            format_scan_summary(
                batch,
                accepted_count=sum(1 for item in results if item.decision.accepted),
                opened_count=sum(1 for item in results if item.position),
            )
        )
        for result in results[:10]:
            if result.position is not None or result.decision.accepted:
                self.telegram.send_message(format_signal_message(result.signal))
        self._send_command_footer()

    def _send_status(self) -> None:
        self.app.dashboard_state.refresh_once()
        self.telegram.send_message(format_status_message(self.app.dashboard_state.get_state_threadsafe()))
        self._send_command_footer()

    def _apply_control(self, action: str, value: Any = None) -> None:
        result = self.app.control_plane.apply_sync(ControlRequest(action, value))
        self.telegram.send_message(result.message)
        if action in {"research_run_now", "tuner_run_now", "tuner_promote_latest", "tuner_reject_latest", "start", "stop"}:
            self._send_status()
        else:
            self._send_command_footer()

    def _handle_command(self, text: str) -> None:
        command = (text or "").strip()
        lower = command.lower()
        if lower.startswith("/scan"):
            self._send_temperature_results()
            return
        if lower.startswith("/precip"):
            self._send_precipitation_results()
            return
        if lower.startswith("/status") or lower.startswith("/stats"):
            self._send_status()
            return
        if lower.startswith("/start"):
            self._apply_control("start")
            return
        if lower.startswith("/stop"):
            self._apply_control("stop")
            return
        if lower.startswith("/paperauto"):
            parts = command.split(maxsplit=1)
            self._apply_control("toggle_paper_auto_trade", parts[1] if len(parts) == 2 else "false")
            return
        if lower.startswith("/temp"):
            parts = command.split(maxsplit=1)
            self._apply_control("toggle_temperature", parts[1] if len(parts) == 2 else "true")
            return
        if lower.startswith("/preciptoggle"):
            parts = command.split(maxsplit=1)
            self._apply_control("toggle_precipitation", parts[1] if len(parts) == 2 else "true")
            return
        if lower.startswith("/paper"):
            parts = command.split(maxsplit=1)
            if len(parts) == 2:
                self._apply_control("set_paper_capital", parts[1])
            else:
                self.telegram.send_message("Usage: /paper 5000")
                self._send_command_footer()
            return
        if lower.startswith("/research"):
            self._apply_control("research_run_now")
            return
        if lower.startswith("/tune"):
            self._apply_control("tuner_run_now")
            return
        if lower.startswith("/promote"):
            self._apply_control("tuner_promote_latest")
            return
        if lower.startswith("/reject"):
            self._apply_control("tuner_reject_latest")
            return
        self.telegram.send_message(
            "Commands:\n"
            "/scan - run temperature scan\n"
            "/precip - run precipitation scan\n"
            "/status - runtime and paper stats\n"
            "/start - resume automation\n"
            "/stop - pause automation\n"
            "/paper <amount> - reset paper capital\n"
            "/paperauto on|off - toggle auto paper trading\n"
            "/temp on|off - toggle temperature automation\n"
            "/preciptoggle on|off - toggle precipitation automation\n"
            "/research - refresh research artifacts\n"
            "/tune - build a tuning candidate\n"
            "/promote - promote latest candidate\n"
            "/reject - reject latest candidate"
        )
        self._send_command_footer()

    def _handle_callback(self, data: str) -> None:
        action = str(data or "").split(":", 1)[-1]
        if action == "scan":
            self._send_temperature_results()
        elif action == "precip":
            self._send_precipitation_results()
        elif action == "status":
            self._send_status()
        elif action == "start":
            self._apply_control("start")
        elif action == "stop":
            self._apply_control("stop")
        elif action == "paperauto":
            current = bool(self.app.runtime.get_status_snapshot().get("paper_auto_trade", True))
            self._apply_control("toggle_paper_auto_trade", "false" if current else "true")
        elif action == "research":
            self._apply_control("research_run_now")
        elif action == "tune":
            self._apply_control("tuner_run_now")
        else:
            self.telegram.send_message(f"Unknown callback: {action}")
            self._send_command_footer()
