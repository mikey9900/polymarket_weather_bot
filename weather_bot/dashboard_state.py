"""Thread-safe dashboard snapshot service."""

from __future__ import annotations

import threading
from collections import deque
from datetime import datetime, timezone
from typing import Any


class DashboardStateService:
    def __init__(self, *, tracker, runtime, control_plane, refresh_seconds: float = 5.0, codex_manager=None):
        self.tracker = tracker
        self.runtime = runtime
        self.control_plane = control_plane
        self.codex_manager = codex_manager
        self.refresh_seconds = max(1.0, float(refresh_seconds))
        self._lock = threading.Lock()
        self._history: deque[dict[str, Any]] = deque(maxlen=240)
        self._state: dict[str, Any] = {}
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self.refresh_once()
        self._thread = threading.Thread(target=self._loop, name="weather-dashboard", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
        self._thread = None

    def refresh_once(self) -> None:
        snapshot = self._build_snapshot()
        with self._lock:
            self._state = snapshot
            self._history.append(
                {
                    "timestamp_utc": snapshot["timestamp_utc"],
                    "paper_pnl": snapshot["summary"]["paper"]["pnl"],
                    "open_positions": snapshot["summary"]["paper"]["open_positions"],
                    "scan_in_progress": snapshot["controls"]["scan_in_progress"],
                }
            )

    def _loop(self) -> None:
        while not self._stop.wait(self.refresh_seconds):
            try:
                self.refresh_once()
            except Exception:
                continue

    def _build_snapshot(self) -> dict[str, Any]:
        paper_stats = self.tracker.get_paper_stats()
        runtime_status = self.runtime.get_status_snapshot()
        payload = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "controls": self.control_plane.build_controls_payload(),
            "runtime": runtime_status,
            "summary": {
                "paper": {
                    "initial": paper_stats["initial_capital"],
                    "balance": paper_stats["current_balance"],
                    "equity": paper_stats["current_equity"],
                    "pnl": paper_stats["total_pnl"],
                    "open_positions": paper_stats["open_positions"],
                    "wins": paper_stats["wins"],
                    "losses": paper_stats["losses"],
                    "win_rate": paper_stats["win_rate"],
                }
            },
            "recent_signals": self.tracker.get_recent_signals(limit=12),
            "recent_trades": self.tracker.get_recent_paper_positions(limit=12),
            "recent_resolutions": self.tracker.get_recent_resolutions(limit=12),
            "recent_operator_actions": self.tracker.get_recent_operator_actions(limit=12),
            "signal_summary_24h": self.tracker.get_signal_summary(),
        }
        strategy_engine = getattr(self.runtime, "strategy_engine", None)
        research_provider = getattr(strategy_engine, "research_provider", None)
        if research_provider is not None and hasattr(research_provider, "status"):
            payload["research_runtime"] = research_provider.status()
        if self.codex_manager is not None:
            payload.update(self.codex_manager.snapshot())
        return payload

    def get_state_threadsafe(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._state)

    def get_history_threadsafe(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._history)

    def apply_control_threadsafe(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = self.control_plane.apply_sync(self.control_plane_request(payload))
        self.refresh_once()
        response = result.to_dict()
        response["state"] = self.get_state_threadsafe()
        return response

    @staticmethod
    def control_plane_request(payload: dict[str, Any]):
        from .control_plane import ControlRequest

        return ControlRequest.from_payload(payload)
