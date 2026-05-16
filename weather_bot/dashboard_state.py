"""Thread-safe dashboard snapshot service."""

from __future__ import annotations

import json
import threading
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class DashboardStateService:
    def __init__(self, *, tracker, runtime, control_plane, refresh_seconds: float = 5.0, codex_manager=None, state_export_path: str | Path | None = None, analysis_exporter=None):
        self.tracker = tracker
        self.runtime = runtime
        self.control_plane = control_plane
        self.codex_manager = codex_manager
        self.analysis_exporter = analysis_exporter
        self.refresh_seconds = max(1.0, float(refresh_seconds))
        self.state_export_path = Path(state_export_path) if state_export_path else None
        self._state_export_error: str | None = None
        if self.state_export_path is not None:
            try:
                self.state_export_path.parent.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                self._state_export_error = str(exc)
                self.state_export_path = None
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
        self._sync_export(snapshot)
        with self._lock:
            self._state = snapshot
            self._history.append(
                {
                    "timestamp_utc": snapshot["timestamp_utc"],
                    "paper_pnl": snapshot["summary"]["paper"]["pnl"],
                    "execution_pnl": snapshot["summary"].get("execution", {}).get("scoreboard_pnl", 0.0),
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
        shadow_execution_summary = self.tracker.get_shadow_execution_summary()
        runtime_status = self.runtime.get_status_snapshot()
        analysis_status = self._analysis_export_status()
        stale_after_s = getattr(getattr(self.runtime, "config", None), "paper", None)
        stale_after_s = getattr(stale_after_s, "mark_stale_after_seconds", None)
        app_timezone = getattr(getattr(self.runtime, "config", None), "app", None)
        app_timezone = getattr(app_timezone, "timezone", "UTC")
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
                },
                "execution": _execution_scoreboard_summary(
                    paper_stats=paper_stats,
                    shadow_execution_summary=shadow_execution_summary,
                ),
            },
            "pnl_analytics": self.tracker.get_pnl_analytics(timezone_name=app_timezone),
            "paper_vs_shadow_daily_summary": self.tracker.get_paper_vs_shadow_daily_summary(
                timezone_name=app_timezone,
                weekend_only=True,
            ),
            "decision_activity_12h": self.tracker.get_decision_activity_summary(
                hours=12,
                timezone_name=app_timezone,
            ),
            "open_positions": self.tracker.get_dashboard_paper_positions(
                limit=250,
                status="open",
                mark_stale_after_seconds=stale_after_s,
            ),
            "recent_signals": self.tracker.get_recent_signals(limit=12),
            "recent_trades": self.tracker.get_dashboard_paper_positions(
                limit=12,
                mark_stale_after_seconds=stale_after_s,
            ),
            "recent_outcomes": self.tracker.get_dashboard_paper_positions(
                limit=10,
                statuses=("closed", "resolved"),
                mark_stale_after_seconds=stale_after_s,
            ),
            "recent_resolutions": self.tracker.get_recent_resolutions(limit=12),
            "recent_operator_actions": self.tracker.get_recent_operator_actions(limit=12),
            "recent_shadow_orders": self.tracker.get_recent_shadow_order_intents(limit=12),
            "shadow_execution_summary": shadow_execution_summary,
            "shadow_execution_orders": self.tracker.get_recent_shadow_exec_orders(limit=50),
            "shadow_execution_positions": self.tracker.get_shadow_exec_positions(limit=50),
            "shadow_execution_fills": self.tracker.get_recent_shadow_exec_fills(limit=50),
            "shadow_execution_trade_events": self.tracker.get_recent_shadow_exec_trade_events(limit=50),
            "shadow_execution_missed": self.tracker.get_shadow_execution_missed_paper_trades(limit=50),
            "signal_summary_24h": self.tracker.get_signal_summary(),
            "exports": {
                "dashboard_state_path": str(self.state_export_path) if self.state_export_path is not None else None,
                "dashboard_state_error": self._state_export_error,
                "scan_export_root": str(self.runtime.scan_export_root) if getattr(self.runtime, "scan_export_root", None) is not None else None,
                "scan_export_error": runtime_status.get("last_scan_export_error"),
                **analysis_status,
            },
        }
        strategy_engine = getattr(self.runtime, "strategy_engine", None)
        research_provider = getattr(strategy_engine, "research_provider", None)
        if research_provider is not None and hasattr(research_provider, "status"):
            payload["research_runtime"] = research_provider.status()
        if self.codex_manager is not None:
            payload.update(self.codex_manager.snapshot())
        return payload

    def _sync_export(self, snapshot: dict[str, Any]) -> None:
        exports = snapshot.setdefault("exports", {})
        exports["dashboard_state_path"] = str(self.state_export_path) if self.state_export_path is not None else None
        if self.state_export_path is None:
            exports["dashboard_state_error"] = self._state_export_error
            return
        try:
            self.state_export_path.write_text(json.dumps(snapshot, indent=2, sort_keys=True), encoding="utf-8")
            self._state_export_error = None
        except OSError as exc:
            self._state_export_error = str(exc)
        exports["dashboard_state_error"] = self._state_export_error

    def get_state_threadsafe(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._state)

    def get_history_threadsafe(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._history)

    def apply_control_threadsafe(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            request = self.control_plane_request(payload)
            result = self.control_plane.apply_sync(request)
        except Exception as exc:
            return {
                "ok": False,
                "status": 500,
                "message": f"Control handler crashed: {type(exc).__name__}: {exc}",
                "state": self.get_state_threadsafe(),
            }
        if self._should_skip_refresh(request.action, result.status):
            response = result.to_dict()
            response["state"] = self._fast_control_state()
            return response
        refresh_error: str | None = None
        try:
            self.refresh_once()
        except Exception as exc:
            refresh_error = f"{type(exc).__name__}: {exc}"
        response = result.to_dict()
        response["state"] = self.get_state_threadsafe()
        if refresh_error:
            response["refresh_error"] = refresh_error
            response["message"] = f"{response.get('message', 'Control applied.')} State refresh warning: {refresh_error}"
        return response

    def _fast_control_state(self) -> dict[str, Any]:
        snapshot = self.get_state_threadsafe()
        if not snapshot:
            return {
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "controls": self.control_plane.build_controls_payload(),
                "runtime": self.runtime.get_status_snapshot(),
                "summary": {"paper": {}, "execution": {}},
                "pnl_analytics": {"generated_at": datetime.now(timezone.utc).isoformat(), "open_book": {}, "windows": {}},
                "paper_vs_shadow_daily_summary": {},
                "decision_activity_12h": {},
                "open_positions": [],
                "recent_signals": [],
                "recent_trades": [],
                "recent_outcomes": [],
                "recent_resolutions": [],
                "recent_operator_actions": [],
                "recent_shadow_orders": [],
                "shadow_execution_summary": {},
                "shadow_execution_orders": [],
                "shadow_execution_positions": [],
                "shadow_execution_fills": [],
                "shadow_execution_trade_events": [],
                "shadow_execution_missed": [],
                "signal_summary_24h": {},
                "exports": {
                    "dashboard_state_path": str(self.state_export_path) if self.state_export_path is not None else None,
                    "dashboard_state_error": self._state_export_error,
                    "scan_export_root": str(self.runtime.scan_export_root) if getattr(self.runtime, "scan_export_root", None) is not None else None,
                    "scan_export_error": self.runtime.get_status_snapshot().get("last_scan_export_error"),
                    **self._analysis_export_status(),
                },
            }
        runtime_status = self.runtime.get_status_snapshot()
        paper_stats = self.tracker.get_paper_stats()
        shadow_execution_summary = self.tracker.get_shadow_execution_summary()
        app_timezone = getattr(getattr(self.runtime, "config", None), "app", None)
        app_timezone = getattr(app_timezone, "timezone", "UTC")
        snapshot["timestamp_utc"] = datetime.now(timezone.utc).isoformat()
        snapshot["runtime"] = runtime_status
        snapshot["controls"] = self.control_plane.build_controls_payload()
        summary = dict(snapshot.get("summary") or {})
        summary["execution"] = _execution_scoreboard_summary(
            paper_stats=paper_stats,
            shadow_execution_summary=shadow_execution_summary,
        )
        snapshot["paper_vs_shadow_daily_summary"] = self.tracker.get_paper_vs_shadow_daily_summary(
            timezone_name=app_timezone,
            weekend_only=True,
        )
        snapshot["decision_activity_12h"] = self.tracker.get_decision_activity_summary(
            hours=12,
            timezone_name=app_timezone,
        )
        snapshot["summary"] = summary
        snapshot["shadow_execution_summary"] = shadow_execution_summary
        exports = dict(snapshot.get("exports") or {})
        exports["dashboard_state_path"] = str(self.state_export_path) if self.state_export_path is not None else None
        exports["dashboard_state_error"] = self._state_export_error
        exports["scan_export_root"] = str(self.runtime.scan_export_root) if getattr(self.runtime, "scan_export_root", None) is not None else None
        exports["scan_export_error"] = runtime_status.get("last_scan_export_error")
        exports.update(self._analysis_export_status())
        snapshot["exports"] = exports
        return snapshot

    def _analysis_export_status(self) -> dict[str, Any]:
        if self.analysis_exporter is None:
            return {
                "analysis_bundle_label": None,
                "analysis_bundle_root": None,
                "latest_analysis_bundle_path": None,
                "latest_analysis_bundle_exists": False,
                "latest_analysis_index_path": None,
                "latest_analysis_index_exists": False,
                "latest_analysis_report_path": None,
                "latest_analysis_report_exists": False,
                "last_analysis_bundle_path": None,
                "last_analysis_report_path": None,
                "last_analysis_bundle_error": None,
                "last_analysis_bundle_at": None,
                "analysis_bundle_export_in_progress": False,
                "analysis_bundle_export_started_at": None,
                "analysis_dropbox_enabled": False,
                "analysis_dropbox_root": None,
                "analysis_dropbox_configuration_error": None,
                "last_analysis_bundle_dropbox_path": None,
                "last_analysis_bundle_dropbox_url": None,
                "last_analysis_index_dropbox_path": None,
                "last_analysis_index_dropbox_url": None,
                "last_analysis_report_dropbox_path": None,
                "last_analysis_report_dropbox_url": None,
                "last_analysis_bundle_dropbox_error": None,
            }
        return dict(self.analysis_exporter.status())

    @staticmethod
    def _should_skip_refresh(action: str, status: int) -> bool:
        return int(status) == 202 and str(action or "") in {"scan_temperature", "scan_precipitation", "export_analysis_bundle"}

    @staticmethod
    def control_plane_request(payload: dict[str, Any]):
        from .control_plane import ControlRequest

        return ControlRequest.from_payload(payload)


def _execution_scoreboard_summary(
    *,
    paper_stats: dict[str, Any],
    shadow_execution_summary: dict[str, Any],
) -> dict[str, Any]:
    """Money-like scoreboard derived from realistic shadow execution."""

    paper_signal_pnl = float(
        shadow_execution_summary.get(
            "paper_signal_total_pnl",
            shadow_execution_summary.get("paper_total_pnl", paper_stats.get("total_pnl", 0.0)),
        )
        or 0.0
    )
    scoreboard_pnl = float(
        shadow_execution_summary.get(
            "scoreboard_pnl",
            shadow_execution_summary.get("realistic_total_pnl", 0.0),
        )
        or 0.0
    )
    entry_orders = int(shadow_execution_summary.get("entry_order_count") or 0)
    entry_fills = int(shadow_execution_summary.get("realistic_entry_fill_count") or 0)
    entry_fill_rate = float(shadow_execution_summary.get("entry_fill_rate") or 0.0)
    if entry_orders <= 0:
        status = "awaiting_shadow_orders"
        status_label = "AWAITING ORDERS"
    elif entry_fills <= 0:
        status = "no_real_entry_fills"
        status_label = "NO REAL FILLS"
    elif int(shadow_execution_summary.get("open_order_count") or 0) > 0:
        status = "tracking_live_orders"
        status_label = "TRACKING LIVE ORDERS"
    else:
        status = "tracking_executable_pnl"
        status_label = "TRACKING EXEC P/L"
    return {
        "scoreboard_label": "Executable Shadow P/L",
        "scoreboard_pnl": round(scoreboard_pnl, 6),
        "paper_signal_label": "Paper Signal P/L",
        "paper_signal_pnl": round(paper_signal_pnl, 6),
        "signal_vs_execution_gap": round(paper_signal_pnl - scoreboard_pnl, 6),
        "execution_vs_signal_gap": round(scoreboard_pnl - paper_signal_pnl, 6),
        "entry_order_count": entry_orders,
        "entry_fill_count": entry_fills,
        "entry_fill_rate": round(entry_fill_rate, 2),
        "open_exposure": float(shadow_execution_summary.get("open_exposure") or 0.0),
        "missed_paper_pnl": float(shadow_execution_summary.get("missed_paper_pnl") or 0.0),
        "status": status,
        "status_label": status_label,
    }
