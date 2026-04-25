"""Operator control surface shared by HA and Telegram."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ControlRequest:
    action: str
    value: Any = None

    @classmethod
    def from_payload(cls, payload: dict[str, Any] | None) -> "ControlRequest":
        payload = payload or {}
        return cls(str(payload.get("action") or "").strip().lower(), payload.get("value"))


@dataclass
class ControlResult:
    ok: bool
    status: int
    message: str
    action: str
    state: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "ok": bool(self.ok),
            "status": int(self.status),
            "message": str(self.message),
        }
        if self.state is not None:
            payload["state"] = self.state
        return payload


class ControlPlane:
    def __init__(self, runtime, tracker, *, codex_manager=None):
        self.runtime = runtime
        self.tracker = tracker
        self.codex_manager = codex_manager
        self._last_control = {"action": None, "ok": None, "message": "Operator link ready."}

    def last_control(self) -> dict[str, Any]:
        return dict(self._last_control)

    def build_controls_payload(self) -> dict[str, Any]:
        runtime_status = self.runtime.get_status_snapshot()
        paper = self.tracker.get_paper_stats()
        return {
            "state": runtime_status.get("state", "unknown"),
            "temperature_enabled": runtime_status.get("temperature_enabled", True),
            "precipitation_enabled": runtime_status.get("precipitation_enabled", True),
            "paper_auto_trade": runtime_status.get("paper_auto_trade", True),
            "scan_in_progress": runtime_status.get("scan_in_progress", False),
            "scan_queue_depth": runtime_status.get("scan_queue_depth", 0),
            "pending_scan_types": runtime_status.get("pending_scan_types", []),
            "active_scan_type": runtime_status.get("active_scan_type"),
            "scan_worker_healthy": runtime_status.get("scan_worker_healthy", False),
            "last_scan_worker_error": runtime_status.get("last_scan_worker_error"),
            "last_scan_export_error": runtime_status.get("last_scan_export_error"),
            "open_position_review_in_progress": runtime_status.get("open_position_review_in_progress", False),
            "last_open_position_review_at": runtime_status.get("last_open_position_review_at"),
            "last_open_position_review_status": runtime_status.get("last_open_position_review_status"),
            "last_open_position_review_error": runtime_status.get("last_open_position_review_error"),
            "last_open_position_review_reason": runtime_status.get("last_open_position_review_reason"),
            "last_open_position_review_count": runtime_status.get("last_open_position_review_count", 0),
            "last_open_position_close_count": runtime_status.get("last_open_position_close_count", 0),
            "paper_balance": paper.get("current_balance", 0.0),
            "paper_equity": paper.get("current_equity", 0.0),
            "paper_initial_capital": paper.get("initial_capital", 0.0),
            "paper_max_open_positions": runtime_status.get("paper_max_open_positions", getattr(self.runtime.strategy_engine, "paper_max_open_positions", 0)),
            "paper_open_positions": paper.get("open_positions", 0),
            "available_actions": {
                "start": True,
                "stop": True,
                "scan_temperature": True,
                "scan_precipitation": True,
                "set_paper_capital": True,
                "set_paper_max_open_positions": True,
                "close_position": True,
                "toggle_temperature": True,
                "toggle_precipitation": True,
                "toggle_paper_auto_trade": True,
                "research_run_now": self.codex_manager is not None,
                "tuner_run_now": self.codex_manager is not None,
                "tuner_promote_latest": self.codex_manager is not None,
                "tuner_reject_latest": self.codex_manager is not None,
            },
            "last_action": self._last_control.get("action"),
            "last_message": self._last_control.get("message"),
            "last_ok": self._last_control.get("ok"),
        }

    def apply_sync(self, request: ControlRequest) -> ControlResult:
        action = request.action
        if not action:
            return self._record(
                ControlResult(
                    False,
                    400,
                    "Dashboard command was empty. Reload the dashboard and try again.",
                    action,
                )
            )
        if action == "start":
            self.runtime.resume()
            return self._record(ControlResult(True, 200, "Automation resumed.", action))
        if action == "stop":
            self.runtime.pause()
            return self._record(ControlResult(True, 200, "Automation paused.", action))
        if action == "scan_temperature":
            queued = self.runtime.request_scan(
                "temperature",
                send_alerts=False,
                reason="operator",
                ignore_pause=True,
                ignore_enabled=True,
            )
            return self._record(
                ControlResult(bool(queued.get("ok")), 202, str(queued.get("message")), action),
            )
        if action == "scan_precipitation":
            queued = self.runtime.request_scan(
                "precipitation",
                send_alerts=False,
                reason="operator",
                ignore_pause=True,
                ignore_enabled=True,
            )
            return self._record(
                ControlResult(bool(queued.get("ok")), 202, str(queued.get("message")), action),
            )
        if action == "set_paper_capital":
            try:
                amount = float(request.value)
            except (TypeError, ValueError):
                return self._record(ControlResult(False, 400, "Paper capital must be numeric.", action))
            self.runtime.reset_paper_capital(amount)
            return self._record(ControlResult(True, 200, f"Paper capital reset to ${amount:.2f}.", action))
        if action == "set_paper_max_open_positions":
            try:
                amount = _coerce_int(request.value, keys=("limit", "value", "paper_max_open_positions", "max_open_positions"))
            except (TypeError, ValueError):
                return self._record(ControlResult(False, 400, "Open-position cap must be numeric.", action))
            limit = self.runtime.set_paper_max_open_positions(amount)
            return self._record(ControlResult(True, 200, f"Global open-position cap set to {limit}.", action))
        if action == "close_position":
            value = request.value if isinstance(request.value, dict) else {"position_id": request.value}
            try:
                position_id = int(value.get("position_id"))
            except (TypeError, ValueError, AttributeError):
                return self._record(ControlResult(False, 400, "A numeric paper position id is required.", action))
            reason = str(value.get("reason") or "manual_dashboard_sell")
            result = self.runtime.close_position(position_id, reason=reason)
            return self._record(ControlResult(bool(result.get("ok")), int(result.get("status", 200)), str(result.get("message")), action))
        if action == "toggle_temperature":
            enabled = self.runtime.set_temperature_enabled(_coerce_bool(request.value))
            state = "enabled" if enabled else "disabled"
            return self._record(ControlResult(True, 200, f"Temperature automation {state}.", action))
        if action == "toggle_precipitation":
            enabled = self.runtime.set_precipitation_enabled(_coerce_bool(request.value))
            state = "enabled" if enabled else "disabled"
            return self._record(ControlResult(True, 200, f"Precipitation automation {state}.", action))
        if action == "toggle_paper_auto_trade":
            enabled = self.runtime.set_paper_auto_trade(_coerce_bool(request.value))
            state = "enabled" if enabled else "disabled"
            return self._record(ControlResult(True, 200, f"Automatic paper trading {state}.", action))
        if self.codex_manager is not None:
            if action == "research_run_now":
                result = self.codex_manager.enqueue_daily_refresh(requested_by="operator")
                return self._record(ControlResult(bool(result.get("ok")), int(result.get("status", 200)), str(result.get("message")), action))
            if action == "tuner_run_now":
                result = self.codex_manager.enqueue_tuning(requested_by="operator")
                return self._record(ControlResult(bool(result.get("ok")), int(result.get("status", 200)), str(result.get("message")), action))
            if action == "tuner_promote_latest":
                result = self.codex_manager.promote_latest_candidate(requested_by="operator")
                return self._record(ControlResult(bool(result.get("ok")), int(result.get("status", 200)), str(result.get("message")), action))
            if action == "tuner_reject_latest":
                result = self.codex_manager.reject_latest_candidate(requested_by="operator")
                return self._record(ControlResult(bool(result.get("ok")), int(result.get("status", 200)), str(result.get("message")), action))
        return self._record(ControlResult(False, 400, f"Unknown action: {action or 'empty'}", action))

    def _record(self, result: ControlResult) -> ControlResult:
        self._last_control = {"action": result.action, "ok": result.ok, "message": result.message}
        self.tracker.record_operator_action(result.action, result.to_dict())
        return result


def _coerce_bool(value: Any) -> bool:
    return str(value).strip().lower() not in {"0", "false", "no", "off", ""}


def _coerce_int(value: Any, *, keys: tuple[str, ...] = ()) -> int:
    raw = value
    if isinstance(raw, dict):
        for key in keys:
            if key in raw:
                raw = raw.get(key)
                break
        else:
            raise ValueError("missing numeric value")
    if isinstance(raw, str):
        raw = raw.strip()
        if raw == "":
            raise ValueError("empty numeric value")
    return int(float(raw))
