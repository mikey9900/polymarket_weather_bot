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
            "paper_balance": paper.get("current_balance", 0.0),
            "paper_equity": paper.get("current_equity", 0.0),
            "paper_initial_capital": paper.get("initial_capital", 0.0),
            "paper_open_positions": paper.get("open_positions", 0),
            "available_actions": {
                "start": True,
                "stop": True,
                "scan_temperature": True,
                "scan_precipitation": True,
                "set_paper_capital": True,
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
        if action == "start":
            self.runtime.resume()
            return self._record(ControlResult(True, 200, "Automation resumed.", action))
        if action == "stop":
            self.runtime.pause()
            return self._record(ControlResult(True, 200, "Automation paused.", action))
        if action == "scan_temperature":
            batch, results = self.runtime.run_temperature_scan(send_alerts=False)
            return self._record(
                ControlResult(True, 200, f"Temperature scan completed with {len(batch.signals)} signals.", action, {"opened": sum(1 for item in results if item.position)}),
            )
        if action == "scan_precipitation":
            batch, results = self.runtime.run_precipitation_scan(send_alerts=False)
            return self._record(
                ControlResult(True, 200, f"Precipitation scan completed with {len(batch.signals)} signals.", action, {"opened": sum(1 for item in results if item.position)}),
            )
        if action == "set_paper_capital":
            try:
                amount = float(request.value)
            except (TypeError, ValueError):
                return self._record(ControlResult(False, 400, "Paper capital must be numeric.", action))
            self.runtime.reset_paper_capital(amount)
            return self._record(ControlResult(True, 200, f"Paper capital reset to ${amount:.2f}.", action))
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
