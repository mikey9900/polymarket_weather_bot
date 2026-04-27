"""Operator control surface shared by HA and Telegram."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from .models import iso_now

OPEN_POSITION_CAP_KEYS = (
    "limit",
    "paper_max_open_positions",
    "max_open_positions",
    "open_position_cap",
    "openPositionCap",
    "paperMaxOpenPositions",
    "maxOpenPositions",
)
OPEN_POSITION_CAP_VALUE_KEYS = ("value", *OPEN_POSITION_CAP_KEYS)
TEMPERATURE_SCAN_INTERVAL_KEYS = (
    "temperature_scan_minutes",
    "temperature_scan_interval_minutes",
    "temp_scan_minutes",
    "temp_scan_interval_minutes",
    "scan_minutes",
    "minutes",
    "value",
)
PRECIPITATION_SCAN_INTERVAL_KEYS = (
    "precipitation_scan_minutes",
    "precipitation_scan_interval_minutes",
    "rain_scan_minutes",
    "rain_scan_interval_minutes",
    "scan_minutes",
    "minutes",
    "value",
)
TEMPERATURE_MARKET_SCOPE_KEYS = (
    "temperature_market_scope",
    "market_scope",
    "scope",
    "region",
    "temperatureMarketScope",
    "value",
)
ENTRY_EDGE_LIMIT_KEYS = (
    "edge_pct",
    "min_edge_abs_pct",
    "paper_entry_min_edge_abs_pct",
    "edge_limit",
    "entry_edge_limit",
    "paper_entry_min_edge_abs",
    "entryEdgeLimit",
    "paperEntryMinEdgeAbs",
)
ENTRY_EDGE_LIMIT_VALUE_KEYS = ("value", *ENTRY_EDGE_LIMIT_KEYS)


@dataclass(frozen=True)
class ControlRequest:
    action: str
    value: Any = None

    @classmethod
    def from_payload(cls, payload: dict[str, Any] | None) -> "ControlRequest":
        payload = payload or {}
        action = str(payload.get("action") or "").strip().lower()
        if not action:
            action = _infer_action_from_payload(payload)
        if "value" in payload:
            value = payload.get("value")
        else:
            value_payload = {key: value for key, value in payload.items() if key != "action"}
            value = value_payload or None
        return cls(action, value)


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
        self._last_control = {"action": None, "ok": None, "message": "Operator link ready.", "created_at": None}

    def last_control(self) -> dict[str, Any]:
        return dict(self._last_control)

    def build_controls_payload(self) -> dict[str, Any]:
        runtime_status = self.runtime.get_status_snapshot()
        paper = self.tracker.get_paper_stats()
        temperature_scan_interval_seconds = runtime_status.get(
            "auto_temperature_scan_interval_seconds",
            _scheduled_interval_seconds(
                getattr(self.runtime.config.app, "auto_temperature_scan_seconds", 0),
                getattr(self.runtime.config.app, "auto_temperature_scan_minutes", 120),
                minimum_seconds=5,
            ),
        )
        precipitation_scan_interval_seconds = runtime_status.get(
            "auto_precipitation_scan_interval_seconds",
            _scheduled_interval_seconds(
                getattr(self.runtime.config.app, "auto_precipitation_scan_seconds", 0),
                getattr(self.runtime.config.app, "auto_precipitation_scan_minutes", 360),
                minimum_seconds=5,
            ),
        )
        return {
            "state": runtime_status.get("state", "unknown"),
            "temperature_enabled": runtime_status.get("temperature_enabled", True),
            "temperature_market_scope": runtime_status.get("temperature_market_scope", "both"),
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
            "temperature_scan_interval_minutes": max(5, int(round(float(temperature_scan_interval_seconds) / 60.0))),
            "precipitation_scan_interval_minutes": max(5, int(round(float(precipitation_scan_interval_seconds) / 60.0))),
            "paper_balance": paper.get("current_balance", 0.0),
            "paper_equity": paper.get("current_equity", 0.0),
            "paper_initial_capital": paper.get("initial_capital", 0.0),
            "paper_max_open_positions": runtime_status.get("paper_max_open_positions", getattr(self.runtime.strategy_engine, "paper_max_open_positions", 0)),
            "paper_entry_min_edge_abs": runtime_status.get(
                "paper_entry_min_edge_abs",
                getattr(self.runtime.strategy_engine, "paper_entry_min_edge_abs", 0.0),
            ),
            "paper_open_positions": paper.get("open_positions", 0),
            "available_actions": {
                "start": True,
                "stop": True,
                "scan_temperature": True,
                "scan_precipitation": True,
                "set_temperature_scan_interval_minutes": True,
                "set_precipitation_scan_interval_minutes": True,
                "set_temperature_market_scope": True,
                "set_paper_capital": True,
                "set_paper_max_open_positions": True,
                "set_paper_entry_min_edge_abs": True,
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
            "last_action_at": self._last_control.get("created_at"),
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
        if action == "set_temperature_scan_interval_minutes":
            try:
                value = _coerce_mapping(
                    request.value,
                    fallback_key="temperature_scan_minutes",
                    nested_keys=("value", "payload", "data"),
                )
                minutes = _coerce_int(value, keys=TEMPERATURE_SCAN_INTERVAL_KEYS)
            except (TypeError, ValueError):
                return self._record(ControlResult(False, 400, "Temperature scan cadence must be numeric.", action))
            cadence = self.runtime.set_auto_temperature_scan_minutes(minutes)
            return self._record(
                ControlResult(
                    True,
                    200,
                    f"Temperature edge scan cadence set to every {cadence} minutes for future scheduled sweeps.",
                    action,
                )
            )
        if action == "set_temperature_market_scope":
            try:
                value = _coerce_mapping(
                    request.value,
                    fallback_key="temperature_market_scope",
                    nested_keys=("value", "payload", "data"),
                )
                raw_scope = _coerce_text(value, keys=TEMPERATURE_MARKET_SCOPE_KEYS)
            except (TypeError, ValueError):
                return self._record(ControlResult(False, 400, "Temperature market scope is required.", action))
            scope = self.runtime.set_temperature_market_scope(raw_scope)
            label = _temperature_market_scope_label(scope)
            return self._record(
                ControlResult(
                    True,
                    200,
                    f"Temperature market scope set to {label} for future temperature scans. Open positions keep reviewing normally.",
                    action,
                )
            )
        if action == "set_precipitation_scan_interval_minutes":
            try:
                value = _coerce_mapping(
                    request.value,
                    fallback_key="precipitation_scan_minutes",
                    nested_keys=("value", "payload", "data"),
                )
                minutes = _coerce_int(value, keys=PRECIPITATION_SCAN_INTERVAL_KEYS)
            except (TypeError, ValueError):
                return self._record(ControlResult(False, 400, "Precipitation scan cadence must be numeric.", action))
            cadence = self.runtime.set_auto_precipitation_scan_minutes(minutes)
            return self._record(
                ControlResult(
                    True,
                    200,
                    f"Precipitation edge scan cadence set to every {cadence} minutes for future scheduled sweeps.",
                    action,
                )
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
                value = _coerce_mapping(
                    request.value,
                    fallback_key="limit",
                    nested_keys=("value", "payload", "data"),
                )
                amount = _coerce_int(value, keys=OPEN_POSITION_CAP_VALUE_KEYS)
            except (TypeError, ValueError):
                return self._record(ControlResult(False, 400, "Open-position cap must be numeric.", action))
            limit = self.runtime.set_paper_max_open_positions(amount)
            return self._record(
                ControlResult(
                    True,
                    200,
                    f"Global open-position cap set to {limit}. Existing open positions stay open; this only gates future entries.",
                    action,
                )
            )
        if action == "set_paper_entry_min_edge_abs":
            try:
                value = _coerce_mapping(
                    request.value,
                    fallback_key="edge_pct",
                    nested_keys=("value", "payload", "data"),
                )
                floor = _coerce_percent_as_probability(value, keys=ENTRY_EDGE_LIMIT_VALUE_KEYS)
            except (TypeError, ValueError):
                return self._record(ControlResult(False, 400, "Entry edge floor must be numeric.", action))
            edge_floor = self.runtime.set_paper_entry_min_edge_abs(floor)
            return self._record(
                ControlResult(
                    True,
                    200,
                    f"Entry edge floor set to {edge_floor:.0%} for future entries only. Current open positions keep their existing exit rules.",
                    action,
                )
            )
        if action == "close_position":
            value = _coerce_mapping(
                request.value,
                fallback_key="position_id",
                nested_keys=("value", "payload", "data"),
            )
            try:
                position_id = _coerce_int(
                    value,
                    keys=(
                        "position_id",
                        "id",
                        "positionId",
                        "paper_position_id",
                        "open_position_id",
                        "value",
                    ),
                )
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
        created_at = iso_now()
        self._last_control = {"action": result.action, "ok": result.ok, "message": result.message, "created_at": created_at}
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
        if raw.startswith("{") and raw.endswith("}"):
            try:
                parsed = _coerce_int(json.loads(raw), keys=keys)
            except Exception as exc:  # pragma: no cover - defensive parsing fallback
                raise ValueError("invalid numeric value") from exc
            return parsed
    return int(float(raw))


def _coerce_mapping(
    value: Any,
    *,
    fallback_key: str,
    nested_keys: tuple[str, ...] = (),
) -> dict[str, Any]:
    raw = value
    while True:
        if isinstance(raw, dict):
            for key in nested_keys:
                nested = raw.get(key)
                if isinstance(nested, dict):
                    raw = nested
                    break
                if isinstance(nested, str):
                    nested_text = nested.strip()
                    if nested_text.startswith("{") and nested_text.endswith("}"):
                        try:
                            parsed = json.loads(nested_text)
                        except json.JSONDecodeError:
                            parsed = None
                        if isinstance(parsed, dict):
                            raw = parsed
                            break
            else:
                return raw
            continue
        if isinstance(raw, str):
            text = raw.strip()
            if text.startswith("{") and text.endswith("}"):
                parsed = json.loads(text)
                if isinstance(parsed, dict):
                    raw = parsed
                    continue
            return {fallback_key: raw}
        return {fallback_key: raw}


def _jsonish_mapping(value: Any) -> dict[str, Any] | None:
    raw = value
    if isinstance(raw, str):
        text = raw.strip()
        if text.startswith("{") and text.endswith("}"):
            try:
                raw = json.loads(text)
            except json.JSONDecodeError:
                return None
    return raw if isinstance(raw, dict) else None


def _infer_action_from_payload(payload: dict[str, Any]) -> str:
    sources: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        sources.append(payload)
    for key in ("value", "payload", "data"):
        mapping = _jsonish_mapping(payload.get(key)) if isinstance(payload, dict) else None
        if mapping is not None:
            sources.append(mapping)
            for nested_key in ("value", "payload", "data"):
                nested = _jsonish_mapping(mapping.get(nested_key))
                if nested is not None:
                    sources.append(nested)
    for source in sources:
        if any(key in source for key in ("temperature_scan_minutes", "temperature_scan_interval_minutes", "temp_scan_minutes", "temp_scan_interval_minutes")):
            return "set_temperature_scan_interval_minutes"
        if any(key in source for key in TEMPERATURE_MARKET_SCOPE_KEYS[:-1]):
            return "set_temperature_market_scope"
        if any(key in source for key in ("precipitation_scan_minutes", "precipitation_scan_interval_minutes", "rain_scan_minutes", "rain_scan_interval_minutes")):
            return "set_precipitation_scan_interval_minutes"
        if any(key in source for key in ENTRY_EDGE_LIMIT_KEYS):
            return "set_paper_entry_min_edge_abs"
        if any(key in source for key in OPEN_POSITION_CAP_KEYS):
            return "set_paper_max_open_positions"
        if any(key in source for key in ("position_id", "positionId", "paper_position_id", "open_position_id")):
            return "close_position"
        if "id" in source and any(key in source for key in ("reason", "position", "position_ref")):
            return "close_position"
        if any(key in source for key in ("capital", "paper_capital", "paper_initial_capital")):
            return "set_paper_capital"
    return ""


def _coerce_text(value: Any, *, keys: tuple[str, ...] = ()) -> str:
    raw = value
    if isinstance(raw, dict):
        for key in keys:
            if key in raw:
                raw = raw.get(key)
                break
        else:
            raise ValueError("missing text value")
    text = str(raw or "").strip()
    if not text:
        raise ValueError("empty text value")
    return text


def _temperature_market_scope_label(value: Any) -> str:
    scope = str(value or "").strip().lower()
    if scope == "north_america":
        return "North America"
    if scope == "international":
        return "International"
    return "Both"


def _coerce_percent_as_probability(value: Any, *, keys: tuple[str, ...] = ()) -> float:
    raw = value
    if isinstance(raw, dict):
        for key in keys:
            if key in raw:
                raw = raw.get(key)
                break
        else:
            raise ValueError("missing percent value")
    if isinstance(raw, str):
        raw = raw.strip()
        if raw == "":
            raise ValueError("empty percent value")
        if raw.startswith("{") and raw.endswith("}"):
            try:
                return _coerce_percent_as_probability(json.loads(raw), keys=keys)
            except Exception as exc:  # pragma: no cover - defensive parsing fallback
                raise ValueError("invalid percent value") from exc
        if raw.endswith("%"):
            raw = raw[:-1].strip()
    amount = float(raw)
    if amount > 1.0:
        amount = amount / 100.0
    return float(amount)


def _scheduled_interval_seconds(seconds_value: Any, minutes_value: Any, *, minimum_seconds: int) -> int:
    try:
        seconds = int(seconds_value or 0)
    except (TypeError, ValueError):
        seconds = 0
    if seconds > 0:
        return max(int(minimum_seconds), seconds)
    try:
        minutes = int(minutes_value or 0)
    except (TypeError, ValueError):
        minutes = 0
    if minutes > 0:
        return max(int(minimum_seconds), minutes * 60)
    return int(minimum_seconds)
