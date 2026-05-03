"""Weather runtime loops and orchestration."""

from __future__ import annotations

import inspect
import json
import logging
import threading
import time
from collections import deque
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

from forecast.forecast_engine import get_both_bucket_probabilities
from logic.discrepancy_logic import find_discrepancies
from polymarket.polymarket_prices import get_yes_price
from precipitation.precip_forecast import calc_precip_bucket_probs, get_om_monthly_precip, get_vc_monthly_precip
from precipitation.precip_parser import parse_precip_bucket
from parser.weather_parser import parse_temperature_bucket
from scanner.weather_event_scanner import normalize_temperature_market_scope

from .execution import execution_mode_records_shadow_orders, normalize_execution_mode
from .messages import format_resolution_message, format_scan_summary, format_signal_message
from .models import ResolutionOutcome, ScanBatch, WeatherSignal
from .precipitation_signals import _build_precip_signal, scan_precipitation_signals
from .temperature import _build_temperature_signal, scan_temperature_signals


logger = logging.getLogger(__name__)
TEMPERATURE_FORECAST_KEYS = ("wu", "openmeteo", "vc", "noaa", "weatherapi")


class WeatherRuntime:
    def __init__(
        self,
        *,
        config,
        tracker,
        strategy_engine,
        telegram,
        temperature_scanner=scan_temperature_signals,
        precipitation_scanner=scan_precipitation_signals,
        resolution_fetcher=None,
        price_fetcher=None,
        scan_export_root: str | Path | None = None,
    ):
        self.config = config
        self.tracker = tracker
        self.strategy_engine = strategy_engine
        self.telegram = telegram
        self.temperature_scanner = temperature_scanner
        self.precipitation_scanner = precipitation_scanner
        self.resolution_fetcher = resolution_fetcher or get_market_resolution
        self.price_fetcher = price_fetcher or get_yes_price
        self.scan_export_root = Path(scan_export_root) if scan_export_root else None
        scan_export_error = None
        if self.scan_export_root is not None:
            try:
                self.scan_export_root.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                scan_export_error = str(exc)
                self.scan_export_root = None
        self._state_lock = threading.RLock()
        self._scan_lock = threading.RLock()
        self._queue_condition = threading.Condition()
        self._scan_queue: deque[dict[str, Any]] = deque()
        self._stop_event = threading.Event()
        self._threads: list[threading.Thread] = []
        self._open_position_weather_cache: dict[str, dict[str, Any]] = {}
        self._next_scheduled_scan_at: dict[str, datetime | None] = {
            "temperature": None,
            "precipitation": None,
        }
        default_state = {
            "state": "running",
            "temperature_enabled": bool(self.config.temperature.enabled),
            "temperature_market_scope": normalize_temperature_market_scope(
                getattr(self.config.temperature, "market_scope", "both")
            ),
            "precipitation_enabled": bool(self.config.precipitation.enabled),
            "paper_auto_trade": True,
            "paper_max_open_positions": int(getattr(self.strategy_engine, "paper_max_open_positions", self.config.paper.max_open_positions)),
            "paper_execution_mode": normalize_execution_mode(
                getattr(self.strategy_engine, "paper_execution_mode", getattr(self.config.paper, "execution_mode", "paper"))
            ),
            "auto_temperature_scan_interval_seconds": self._configured_temperature_scan_interval_seconds(),
            "auto_precipitation_scan_interval_seconds": self._configured_precipitation_scan_interval_seconds(),
            "paper_entry_min_edge_abs": float(
                getattr(self.strategy_engine, "paper_entry_min_edge_abs", self.config.strategy.temperature.min_edge_abs)
            ),
            "paper_entry_min_edge_abs_override": getattr(self.strategy_engine, "paper_entry_min_edge_abs_override", None),
            "paper_temperature_max_no_entry_price": getattr(
                self.strategy_engine,
                "paper_temperature_max_no_entry_price",
                getattr(self.config.strategy.temperature, "max_no_entry_price", None),
            ),
            "paper_temperature_max_no_entry_price_override": getattr(
                self.strategy_engine,
                "paper_temperature_max_no_entry_price_override",
                None,
            ),
            "paper_temperature_no_stop_loss_pnl": getattr(
                self.strategy_engine,
                "paper_temperature_no_stop_loss_pnl",
                getattr(self.config.strategy.temperature, "no_stop_loss_pnl", None),
            ),
            "paper_temperature_no_stop_loss_min_entry_price": getattr(
                self.strategy_engine,
                "paper_temperature_no_stop_loss_min_entry_price",
                getattr(self.config.strategy.temperature, "no_stop_loss_min_entry_price", None),
            ),
            "scan_in_progress": False,
            "scan_queue_depth": 0,
            "pending_scan_types": [],
            "active_scan_type": None,
            "active_scan_started_at": None,
            "scan_worker_healthy": False,
            "last_scan_worker_error": None,
            "last_scan_export_error": scan_export_error,
            "last_temperature_scan_at": None,
            "last_precipitation_scan_at": None,
            "last_temperature_signal_count": 0,
            "last_precipitation_signal_count": 0,
            "last_temperature_scan_status": None,
            "last_precipitation_scan_status": None,
            "last_temperature_scan_duration_ms": None,
            "last_precipitation_scan_duration_ms": None,
            "last_temperature_scan_reason": None,
            "last_precipitation_scan_reason": None,
            "last_temperature_scan_error": None,
            "last_precipitation_scan_error": None,
            "last_temperature_error_count": 0,
            "last_precipitation_error_count": 0,
            "open_position_review_in_progress": False,
            "last_open_position_review_at": None,
            "last_open_position_review_status": None,
            "last_open_position_review_error": None,
            "last_open_position_review_reason": None,
            "last_open_position_review_count": 0,
            "last_open_position_close_count": 0,
            "open_position_weather_refresh_interval_seconds": self._open_position_weather_refresh_interval_seconds(),
            "last_resolution_check_at": None,
            "last_resolution_error": None,
        }
        saved_state = self.tracker.get_runtime_state("runtime_status", default=default_state)
        self._state = {**default_state, **saved_state}
        self._state["temperature_market_scope"] = normalize_temperature_market_scope(
            self._state.get("temperature_market_scope") or getattr(self.config.temperature, "market_scope", "both")
        )
        self._state["precipitation_enabled"] = bool(self.config.precipitation.enabled)
        self._state["open_position_weather_refresh_interval_seconds"] = self._open_position_weather_refresh_interval_seconds()
        self._state["auto_temperature_scan_interval_seconds"] = _normalize_scan_interval_seconds(
            self._state.get("auto_temperature_scan_interval_seconds"),
            default_seconds=self._configured_temperature_scan_interval_seconds(),
        )
        self._state["auto_precipitation_scan_interval_seconds"] = _normalize_scan_interval_seconds(
            self._state.get("auto_precipitation_scan_interval_seconds"),
            default_seconds=self._configured_precipitation_scan_interval_seconds(),
        )
        if hasattr(self.strategy_engine, "set_paper_max_open_positions"):
            limit = self.strategy_engine.set_paper_max_open_positions(int(self._state.get("paper_max_open_positions") or self.config.paper.max_open_positions))
            self._state["paper_max_open_positions"] = int(limit)
        if hasattr(self.strategy_engine, "set_paper_execution_mode"):
            self._state["paper_execution_mode"] = self.strategy_engine.set_paper_execution_mode(
                self._state.get("paper_execution_mode") or getattr(self.config.paper, "execution_mode", "paper")
            )
        else:
            self._state["paper_execution_mode"] = normalize_execution_mode(
                self._state.get("paper_execution_mode") or getattr(self.config.paper, "execution_mode", "paper")
            )
        edge_override = self._state.get("paper_entry_min_edge_abs_override")
        if edge_override is not None and hasattr(self.strategy_engine, "set_paper_entry_min_edge_abs"):
            edge_floor = self.strategy_engine.set_paper_entry_min_edge_abs(float(edge_override))
            self._state["paper_entry_min_edge_abs"] = float(edge_floor)
            self._state["paper_entry_min_edge_abs_override"] = float(edge_floor)
        else:
            self._state["paper_entry_min_edge_abs"] = float(
                getattr(self.strategy_engine, "paper_entry_min_edge_abs", self.config.strategy.temperature.min_edge_abs)
            )
            self._state["paper_entry_min_edge_abs_override"] = None
        no_entry_cap_override = self._state.get("paper_temperature_max_no_entry_price_override")
        if hasattr(self.strategy_engine, "set_paper_temperature_max_no_entry_price"):
            self.strategy_engine.set_paper_temperature_max_no_entry_price(no_entry_cap_override)
            self._state["paper_temperature_max_no_entry_price"] = getattr(
                self.strategy_engine,
                "paper_temperature_max_no_entry_price",
                getattr(self.config.strategy.temperature, "max_no_entry_price", None),
            )
            self._state["paper_temperature_max_no_entry_price_override"] = getattr(
                self.strategy_engine,
                "paper_temperature_max_no_entry_price_override",
                None,
            )
        else:
            self._state["paper_temperature_max_no_entry_price"] = getattr(
                self.strategy_engine,
                "paper_temperature_max_no_entry_price",
                getattr(self.config.strategy.temperature, "max_no_entry_price", None),
            )
            self._state["paper_temperature_max_no_entry_price_override"] = None
        self._state["paper_temperature_no_stop_loss_pnl"] = getattr(
            self.strategy_engine,
            "paper_temperature_no_stop_loss_pnl",
            getattr(self.config.strategy.temperature, "no_stop_loss_pnl", None),
        )
        self._state["paper_temperature_no_stop_loss_min_entry_price"] = getattr(
            self.strategy_engine,
            "paper_temperature_no_stop_loss_min_entry_price",
            getattr(self.config.strategy.temperature, "no_stop_loss_min_entry_price", None),
        )
        if self._reconcile_boot_state():
            self.tracker.set_runtime_state("runtime_status", dict(self._state))
        self._prime_next_scheduled_scans()

    def start_background_loops(self) -> None:
        if self._threads:
            return
        self._stop_event.clear()
        loops = [
            (self._scan_worker_loop, "weather-scan-worker"),
            (self._temperature_loop, "weather-temp-loop"),
            (self._precipitation_loop, "weather-precip-loop"),
            (self._open_position_review_loop, "weather-position-review"),
            (self._resolution_loop, "weather-resolution-loop"),
        ]
        for target, name in loops:
            thread = threading.Thread(target=target, name=name, daemon=True)
            thread.start()
            self._threads.append(thread)

    def stop_background_loops(self) -> None:
        self._stop_event.set()
        with self._queue_condition:
            self._queue_condition.notify_all()
        for thread in self._threads:
            thread.join(timeout=5.0)
        self._threads.clear()
        self._set_next_scheduled_scan("temperature", None)
        self._set_next_scheduled_scan("precipitation", None)
        self._update_state(scan_worker_healthy=False)

    def pause(self) -> None:
        self._update_state(state="paused")

    def resume(self) -> None:
        self._update_state(state="running")

    def reset_paper_capital(self, amount: float) -> None:
        self.tracker.set_paper_capital(amount)

    def set_temperature_enabled(self, enabled: bool) -> bool:
        self._update_state(temperature_enabled=bool(enabled))
        return bool(enabled)

    def set_temperature_market_scope(self, value: str) -> str:
        scope = normalize_temperature_market_scope(value)
        self._update_state(temperature_market_scope=scope)
        return scope

    def set_precipitation_enabled(self, enabled: bool) -> bool:
        self._update_state(precipitation_enabled=bool(enabled))
        return bool(enabled)

    def set_paper_auto_trade(self, enabled: bool) -> bool:
        self._update_state(paper_auto_trade=bool(enabled))
        return bool(enabled)

    def set_paper_execution_mode(self, value: str) -> str:
        mode = normalize_execution_mode(value)
        if hasattr(self.strategy_engine, "set_paper_execution_mode"):
            mode = str(self.strategy_engine.set_paper_execution_mode(mode))
        self._update_state(paper_execution_mode=mode)
        return str(mode)

    def set_paper_max_open_positions(self, value: int) -> int:
        limit = max(1, min(100, int(value)))
        if hasattr(self.strategy_engine, "set_paper_max_open_positions"):
            limit = int(self.strategy_engine.set_paper_max_open_positions(limit))
        self._update_state(paper_max_open_positions=limit)
        return limit

    def set_paper_entry_min_edge_abs(self, value: float) -> float:
        floor = max(0.05, min(0.40, float(value)))
        if hasattr(self.strategy_engine, "set_paper_entry_min_edge_abs"):
            floor = float(self.strategy_engine.set_paper_entry_min_edge_abs(floor))
        self._update_state(
            paper_entry_min_edge_abs=float(floor),
            paper_entry_min_edge_abs_override=float(floor),
        )
        return float(floor)

    def set_paper_temperature_max_no_entry_price(self, value: float | None) -> float | None:
        if hasattr(self.strategy_engine, "set_paper_temperature_max_no_entry_price"):
            self.strategy_engine.set_paper_temperature_max_no_entry_price(value)
            cap = getattr(self.strategy_engine, "paper_temperature_max_no_entry_price", None)
            override = getattr(self.strategy_engine, "paper_temperature_max_no_entry_price_override", None)
        else:
            raw = None if value is None else float(value)
            cap = None if raw is None or raw <= 0 else raw
            override = cap
        self._update_state(
            paper_temperature_max_no_entry_price=cap,
            paper_temperature_max_no_entry_price_override=override,
        )
        return cap

    def set_auto_temperature_scan_minutes(self, value: int) -> int:
        minutes = _normalize_scan_interval_minutes(value)
        self._update_state(auto_temperature_scan_interval_seconds=minutes * 60)
        self._set_next_scheduled_scan(
            "temperature",
            datetime.now(timezone.utc) + timedelta(seconds=minutes * 60),
        )
        return int(minutes)

    def set_auto_precipitation_scan_minutes(self, value: int) -> int:
        minutes = _normalize_scan_interval_minutes(value)
        self._update_state(auto_precipitation_scan_interval_seconds=minutes * 60)
        self._set_next_scheduled_scan(
            "precipitation",
            datetime.now(timezone.utc) + timedelta(seconds=minutes * 60),
        )
        return int(minutes)

    def _temperature_market_scope(self) -> str:
        return normalize_temperature_market_scope(
            self.get_status_snapshot().get("temperature_market_scope")
            or getattr(self.config.temperature, "market_scope", "both")
        )

    def _invoke_temperature_scanner(self, *, limit: int) -> ScanBatch:
        scanner = self.temperature_scanner
        if _callable_accepts_keyword(scanner, "market_scope"):
            return scanner(limit=limit, market_scope=self._temperature_market_scope())
        return scanner(limit=limit)

    def get_status_snapshot(self) -> dict:
        with self._state_lock:
            return dict(self._state)

    def get_next_scheduled_scan_at(self, scan_type: str) -> str | None:
        with self._state_lock:
            due_at = self._next_scheduled_scan_at.get(str(scan_type or "").strip().lower())
        if due_at is None:
            return None
        return due_at.isoformat()

    def request_scan(
        self,
        scan_type: str,
        *,
        send_alerts: bool = True,
        reason: str = "operator",
        limit: int | None = None,
        ignore_pause: bool = False,
        ignore_enabled: bool = False,
    ) -> dict[str, Any]:
        scan_type = str(scan_type or "").strip().lower()
        if scan_type not in {"temperature", "precipitation"}:
            return {"ok": False, "queued": False, "message": f"Unknown scan type: {scan_type or 'empty'}"}
        with self._queue_condition:
            snapshot = self.get_status_snapshot()
            active_type = str(snapshot.get("active_scan_type") or "")
            active_scan_matches = bool(snapshot.get("scan_in_progress")) and active_type == scan_type
            pending_types = [str(job.get("scan_type") or "") for job in self._scan_queue]
            if active_scan_matches or scan_type in pending_types:
                self._sync_queue_state_locked()
                return {"ok": True, "queued": False, "message": f"{scan_type.title()} scan already active or queued."}
            self._scan_queue.append(
                {
                    "scan_type": scan_type,
                    "send_alerts": bool(send_alerts),
                    "reason": str(reason or "operator"),
                    "limit": limit,
                    "ignore_pause": bool(ignore_pause),
                    "ignore_enabled": bool(ignore_enabled),
                }
            )
            self._sync_queue_state_locked()
            self._queue_condition.notify()
        return {"ok": True, "queued": True, "message": f"{scan_type.title()} scan queued."}

    def run_temperature_scan(
        self,
        *,
        send_alerts: bool = True,
        limit: int | None = None,
        ignore_pause: bool = False,
        ignore_enabled: bool = False,
        reason: str = "manual",
    ) -> tuple[ScanBatch, list]:
        return self._run_scan(
            scan_type="temperature",
            enabled_key="temperature_enabled",
            scanner=lambda: self._invoke_temperature_scanner(limit=limit or self.config.temperature.scan_limit),
            auto_trade=self.config.temperature.auto_paper_trade,
            send_alerts=send_alerts,
            ignore_pause=ignore_pause,
            ignore_enabled=ignore_enabled,
            reason=reason,
        )

    def run_precipitation_scan(
        self,
        *,
        send_alerts: bool = True,
        ignore_pause: bool = False,
        ignore_enabled: bool = False,
        reason: str = "manual",
    ) -> tuple[ScanBatch, list]:
        return self._run_scan(
            scan_type="precipitation",
            enabled_key="precipitation_enabled",
            scanner=self.precipitation_scanner,
            auto_trade=self.config.precipitation.auto_paper_trade,
            send_alerts=send_alerts,
            ignore_pause=ignore_pause,
            ignore_enabled=ignore_enabled,
            reason=reason,
        )

    def settle_due_positions(self, *, send_alerts: bool = True) -> list[ResolutionOutcome]:
        outcomes: list[ResolutionOutcome] = []
        open_positions = self.tracker.get_open_positions()
        seen_markets: set[str] = set()
        for position in open_positions:
            market_slug = str(position.get("market_slug") or "")
            if not market_slug or market_slug in seen_markets:
                continue
            seen_markets.add(market_slug)
            resolution = self.resolution_fetcher(market_slug)
            if resolution is None:
                continue
            outcome = self.tracker.settle_market(market_slug, resolution)
            if outcome.resolved_positions:
                outcomes.append(outcome)
                if send_alerts and self.config.alerts.send_resolution_updates:
                    self.telegram.send_message(format_resolution_message(outcome))
        return outcomes

    def close_position(self, position_id: int, *, reason: str = "manual_dashboard_sell") -> dict[str, Any]:
        positions = {
            int(item["id"]): item
            for item in self.tracker.get_dashboard_paper_positions(
                limit=500,
                status="open",
                mark_stale_after_seconds=self.config.paper.mark_stale_after_seconds,
            )
        }
        position = positions.get(int(position_id))
        if position is None:
            return {"ok": False, "status": 404, "message": f"Open paper position {position_id} was not found."}
        mark_age_seconds = _as_float(position.get("mark_age_seconds"))
        fresh_mark: float | None = None
        if (
            position.get("mark_updated_at") in {None, ""}
            or (mark_age_seconds is not None and mark_age_seconds > float(self.config.paper.mark_stale_after_seconds))
        ):
            market_slug = str(position.get("market_slug") or "").strip()
            if market_slug:
                try:
                    fresh_mark = self.price_fetcher(market_slug)
                except Exception as exc:
                    logger.warning("manual close price refresh failed for %s: %s", market_slug, exc)
                    fresh_mark = None
        exit_price = _as_probability(
            fresh_mark
            if fresh_mark is not None
            else (position.get("mark_price") or position.get("market_probability") or position.get("entry_price"))
        )
        shadow_exit_intent = None
        if execution_mode_records_shadow_orders(self.get_status_snapshot().get("paper_execution_mode")):
            shadow_exit_intent = self.tracker.preview_shadow_exit_intent(
                int(position_id),
                execution_mode=str(self.get_status_snapshot().get("paper_execution_mode") or "paper"),
                exit_price=exit_price,
                reason=str(reason or "manual_dashboard_sell"),
                reason_code=str(reason or "manual_dashboard_sell"),
                decision_final_score=_as_float(position.get("mark_final_score") or position.get("decision_final_score")),
                exit_fee_bps=self.config.paper.fee_bps,
                exit_slippage_bps=self.config.paper.exit_slippage_bps,
            )
        result = self.tracker.close_paper_position(
            int(position_id),
            exit_price=exit_price,
            reason=str(reason or "manual_dashboard_sell"),
            mark_probability=_as_probability(position.get("mark_probability") or position.get("outcome_probability")),
            edge_abs=_as_float(position.get("mark_edge_abs") or position.get("edge_abs")),
            final_score=_as_float(position.get("mark_final_score") or position.get("decision_final_score")),
            mark_reason=str(position.get("mark_reason") or "Manual dashboard sell."),
            exit_fee_bps=self.config.paper.fee_bps,
            exit_slippage_bps=self.config.paper.exit_slippage_bps,
            reason_code=str(reason or "manual_dashboard_sell"),
        )
        if result is None:
            return {"ok": False, "status": 404, "message": f"Open paper position {position_id} was not found."}
        if shadow_exit_intent is not None:
            self.tracker.record_shadow_order_intent(replace(shadow_exit_intent, status="mirrored"))
        return {
            "ok": True,
            "status": 200,
            "message": f"Closed paper position {position_id} at {float(result['exit_price']):.1%}.",
            "position": result,
        }

    def review_open_positions(
        self,
        *,
        reason: str = "scheduled_review",
        market_types: set[str] | None = None,
    ) -> dict[str, Any]:
        with self._scan_lock:
            positions = self._open_positions_for_review(market_types)
            if not positions:
                self._set_open_position_review_state(
                    status="idle",
                    reason=reason,
                    reviewed_count=0,
                    closed_count=0,
                    in_progress=False,
                )
                return {"ok": True, "reviewed": 0, "closed": 0, "reason": reason}

            self._update_state(open_position_review_in_progress=True)
            try:
                self.settle_due_positions(send_alerts=False)
                active_positions = self._open_positions_for_review(market_types)
                summary = self._review_market_type_groups(active_positions, reason=reason)
                self._set_open_position_review_state(
                    status="completed",
                    reason=reason,
                    reviewed_count=int(summary["reviewed"]),
                    closed_count=int(summary["closed"]),
                    in_progress=False,
                )
                return {"ok": True, "reviewed": int(summary["reviewed"]), "closed": int(summary["closed"]), "reason": reason}
            except Exception as exc:
                self._set_open_position_review_state(
                    status="failed",
                    reason=reason,
                    error=str(exc),
                    in_progress=False,
                )
                raise

    def wait_for_idle(self, timeout: float = 10.0) -> bool:
        deadline = time.monotonic() + max(0.1, float(timeout))
        while time.monotonic() < deadline:
            snapshot = self.get_status_snapshot()
            if not snapshot.get("scan_in_progress") and int(snapshot.get("scan_queue_depth") or 0) == 0:
                return True
            time.sleep(0.05)
        return False

    def _scan_worker_loop(self) -> None:
        self._update_state(scan_worker_healthy=True, last_scan_worker_error=None)
        while True:
            with self._queue_condition:
                while not self._scan_queue and not self._stop_event.is_set():
                    self._queue_condition.wait(timeout=1.0)
                if self._stop_event.is_set() and not self._scan_queue:
                    return
                job = self._scan_queue.popleft()
                self._sync_queue_state_locked()
            try:
                if job["scan_type"] == "temperature":
                    self.run_temperature_scan(
                        send_alerts=bool(job["send_alerts"]),
                        limit=job.get("limit"),
                        ignore_pause=bool(job["ignore_pause"]),
                        ignore_enabled=bool(job["ignore_enabled"]),
                        reason=str(job.get("reason") or "queued"),
                    )
                else:
                    self.run_precipitation_scan(
                        send_alerts=bool(job["send_alerts"]),
                        ignore_pause=bool(job["ignore_pause"]),
                        ignore_enabled=bool(job["ignore_enabled"]),
                        reason=str(job.get("reason") or "queued"),
                    )
                self._update_state(scan_worker_healthy=True, last_scan_worker_error=None)
            except Exception as exc:  # pragma: no cover - guarded by scan error handling
                self._update_state(scan_worker_healthy=True, last_scan_worker_error=str(exc))

    def _run_scan(
        self,
        *,
        scan_type: str,
        enabled_key: str,
        scanner,
        auto_trade: bool,
        send_alerts: bool,
        ignore_pause: bool,
        ignore_enabled: bool,
        reason: str,
    ) -> tuple[ScanBatch, list]:
        timestamp = datetime.now(timezone.utc).isoformat()
        with self._scan_lock:
            state = self.get_status_snapshot()
            if not ignore_pause and state.get("state") == "paused":
                return self._empty_batch(scan_type), []
            if not ignore_enabled and not state.get(enabled_key, True):
                return self._empty_batch(scan_type), []

            scan_started_monotonic = time.monotonic()
            status_fields = _scan_status_fields(scan_type)
            self._update_state(
                scan_in_progress=True,
                active_scan_type=scan_type,
                active_scan_started_at=timestamp,
                **{
                    status_fields["reason"]: str(reason or "manual"),
                    status_fields["status"]: "running",
                    status_fields["error"]: None,
                },
            )
            try:
                settled = self.settle_due_positions(send_alerts=send_alerts)
                batch = scanner()
                self._store_open_position_weather_batch(scan_type, batch)
                results = self.strategy_engine.process_signals(
                    batch.signals,
                    auto_trade_enabled=bool(self.get_status_snapshot().get("paper_auto_trade", True) and auto_trade),
                )
                accepted = sum(1 for item in results if item.decision.accepted)
                opened = sum(1 for item in results if item.position is not None)
                review_summary = self._review_positions_for_signals(
                    scan_type=scan_type,
                    signals=batch.signals,
                    trigger=f"scan:{reason or scan_type}",
                    allow_close_on_missing_signal=int(batch.error_count or 0) == 0,
                )
                if send_alerts and self.config.alerts.telegram_enabled and self.config.alerts.send_scan_summary:
                    self.telegram.send_message(
                        format_scan_summary(
                            batch,
                            accepted_count=accepted,
                            opened_count=opened,
                            settled_count=sum(item.resolved_positions for item in settled),
                        )
                    )
                    for result in results:
                        if result.position is not None and self.config.alerts.send_paper_entries:
                            self.telegram.send_message(format_signal_message(result.signal))
                duration_ms = int(round((time.monotonic() - scan_started_monotonic) * 1000))
                finished_at = datetime.now(timezone.utc).isoformat()
                self._update_state(
                    scan_in_progress=False,
                    active_scan_type=None,
                    active_scan_started_at=None,
                    **{
                        status_fields["at"]: finished_at,
                        status_fields["count"]: len(batch.signals),
                        status_fields["status"]: "completed",
                        status_fields["duration_ms"]: duration_ms,
                        status_fields["error"]: None,
                        status_fields["error_count"]: int(batch.error_count),
                        "last_open_position_review_at": finished_at,
                        "last_open_position_review_status": "completed",
                        "last_open_position_review_error": None,
                        "last_open_position_review_reason": f"scan:{reason or scan_type}",
                        "last_open_position_review_count": int(review_summary["reviewed"]),
                        "last_open_position_close_count": int(review_summary["closed"]),
                    },
                )
                self._write_scan_export(
                    scan_type=scan_type,
                    status="completed",
                    reason=reason,
                    duration_ms=duration_ms,
                    batch=batch,
                    accepted_count=accepted,
                    opened_count=opened,
                    settled_count=sum(item.resolved_positions for item in settled),
                    error=None,
                )
                return batch, results
            except Exception as exc:
                duration_ms = int(round((time.monotonic() - scan_started_monotonic) * 1000))
                finished_at = datetime.now(timezone.utc).isoformat()
                self._update_state(
                    scan_in_progress=False,
                    active_scan_type=None,
                    active_scan_started_at=None,
                    **{
                        status_fields["at"]: finished_at,
                        status_fields["status"]: "failed",
                        status_fields["duration_ms"]: duration_ms,
                        status_fields["error"]: str(exc),
                    },
                )
                self._write_scan_export(
                    scan_type=scan_type,
                    status="failed",
                    reason=reason,
                    duration_ms=duration_ms,
                    batch=None,
                    accepted_count=0,
                    opened_count=0,
                    settled_count=0,
                    error=str(exc),
                )
                if send_alerts and self.config.alerts.telegram_enabled:
                    self.telegram.send_message(f"*{scan_type.title()} Scan Failed*\n`{exc}`")
                raise

    def _review_positions_for_signals(
        self,
        *,
        scan_type: str,
        signals: list[WeatherSignal],
        positions: list[dict[str, Any]] | None = None,
        trigger: str,
        allow_close_on_missing_signal: bool,
        refresh_market_prices: bool = True,
    ) -> dict[str, int]:
        if positions is None:
            positions = [
                item
                for item in self.tracker.get_dashboard_paper_positions(limit=500, status="open")
                if str(item.get("market_type") or "") == scan_type
            ]
        if not positions:
            return {"reviewed": 0, "closed": 0}

        signal_map = self._build_review_signal_map(
            signals=signals,
            positions=positions,
            refresh_market_prices=refresh_market_prices,
        )
        reviewed = 0
        closed = 0
        reviewed_at = datetime.now(timezone.utc).isoformat()

        for position in positions:
            market_slug = str(position.get("market_slug") or "")
            direction = str(position.get("direction") or "").upper()
            signal = signal_map.get((market_slug, direction))
            opposite_signal = signal_map.get((market_slug, "NO" if direction == "YES" else "YES"))
            decision = self.strategy_engine.evaluate_position_exit(
                position,
                signal=signal,
                opposite_signal=opposite_signal,
                allow_close_on_missing_signal=allow_close_on_missing_signal,
            )
            mark_price = _as_probability(decision.mark_price or position.get("mark_price") or position.get("entry_price"))
            mark_probability = _as_probability(decision.mark_probability or position.get("mark_probability") or position.get("outcome_probability"))
            self.tracker.update_paper_position_review(
                int(position["id"]),
                mark_price=mark_price,
                mark_probability=mark_probability,
                edge_abs=decision.edge_abs,
                final_score=decision.final_score,
                reviewed_at=reviewed_at,
                reason=f"{trigger}: {decision.reason}",
                reason_code=decision.reason_code,
                exit_fee_bps=self.config.paper.fee_bps,
                exit_slippage_bps=self.config.paper.exit_slippage_bps,
            )
            reviewed += 1
            if not decision.should_close:
                continue
            shadow_exit_intent = None
            if execution_mode_records_shadow_orders(self.get_status_snapshot().get("paper_execution_mode")):
                shadow_exit_intent = self.tracker.preview_shadow_exit_intent(
                    int(position["id"]),
                    execution_mode=str(self.get_status_snapshot().get("paper_execution_mode") or "paper"),
                    exit_price=mark_price,
                    reason=decision.reason,
                    reason_code=decision.reason_code,
                    decision_final_score=decision.final_score,
                    exit_fee_bps=self.config.paper.fee_bps,
                    exit_slippage_bps=self.config.paper.exit_slippage_bps,
                )
            result = self.tracker.close_paper_position(
                int(position["id"]),
                exit_price=mark_price,
                reason=decision.reason,
                closed_at=reviewed_at,
                mark_probability=mark_probability,
                edge_abs=decision.edge_abs,
                final_score=decision.final_score,
                mark_reason=f"{trigger}: {decision.reason}",
                reason_code=decision.reason_code,
            )
            if result is not None:
                if shadow_exit_intent is not None:
                    self.tracker.record_shadow_order_intent(replace(shadow_exit_intent, status="mirrored"))
                closed += 1

        return {"reviewed": reviewed, "closed": closed}

    def _open_position_weather_refresh_interval_seconds(self) -> int:
        minutes = int(getattr(self.config.app, "open_position_weather_refresh_minutes", 60) or 0)
        if minutes <= 0:
            return 0
        return max(60, minutes * 60)

    def _store_open_position_weather_batch(self, market_type: str, batch: ScanBatch) -> None:
        self._open_position_weather_cache[str(market_type or "")] = {
            "scope": "scan",
            "batch": batch,
            "refreshed_at_monotonic": time.monotonic(),
        }

    def _open_positions_for_review(self, market_types: set[str] | None = None) -> list[dict[str, Any]]:
        positions = self.tracker.get_dashboard_paper_positions(limit=500, status="open")
        return self._filter_positions_by_market_types(positions, market_types)

    def _filter_positions_by_market_types(
        self,
        positions: list[dict[str, Any]],
        market_types: set[str] | None,
    ) -> list[dict[str, Any]]:
        if not market_types:
            return positions
        allowed = {str(market_type or "") for market_type in market_types}
        return [item for item in positions if str(item.get("market_type") or "") in allowed]

    def _set_open_position_review_state(
        self,
        *,
        status: str,
        reason: str,
        reviewed_count: int = 0,
        closed_count: int = 0,
        error: str | None = None,
        in_progress: bool,
    ) -> None:
        reviewed_at = datetime.now(timezone.utc).isoformat()
        self._update_state(
            open_position_review_in_progress=bool(in_progress),
            last_open_position_review_at=reviewed_at,
            last_open_position_review_status=str(status or "idle"),
            last_open_position_review_error=error,
            last_open_position_review_reason=str(reason or "scheduled_review"),
            last_open_position_review_count=int(reviewed_count),
            last_open_position_close_count=int(closed_count),
        )

    def _review_market_type_groups(
        self,
        positions: list[dict[str, Any]],
        *,
        reason: str,
    ) -> dict[str, int]:
        reviewed_count = 0
        closed_count = 0
        for market_type, type_positions in self._positions_grouped_by_market_type(positions).items():
            summary = self._review_market_type_positions(
                market_type=market_type,
                positions=type_positions,
                reason=reason,
            )
            reviewed_count += int(summary["reviewed"])
            closed_count += int(summary["closed"])
        return {"reviewed": reviewed_count, "closed": closed_count}

    def _positions_grouped_by_market_type(
        self,
        positions: list[dict[str, Any]],
    ) -> dict[str, list[dict[str, Any]]]:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for position in positions:
            market_type = str(position.get("market_type") or "")
            grouped.setdefault(market_type, []).append(position)
        return dict(sorted(grouped.items()))

    def _review_market_type_positions(
        self,
        *,
        market_type: str,
        positions: list[dict[str, Any]],
        reason: str,
    ) -> dict[str, int]:
        batch, refresh_market_prices = self._get_review_weather_batch(market_type, positions)
        allow_missing_signal_close = int(batch.error_count or 0) == 0
        return self._review_positions_for_signals(
            scan_type=market_type,
            signals=batch.signals,
            positions=positions,
            trigger=reason,
            allow_close_on_missing_signal=allow_missing_signal_close,
            refresh_market_prices=refresh_market_prices,
        )

    def _get_review_weather_batch(
        self,
        market_type: str,
        positions: list[dict[str, Any]],
    ) -> tuple[ScanBatch, bool]:
        cache_key = str(market_type or "")
        if not positions:
            return self._empty_batch(cache_key), False

        cache_entry = self._open_position_weather_cache.get(cache_key)
        refresh_interval_s = self._open_position_weather_refresh_interval_seconds()
        now = time.monotonic()
        position_keys = self._review_position_keys(cache_key, positions)

        cached_payload_batch = self._cached_review_payload_batch(
            cache_key=cache_key,
            cache_entry=cache_entry,
            position_keys=position_keys,
            refresh_interval_s=refresh_interval_s,
            now=now,
        )
        if cached_payload_batch is not None:
            return cached_payload_batch, False

        try:
            payload = self._build_review_weather_payload(cache_key, positions)
            batch = self._build_review_batch_from_payload(cache_key, payload)
            self._store_review_weather_cache_entry(
                cache_key=cache_key,
                batch=batch,
                payload=payload,
                position_keys=position_keys,
                refreshed_at_monotonic=now,
            )
            return batch, False
        except Exception as exc:
            fallback = self._fallback_review_weather_batch(cache_key=cache_key, cache_entry=cache_entry, error=exc)
            if fallback is not None:
                return fallback
            raise

    def _cached_review_payload_batch(
        self,
        *,
        cache_key: str,
        cache_entry: dict[str, Any] | None,
        position_keys: tuple[tuple[Any, ...], ...],
        refresh_interval_s: int,
        now: float,
    ) -> ScanBatch | None:
        if (
            cache_entry is None
            or str(cache_entry.get("scope") or "") != "review"
            or tuple(cache_entry.get("position_keys") or ()) != position_keys
            or refresh_interval_s <= 0
        ):
            return None
        age_s = now - float(cache_entry.get("refreshed_at_monotonic") or 0.0)
        if age_s >= refresh_interval_s:
            return None
        payload = list(cache_entry.get("payload") or [])
        return self._build_review_batch_from_payload(cache_key, payload)

    def _store_review_weather_cache_entry(
        self,
        *,
        cache_key: str,
        batch: ScanBatch,
        payload: list[dict[str, Any]],
        position_keys: tuple[tuple[Any, ...], ...],
        refreshed_at_monotonic: float,
    ) -> None:
        self._open_position_weather_cache[cache_key] = {
            "scope": "review",
            "batch": batch,
            "payload": payload,
            "position_keys": position_keys,
            "refreshed_at_monotonic": refreshed_at_monotonic,
        }

    def _fallback_review_weather_batch(
        self,
        *,
        cache_key: str,
        cache_entry: dict[str, Any] | None,
        error: Exception,
    ) -> tuple[ScanBatch, bool] | None:
        if cache_entry is None:
            return None
        logger.warning("open position weather refresh failed for %s; reusing cached data: %s", cache_key, error)
        if str(cache_entry.get("scope") or "") == "review":
            payload = list(cache_entry.get("payload") or [])
            if payload:
                return self._build_review_batch_from_payload(cache_key, payload), False
        batch = cache_entry.get("batch")
        if isinstance(batch, ScanBatch):
            return batch, True
        return None

    def _build_review_signal_map(
        self,
        *,
        signals: list[WeatherSignal],
        positions: list[dict[str, Any]],
        refresh_market_prices: bool = True,
    ) -> dict[tuple[str, str], WeatherSignal]:
        active_markets = {
            str(position.get("market_slug") or "").strip()
            for position in positions
            if str(position.get("market_slug") or "").strip()
        }
        if not active_markets or not signals:
            return {}

        base_signals: dict[str, WeatherSignal] = {}
        for signal in signals:
            market_slug = str(signal.market_slug or "").strip()
            if not market_slug or market_slug not in active_markets:
                continue
            current = base_signals.get(market_slug)
            if current is None or float(signal.edge_abs or 0.0) > float(current.edge_abs or 0.0):
                base_signals[market_slug] = signal
        if not base_signals:
            return {}

        signal_map: dict[tuple[str, str], WeatherSignal] = {}
        if not refresh_market_prices:
            for market_slug, base_signal in base_signals.items():
                signal_map[(market_slug, str(base_signal.direction or "").upper())] = base_signal
            return signal_map

        market_probs = self._fetch_review_market_probs(base_signals.keys())
        for market_slug, base_signal in base_signals.items():
            market_prob = market_probs.get(market_slug)
            if market_prob is None:
                market_prob = _as_probability(base_signal.market_prob)
            if market_prob is None:
                continue
            review_signal = _build_review_signal(base_signal, market_prob)
            signal_map[(market_slug, str(review_signal.direction or "").upper())] = review_signal
        return signal_map

    def _fetch_review_market_probs(self, market_slugs) -> dict[str, float | None]:
        market_probs: dict[str, float | None] = {}
        for market_slug in market_slugs:
            slug = str(market_slug or "").strip()
            if not slug:
                continue
            try:
                market_probs[slug] = _as_probability(self.price_fetcher(slug))
            except Exception as exc:
                logger.warning("open position review price refresh failed for %s: %s", slug, exc)
                market_probs[slug] = None
        return market_probs

    def _review_position_keys(self, market_type: str, positions: list[dict[str, Any]]) -> tuple[tuple[Any, ...], ...]:
        keys: list[tuple[Any, ...]] = []
        for position in positions:
            market_slug = str(position.get("market_slug") or "").strip()
            city_slug = str(position.get("city_slug") or "").strip()
            label = self._review_position_label(position)
            if market_type == "temperature":
                event_date = str(position.get("event_date") or "").strip()
                keys.append((market_slug, city_slug, event_date, label))
                continue
            if market_type == "precipitation":
                year_month = self._review_precip_year_month(position)
                if year_month is None:
                    continue
                year, month = year_month
                keys.append((market_slug, city_slug, year, month, label))
                continue
            keys.append((market_slug, city_slug, label))
        return tuple(sorted(set(keys)))

    def _review_position_label(self, position: dict[str, Any]) -> str:
        return str(position.get("target_label") or position.get("label") or "").strip()

    def _build_review_weather_payload(self, market_type: str, positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if market_type == "temperature":
            return self._build_review_temperature_payload(positions)
        if market_type == "precipitation":
            return self._build_review_precipitation_payload(positions)
        return []

    def _build_review_temperature_payload(self, positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[tuple[str, date], list[tuple[dict[str, Any], dict[str, Any]]]] = {}
        for position in positions:
            city_slug = str(position.get("city_slug") or "").strip()
            event_date = self._review_event_date(position)
            label = self._review_position_label(position)
            bucket = parse_temperature_bucket(label) if label else None
            if not city_slug or event_date is None or not isinstance(bucket, dict):
                continue
            grouped.setdefault((city_slug, event_date), []).append((position, bucket))

        payload: list[dict[str, Any]] = []
        for (city_slug, event_date), items in grouped.items():
            buckets = [dict(bucket) for _, bucket in items]
            forecast_data = get_both_bucket_probabilities(
                city_slug,
                event_date,
                buckets,
                provider_context="review",
            )
            provider_data = dict(forecast_data) if self._has_temperature_forecast_data(forecast_data) else None
            for position, bucket in items:
                payload.append(
                    self._review_payload_item(
                        position,
                        market_type="temperature",
                        city_slug=city_slug,
                        event_date=event_date.isoformat(),
                        label=bucket.get("label"),
                        bucket=dict(bucket),
                        forecast_data=provider_data,
                        remaining_to_resolution_s=_as_float(position.get("remaining_to_resolution_s")),
                    )
                )
        return payload

    def _build_review_precipitation_payload(self, positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[tuple[str, int, int], list[tuple[dict[str, Any], dict[str, Any]]]] = {}
        for position in positions:
            city_slug = str(position.get("city_slug") or "").strip()
            year_month = self._review_precip_year_month(position)
            label = self._review_position_label(position)
            bucket = parse_precip_bucket(label) if label else None
            if not city_slug or year_month is None or not isinstance(bucket, dict):
                continue
            year, month = year_month
            grouped.setdefault((city_slug, year, month), []).append((position, bucket))

        payload: list[dict[str, Any]] = []
        for (city_slug, year, month), items in grouped.items():
            om_data = get_om_monthly_precip(city_slug, year, month)
            vc_data = get_vc_monthly_precip(city_slug, year, month)
            provider_data = None
            if om_data or vc_data:
                observed = (om_data or vc_data)["observed"]
                unit = str((om_data or vc_data)["unit"])
                unique_buckets: dict[str, dict[str, Any]] = {}
                for _, bucket in items:
                    unique_buckets.setdefault(str(bucket.get("label") or ""), dict(bucket))
                bucket_list = list(unique_buckets.values())
                om_probs = calc_precip_bucket_probs(observed, om_data["forecast"], bucket_list, unit) if om_data else None
                vc_probs = calc_precip_bucket_probs(observed, vc_data["forecast"], bucket_list, unit) if vc_data else None
                provider_data = {
                    "observed": observed,
                    "openmeteo": om_probs,
                    "vc": vc_probs,
                    "om_temp": om_data["total_projected"] if om_data else None,
                    "vc_temp": vc_data["total_projected"] if vc_data else None,
                    "unit": unit,
                        }
            for position, bucket in items:
                payload.append(
                    self._review_payload_item(
                        position,
                        market_type="precipitation",
                        city_slug=city_slug,
                        event_date=f"{year:04d}-{month:02d}-01",
                        label=bucket.get("label"),
                        bucket=dict(bucket),
                        forecast_data=provider_data,
                    )
                )
        return payload

    def _has_temperature_forecast_data(self, forecast_data: dict[str, Any]) -> bool:
        return any(forecast_data.get(key) is not None for key in TEMPERATURE_FORECAST_KEYS)

    def _review_payload_item(
        self,
        position: dict[str, Any],
        *,
        market_type: str,
        city_slug: str,
        event_date: str,
        label: Any,
        bucket: dict[str, Any],
        forecast_data: dict[str, Any] | None,
        remaining_to_resolution_s: float | None = None,
    ) -> dict[str, Any]:
        payload = {
            "market_type": str(market_type or ""),
            "market_slug": str(position.get("market_slug") or "").strip(),
            "event_slug": str(position.get("event_slug") or "").strip(),
            "event_title": str(position.get("event_title") or position.get("market_slug") or "Unknown market"),
            "city_slug": str(city_slug or ""),
            "event_date": str(event_date or ""),
            "label": label,
            "bucket": dict(bucket),
            "liquidity": float(position.get("liquidity") or 0.0),
            "fallback_market_prob": self._review_fallback_market_prob(position),
            "forecast_data": forecast_data,
        }
        if remaining_to_resolution_s is not None:
            payload["remaining_to_resolution_s"] = remaining_to_resolution_s
        return payload

    def _review_fallback_market_prob(self, position: dict[str, Any]) -> float | None:
        return _as_probability(
            position.get("market_probability") or position.get("mark_price") or position.get("entry_price")
        )

    def _build_review_batch_from_payload(self, market_type: str, payload: list[dict[str, Any]]) -> ScanBatch:
        if market_type == "temperature":
            return self._build_temperature_review_batch(payload)
        if market_type == "precipitation":
            return self._build_precipitation_review_batch(payload)
        return self._empty_batch(market_type)

    def _build_temperature_review_batch(self, payload: list[dict[str, Any]]) -> ScanBatch:
        created_at = datetime.now(timezone.utc)
        market_probs = self._fetch_review_market_probs([item.get("market_slug") for item in payload])
        signals: list[WeatherSignal] = []
        error_count = 0
        error_samples: list[str] = []
        skipped_events = 0

        for item in payload:
            forecast_data = item.get("forecast_data")
            if not isinstance(forecast_data, dict):
                error_count += 1
                skipped_events += 1
                self._append_review_batch_error(
                    error_samples,
                    f"temperature provider unavailable for {item.get('market_slug')}",
                )
                continue
            market_slug = str(item.get("market_slug") or "").strip()
            market_prob = self._review_batch_market_probability(item, market_probs)
            if market_prob is None:
                skipped_events += 1
                continue
            bucket = self._review_batch_bucket(item, market_prob, market_slug)
            discrepancies = find_discrepancies(
                event_title=str(item.get("event_title") or "Unknown"),
                city_slug=str(item.get("city_slug") or ""),
                event_date=str(item.get("event_date") or ""),
                buckets=[bucket],
                wu_probs=forecast_data.get("wu"),
                om_probs=forecast_data.get("openmeteo"),
                wu_temp=forecast_data.get("wu_temp"),
                om_temp=forecast_data.get("om_temp"),
                unit_symbol=str(forecast_data.get("unit") or "F"),
                vc_probs=forecast_data.get("vc"),
                vc_temp=forecast_data.get("vc_temp"),
                noaa_probs=forecast_data.get("noaa"),
                noaa_temp=forecast_data.get("noaa_temp"),
                weatherapi_probs=forecast_data.get("weatherapi"),
                weatherapi_temp=forecast_data.get("weatherapi_temp"),
            )
            event_end = self._review_event_end(created_at, item.get("remaining_to_resolution_s"))
            for discrepancy in discrepancies:
                signals.append(
                    _build_temperature_signal(
                        event={"title": item.get("event_title"), "slug": item.get("event_slug")},
                        discrepancy=discrepancy,
                        event_end=event_end,
                        created_at=created_at,
                    )
                )

        signals.sort(key=lambda signal: signal.score, reverse=True)
        finished_at = datetime.now(timezone.utc).isoformat()
        return ScanBatch(
            scan_type="temperature",
            signals=signals,
            total_events=len(payload),
            processed_events=len(payload),
            flagged_events=len(signals),
            skipped_events=skipped_events,
            started_at=created_at.isoformat(),
            finished_at=finished_at,
            error_count=error_count,
            error_samples=error_samples,
        )

    def _build_precipitation_review_batch(self, payload: list[dict[str, Any]]) -> ScanBatch:
        created_at = datetime.now(timezone.utc)
        market_probs = self._fetch_review_market_probs([item.get("market_slug") for item in payload])
        signals: list[WeatherSignal] = []
        error_count = 0
        error_samples: list[str] = []
        skipped_events = 0

        for item in payload:
            forecast_data = item.get("forecast_data")
            if not isinstance(forecast_data, dict):
                error_count += 1
                skipped_events += 1
                self._append_review_batch_error(
                    error_samples,
                    f"precip provider unavailable for {item.get('market_slug')}",
                )
                continue
            market_slug = str(item.get("market_slug") or "").strip()
            market_prob = self._review_batch_market_probability(item, market_probs)
            if market_prob is None:
                skipped_events += 1
                continue
            bucket = self._review_batch_bucket(item, market_prob, market_slug)
            discrepancies = find_discrepancies(
                event_title=str(item.get("event_title") or "Unknown"),
                city_slug=str(item.get("city_slug") or ""),
                event_date=str(item.get("event_date") or ""),
                buckets=[bucket],
                wu_probs=None,
                om_probs=forecast_data.get("openmeteo"),
                wu_temp=forecast_data.get("observed"),
                om_temp=forecast_data.get("om_temp"),
                unit_symbol=str(forecast_data.get("unit") or "in"),
                vc_probs=forecast_data.get("vc"),
                vc_temp=forecast_data.get("vc_temp"),
                noaa_probs=None,
                noaa_temp=None,
            )
            for discrepancy in discrepancies:
                signals.append(_build_precip_signal(discrepancy, created_at))

        signals.sort(key=lambda signal: signal.score, reverse=True)
        finished_at = datetime.now(timezone.utc).isoformat()
        return ScanBatch(
            scan_type="precipitation",
            signals=signals,
            total_events=len(payload),
            processed_events=len(payload),
            flagged_events=len(signals),
            skipped_events=skipped_events,
            started_at=created_at.isoformat(),
            finished_at=finished_at,
            error_count=error_count,
            error_samples=error_samples,
        )

    def _review_batch_market_probability(
        self,
        item: dict[str, Any],
        market_probs: dict[str, float | None],
    ) -> float | None:
        market_slug = str(item.get("market_slug") or "").strip()
        market_prob = market_probs.get(market_slug)
        if market_prob is not None:
            return market_prob
        return _as_probability(item.get("fallback_market_prob"))

    def _review_batch_bucket(
        self,
        item: dict[str, Any],
        market_prob: float,
        market_slug: str,
    ) -> dict[str, Any]:
        bucket = dict(item.get("bucket") or {})
        bucket["market_yes_price"] = market_prob
        bucket["market_slug"] = market_slug
        bucket["event_slug"] = str(item.get("event_slug") or "")
        bucket["liquidity"] = float(item.get("liquidity") or 0.0)
        return bucket

    def _append_review_batch_error(self, error_samples: list[str], message: str) -> None:
        if len(error_samples) < 5:
            error_samples.append(str(message))

    def _review_event_date(self, position: dict[str, Any]) -> date | None:
        raw = str(position.get("event_date") or "").strip()
        if not raw:
            return None
        try:
            return date.fromisoformat(raw[:10])
        except ValueError:
            return None

    def _review_precip_year_month(self, position: dict[str, Any]) -> tuple[int, int] | None:
        event_date = self._review_event_date(position)
        if event_date is None:
            return None
        return event_date.year, event_date.month

    def _review_event_end(self, created_at: datetime, remaining_to_resolution_s: Any) -> datetime | None:
        seconds = _as_float(remaining_to_resolution_s)
        if seconds is None:
            return None
        return created_at + timedelta(seconds=max(0.0, seconds))

    def _temperature_loop(self) -> None:
        self._loop_runner(
            scan_type="temperature",
            interval_seconds_provider=lambda: self.get_status_snapshot().get(
                "auto_temperature_scan_interval_seconds",
                self._configured_temperature_scan_interval_seconds(),
            ),
            enabled_key="temperature_enabled",
            runner=lambda: self.request_scan("temperature", send_alerts=True, reason="scheduled"),
        )

    def _precipitation_loop(self) -> None:
        self._loop_runner(
            scan_type="precipitation",
            interval_seconds_provider=lambda: self.get_status_snapshot().get(
                "auto_precipitation_scan_interval_seconds",
                self._configured_precipitation_scan_interval_seconds(),
            ),
            enabled_key="precipitation_enabled",
            runner=lambda: self.request_scan("precipitation", send_alerts=True, reason="scheduled"),
        )

    def _open_position_review_loop(self) -> None:
        interval_s = _scheduled_interval_seconds(
            self.config.app.open_position_review_seconds,
            0,
            minimum_seconds=10,
        )
        while not self._stop_event.wait(interval_s):
            try:
                state = self.get_status_snapshot()
                if state.get("scan_in_progress"):
                    continue
                if not self.tracker.get_paper_stats().get("open_positions"):
                    continue
                self.review_open_positions(reason="scheduled_open_position_review")
            except Exception:
                continue

    def _resolution_loop(self) -> None:
        interval_s = _scheduled_interval_seconds(
            0,
            self.config.app.resolution_check_minutes,
            minimum_seconds=60,
        )
        while not self._stop_event.wait(interval_s):
            try:
                self.settle_due_positions(send_alerts=True)
                self._update_state(last_resolution_check_at=datetime.now(timezone.utc).isoformat(), last_resolution_error=None)
            except Exception as exc:
                self._update_state(
                    last_resolution_check_at=datetime.now(timezone.utc).isoformat(),
                    last_resolution_error=str(exc),
                )
                continue

    def _loop_runner(self, *, scan_type: str, interval_seconds_provider, enabled_key: str, runner) -> None:
        last_cycle_at = time.monotonic()
        while not self._stop_event.is_set():
            interval_s = max(1, int(interval_seconds_provider()))
            due_at = last_cycle_at + interval_s
            remaining = due_at - time.monotonic()
            due_timestamp = datetime.now(timezone.utc) + timedelta(seconds=max(0.0, remaining))
            self._set_next_scheduled_scan(scan_type, due_timestamp)
            if remaining > 0:
                if self._stop_event.wait(min(1.0, remaining)):
                    break
                continue
            try:
                state = self.get_status_snapshot()
                if state.get("state") == "paused" or not state.get(enabled_key, True):
                    last_cycle_at = time.monotonic()
                    continue
                runner()
                last_cycle_at = time.monotonic()
            except Exception:
                last_cycle_at = time.monotonic()
                continue
        self._set_next_scheduled_scan(scan_type, None)

    def _configured_temperature_scan_interval_seconds(self) -> int:
        return _scheduled_interval_seconds(
            self.config.app.auto_temperature_scan_seconds,
            self.config.app.auto_temperature_scan_minutes,
            minimum_seconds=5,
        )

    def _configured_precipitation_scan_interval_seconds(self) -> int:
        return _scheduled_interval_seconds(
            self.config.app.auto_precipitation_scan_seconds,
            self.config.app.auto_precipitation_scan_minutes,
            minimum_seconds=5,
        )

    def _prime_next_scheduled_scans(self) -> None:
        now = datetime.now(timezone.utc)
        self._set_next_scheduled_scan(
            "temperature",
            now + timedelta(seconds=self._state.get("auto_temperature_scan_interval_seconds", self._configured_temperature_scan_interval_seconds())),
        )
        self._set_next_scheduled_scan(
            "precipitation",
            now + timedelta(seconds=self._state.get("auto_precipitation_scan_interval_seconds", self._configured_precipitation_scan_interval_seconds())),
        )

    def _set_next_scheduled_scan(self, scan_type: str, due_at: datetime | None) -> None:
        key = str(scan_type or "").strip().lower()
        if key not in self._next_scheduled_scan_at:
            return
        with self._state_lock:
            self._next_scheduled_scan_at[key] = due_at

    def _empty_batch(self, scan_type: str) -> ScanBatch:
        timestamp = datetime.now(timezone.utc).isoformat()
        return ScanBatch(
            scan_type=scan_type,
            signals=[],
            total_events=0,
            processed_events=0,
            flagged_events=0,
            skipped_events=0,
            started_at=timestamp,
            finished_at=timestamp,
        )

    def _write_scan_export(
        self,
        *,
        scan_type: str,
        status: str,
        reason: str,
        duration_ms: int,
        batch: ScanBatch | None,
        accepted_count: int,
        opened_count: int,
        settled_count: int,
        error: str | None,
    ) -> None:
        if self.scan_export_root is None:
            return
        finished_at = datetime.now(timezone.utc)
        payload = {
            "scan_type": scan_type,
            "status": status,
            "reason": reason,
            "duration_ms": duration_ms,
            "finished_at": finished_at.isoformat(),
            "accepted_count": accepted_count,
            "opened_count": opened_count,
            "settled_count": settled_count,
            "error": error,
            "batch": batch.to_dict() if batch is not None else None,
        }
        path = self.scan_export_root / f"{finished_at.strftime('%Y%m%dT%H%M%S%fZ')}_{scan_type}_{status}.json"
        try:
            path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
            self._update_state(last_scan_export_error=None)
        except OSError as exc:
            self._update_state(last_scan_export_error=str(exc))

    def _sync_queue_state_locked(self) -> None:
        pending = [str(job.get("scan_type") or "") for job in self._scan_queue]
        self._update_state(scan_queue_depth=len(pending), pending_scan_types=pending)

    def _update_state(self, **changes) -> None:
        with self._state_lock:
            self._state.update(changes)
            self.tracker.set_runtime_state("runtime_status", dict(self._state))

    def _reconcile_boot_state(self) -> bool:
        changes: dict[str, Any] = {
            "scan_in_progress": False,
            "scan_queue_depth": 0,
            "pending_scan_types": [],
            "active_scan_type": None,
            "active_scan_started_at": None,
            "scan_worker_healthy": False,
            "open_position_review_in_progress": False,
        }
        interrupted_scan_types: list[str] = []
        interruption_note = "Previous process exited before this scan finished."
        for scan_type in ("temperature", "precipitation"):
            status_fields = _scan_status_fields(scan_type)
            status = str(self._state.get(status_fields["status"]) or "").strip().lower()
            was_active = bool(self._state.get("scan_in_progress")) and str(self._state.get("active_scan_type") or "") == scan_type
            if status == "running" or was_active:
                interrupted_scan_types.append(scan_type)
                changes[status_fields["status"]] = "interrupted"
                if not str(self._state.get(status_fields["error"]) or "").strip():
                    changes[status_fields["error"]] = interruption_note
        review_status = str(self._state.get("last_open_position_review_status") or "").strip().lower()
        if review_status == "running" or bool(self._state.get("open_position_review_in_progress")):
            changes["last_open_position_review_status"] = "interrupted"
            if not str(self._state.get("last_open_position_review_error") or "").strip():
                changes["last_open_position_review_error"] = "Previous process exited before the open-position review finished."
        if interrupted_scan_types and not str(self._state.get("last_scan_worker_error") or "").strip():
            interrupted_labels = ", ".join(scan_type.title() for scan_type in interrupted_scan_types)
            changes["last_scan_worker_error"] = f"Recovered stale scan state for: {interrupted_labels}."
        changed = False
        for key, value in changes.items():
            if self._state.get(key) != value:
                self._state[key] = value
                changed = True
        return changed


def _scan_status_fields(scan_type: str) -> dict[str, str]:
    prefix = "last_temperature" if scan_type == "temperature" else "last_precipitation"
    return {
        "at": f"{prefix}_scan_at",
        "count": f"{prefix}_signal_count",
        "status": f"{prefix}_scan_status",
        "duration_ms": f"{prefix}_scan_duration_ms",
        "reason": f"{prefix}_scan_reason",
        "error": f"{prefix}_scan_error",
        "error_count": f"{prefix}_error_count",
    }


def _callable_accepts_keyword(func: Any, keyword: str) -> bool:
    try:
        signature = inspect.signature(func)
    except (TypeError, ValueError):
        return False
    parameters = signature.parameters.values()
    return any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD or parameter.name == keyword
        for parameter in parameters
    )


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


def _normalize_scan_interval_seconds(value: Any, *, default_seconds: int) -> int:
    try:
        seconds = int(value or 0)
    except (TypeError, ValueError):
        seconds = 0
    if seconds <= 0:
        seconds = int(default_seconds)
    return max(300, min(21600, int(seconds)))


def _normalize_scan_interval_minutes(value: Any) -> int:
    try:
        minutes = int(value or 0)
    except (TypeError, ValueError):
        minutes = 0
    return max(5, min(360, int(minutes)))


def _build_review_signal(signal: WeatherSignal, market_prob: float) -> WeatherSignal:
    bounded_market_prob = max(0.0, min(1.0, float(market_prob)))
    yes_edge = float(signal.forecast_prob or 0.0) - bounded_market_prob
    direction = "YES" if yes_edge >= 0 else "NO"
    edge_abs = abs(yes_edge)
    raw_payload = dict(signal.raw_payload)
    raw_payload["market_prob"] = bounded_market_prob
    raw_payload["direction"] = direction
    raw_payload["discrepancy"] = edge_abs
    return replace(
        signal,
        direction=direction,
        market_prob=bounded_market_prob,
        edge=edge_abs,
        edge_abs=edge_abs,
        edge_size=_edge_size_label(edge_abs),
        score=_review_signal_score(signal, edge_abs=edge_abs),
        raw_payload=raw_payload,
    )


def _review_signal_score(signal: WeatherSignal, *, edge_abs: float) -> float:
    source_count = max(1, int(signal.source_count or 1))
    liquidity = max(0.0, float(signal.liquidity or 0.0))
    dispersion = max(0.0, float(signal.source_dispersion_pct or 0.0))
    if signal.market_type == "precipitation":
        score = (
            0.45 * min(1.0, edge_abs / 0.25)
            + 0.25 * min(1.0, source_count / 2.0)
            + 0.2 * min(1.0, liquidity / 500.0)
            - min(0.2, dispersion)
        )
        return round(max(0.0, min(0.99, score)), 4)

    if signal.time_to_resolution_s is None:
        timing_score = 0.5
    else:
        hours = max(0.0, float(signal.time_to_resolution_s) / 3600.0)
        timing_score = 0.1 if hours < 1 else 1.0 if hours <= 24 else max(0.35, 1.0 - min(hours, 240.0) / 400.0)
    score = (
        0.4 * min(1.0, edge_abs / 0.25)
        + 0.25 * min(1.0, source_count / 4.0)
        + 0.2 * min(1.0, liquidity / 500.0)
        + 0.15 * timing_score
        - min(0.25, dispersion)
    )
    return round(max(0.0, min(0.99, score)), 4)


def _edge_size_label(edge_abs: float) -> str:
    if edge_abs >= 0.2:
        return "large"
    if edge_abs >= 0.1:
        return "medium"
    return "small"


def _as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_probability(value: Any) -> float | None:
    raw = _as_float(value)
    if raw is None:
        return None
    return round(max(0.0, min(1.0, raw)), 6)


def get_market_resolution(market_slug: str) -> str | None:
    try:
        response = requests.get(
            "https://gamma-api.polymarket.com/markets",
            params={"slug": market_slug},
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json()
        if not payload:
            return None
        market = payload[0]
        if not market.get("closed"):
            return None
        resolution_price = market.get("resolutionPrice")
        if resolution_price is None:
            return None
        return "YES" if float(resolution_price) >= 0.5 else "NO"
    except Exception:
        return None
