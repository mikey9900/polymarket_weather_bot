"""Weather runtime loops and orchestration."""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from polymarket.polymarket_prices import get_yes_price

from .messages import format_resolution_message, format_scan_summary, format_signal_message
from .models import ResolutionOutcome, ScanBatch, WeatherSignal
from .precipitation_signals import scan_precipitation_signals
from .temperature import scan_temperature_signals


logger = logging.getLogger(__name__)


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
        default_state = {
            "state": "running",
            "temperature_enabled": bool(self.config.temperature.enabled),
            "precipitation_enabled": bool(self.config.precipitation.enabled),
            "paper_auto_trade": True,
            "paper_max_open_positions": int(getattr(self.strategy_engine, "paper_max_open_positions", self.config.paper.max_open_positions)),
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
        self._state["open_position_weather_refresh_interval_seconds"] = self._open_position_weather_refresh_interval_seconds()
        if hasattr(self.strategy_engine, "set_paper_max_open_positions"):
            limit = self.strategy_engine.set_paper_max_open_positions(int(self._state.get("paper_max_open_positions") or self.config.paper.max_open_positions))
            self._state["paper_max_open_positions"] = int(limit)

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

    def set_precipitation_enabled(self, enabled: bool) -> bool:
        self._update_state(precipitation_enabled=bool(enabled))
        return bool(enabled)

    def set_paper_auto_trade(self, enabled: bool) -> bool:
        self._update_state(paper_auto_trade=bool(enabled))
        return bool(enabled)

    def set_paper_max_open_positions(self, value: int) -> int:
        limit = max(1, min(100, int(value)))
        if hasattr(self.strategy_engine, "set_paper_max_open_positions"):
            limit = int(self.strategy_engine.set_paper_max_open_positions(limit))
        self._update_state(paper_max_open_positions=limit)
        return limit

    def get_status_snapshot(self) -> dict:
        with self._state_lock:
            return dict(self._state)

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
            active_type = str(self.get_status_snapshot().get("active_scan_type") or "")
            pending_types = [str(job.get("scan_type") or "") for job in self._scan_queue]
            if active_type == scan_type or scan_type in pending_types:
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
            scanner=lambda: self.temperature_scanner(limit=limit or self.config.temperature.scan_limit),
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
        )
        if result is None:
            return {"ok": False, "status": 404, "message": f"Open paper position {position_id} was not found."}
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
            positions = self.tracker.get_dashboard_paper_positions(limit=500, status="open")
            if market_types:
                positions = [item for item in positions if str(item.get("market_type") or "") in market_types]
            if not positions:
                reviewed_at = datetime.now(timezone.utc).isoformat()
                self._update_state(
                    open_position_review_in_progress=False,
                    last_open_position_review_at=reviewed_at,
                    last_open_position_review_status="idle",
                    last_open_position_review_error=None,
                    last_open_position_review_reason=str(reason or "scheduled_review"),
                    last_open_position_review_count=0,
                    last_open_position_close_count=0,
                )
                return {"ok": True, "reviewed": 0, "closed": 0, "reason": reason}

            self._update_state(open_position_review_in_progress=True)
            reviewed_count = 0
            closed_count = 0
            market_groups = sorted({str(item.get("market_type") or "") for item in positions})
            try:
                self.settle_due_positions(send_alerts=False)
                for market_type in market_groups:
                    batch = self._get_review_weather_batch(market_type)
                    allow_missing_signal_close = int(batch.error_count or 0) == 0
                    summary = self._review_positions_for_signals(
                        scan_type=market_type,
                        signals=batch.signals,
                        trigger=reason,
                        allow_close_on_missing_signal=allow_missing_signal_close,
                    )
                    reviewed_count += int(summary["reviewed"])
                    closed_count += int(summary["closed"])
                reviewed_at = datetime.now(timezone.utc).isoformat()
                self._update_state(
                    open_position_review_in_progress=False,
                    last_open_position_review_at=reviewed_at,
                    last_open_position_review_status="completed",
                    last_open_position_review_error=None,
                    last_open_position_review_reason=str(reason or "scheduled_review"),
                    last_open_position_review_count=reviewed_count,
                    last_open_position_close_count=closed_count,
                )
                return {"ok": True, "reviewed": reviewed_count, "closed": closed_count, "reason": reason}
            except Exception as exc:
                reviewed_at = datetime.now(timezone.utc).isoformat()
                self._update_state(
                    open_position_review_in_progress=False,
                    last_open_position_review_at=reviewed_at,
                    last_open_position_review_status="failed",
                    last_open_position_review_error=str(exc),
                    last_open_position_review_reason=str(reason or "scheduled_review"),
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
        trigger: str,
        allow_close_on_missing_signal: bool,
    ) -> dict[str, int]:
        positions = [
            item
            for item in self.tracker.get_dashboard_paper_positions(limit=500, status="open")
            if str(item.get("market_type") or "") == scan_type
        ]
        if not positions:
            return {"reviewed": 0, "closed": 0}

        signal_map = self._build_review_signal_map(signals=signals, positions=positions)
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
                exit_fee_bps=self.config.paper.fee_bps,
                exit_slippage_bps=self.config.paper.exit_slippage_bps,
            )
            reviewed += 1
            if not decision.should_close:
                continue
            result = self.tracker.close_paper_position(
                int(position["id"]),
                exit_price=mark_price,
                reason=decision.reason,
                closed_at=reviewed_at,
                mark_probability=mark_probability,
                edge_abs=decision.edge_abs,
                final_score=decision.final_score,
                mark_reason=f"{trigger}: {decision.reason}",
            )
            if result is not None:
                closed += 1

        return {"reviewed": reviewed, "closed": closed}

    def _open_position_weather_refresh_interval_seconds(self) -> int:
        minutes = int(getattr(self.config.app, "open_position_weather_refresh_minutes", 15) or 0)
        if minutes <= 0:
            return 0
        return max(60, minutes * 60)

    def _store_open_position_weather_batch(self, market_type: str, batch: ScanBatch) -> None:
        self._open_position_weather_cache[str(market_type or "")] = {
            "batch": batch,
            "refreshed_at_monotonic": time.monotonic(),
        }

    def _get_review_weather_batch(self, market_type: str) -> ScanBatch:
        cache_key = str(market_type or "")
        cache_entry = self._open_position_weather_cache.get(cache_key)
        refresh_interval_s = self._open_position_weather_refresh_interval_seconds()
        now = time.monotonic()
        if cache_entry is not None and refresh_interval_s > 0:
            age_s = now - float(cache_entry.get("refreshed_at_monotonic") or 0.0)
            if age_s < refresh_interval_s:
                return cache_entry["batch"]
        try:
            if cache_key == "temperature":
                batch = self.temperature_scanner(limit=self.config.temperature.scan_limit)
            elif cache_key == "precipitation":
                batch = self.precipitation_scanner()
            else:
                return self._empty_batch(cache_key)
        except Exception as exc:
            if cache_entry is not None:
                logger.warning("open position weather refresh failed for %s; reusing cached batch: %s", cache_key, exc)
                return cache_entry["batch"]
            raise
        self._store_open_position_weather_batch(cache_key, batch)
        return batch

    def _build_review_signal_map(
        self,
        *,
        signals: list[WeatherSignal],
        positions: list[dict[str, Any]],
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

        market_probs = self._fetch_review_market_probs(base_signals.keys())
        signal_map: dict[tuple[str, str], WeatherSignal] = {}
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

    def _temperature_loop(self) -> None:
        self._loop_runner(
            interval_seconds=_scheduled_interval_seconds(
                self.config.app.auto_temperature_scan_seconds,
                self.config.app.auto_temperature_scan_minutes,
                minimum_seconds=5,
            ),
            enabled_key="temperature_enabled",
            runner=lambda: self.request_scan("temperature", send_alerts=True, reason="scheduled"),
        )

    def _precipitation_loop(self) -> None:
        self._loop_runner(
            interval_seconds=_scheduled_interval_seconds(
                self.config.app.auto_precipitation_scan_seconds,
                self.config.app.auto_precipitation_scan_minutes,
                minimum_seconds=5,
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

    def _loop_runner(self, *, interval_seconds: int, enabled_key: str, runner) -> None:
        interval_s = max(1, int(interval_seconds))
        while not self._stop_event.wait(interval_s):
            try:
                state = self.get_status_snapshot()
                if state.get("state") == "paused" or not state.get(enabled_key, True):
                    continue
                runner()
            except Exception:
                continue

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
