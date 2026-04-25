"""Weather runtime loops and orchestration."""

from __future__ import annotations

import json
import threading
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from .messages import format_resolution_message, format_scan_summary, format_signal_message
from .models import ResolutionOutcome, ScanBatch
from .precipitation_signals import scan_precipitation_signals
from .temperature import scan_temperature_signals


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
        scan_export_root: str | Path | None = None,
    ):
        self.config = config
        self.tracker = tracker
        self.strategy_engine = strategy_engine
        self.telegram = telegram
        self.temperature_scanner = temperature_scanner
        self.precipitation_scanner = precipitation_scanner
        self.resolution_fetcher = resolution_fetcher or get_market_resolution
        self.scan_export_root = Path(scan_export_root) if scan_export_root else None
        scan_export_error = None
        if self.scan_export_root is not None:
            try:
                self.scan_export_root.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                scan_export_error = str(exc)
                self.scan_export_root = None
        self._state_lock = threading.RLock()
        self._scan_lock = threading.Lock()
        self._queue_condition = threading.Condition()
        self._scan_queue: deque[dict[str, Any]] = deque()
        self._stop_event = threading.Event()
        self._threads: list[threading.Thread] = []
        self._state = self.tracker.get_runtime_state(
            "runtime_status",
            default={
                "state": "running",
                "temperature_enabled": bool(self.config.temperature.enabled),
                "precipitation_enabled": bool(self.config.precipitation.enabled),
                "paper_auto_trade": True,
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
                "last_resolution_check_at": None,
                "last_resolution_error": None,
            },
        )

    def start_background_loops(self) -> None:
        if self._threads:
            return
        self._stop_event.clear()
        loops = [
            (self._scan_worker_loop, "weather-scan-worker"),
            (self._temperature_loop, "weather-temp-loop"),
            (self._precipitation_loop, "weather-precip-loop"),
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
                results = self.strategy_engine.process_signals(
                    batch.signals,
                    auto_trade_enabled=bool(self.get_status_snapshot().get("paper_auto_trade", True) and auto_trade),
                )
                accepted = sum(1 for item in results if item.decision.accepted)
                opened = sum(1 for item in results if item.position is not None)
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

    def _temperature_loop(self) -> None:
        self._loop_runner(
            interval_minutes=int(self.config.app.auto_temperature_scan_minutes),
            enabled_key="temperature_enabled",
            runner=lambda: self.request_scan("temperature", send_alerts=True, reason="scheduled"),
        )

    def _precipitation_loop(self) -> None:
        self._loop_runner(
            interval_minutes=int(self.config.app.auto_precipitation_scan_minutes),
            enabled_key="precipitation_enabled",
            runner=lambda: self.request_scan("precipitation", send_alerts=True, reason="scheduled"),
        )

    def _resolution_loop(self) -> None:
        interval_s = max(60, int(self.config.app.resolution_check_minutes) * 60)
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

    def _loop_runner(self, *, interval_minutes: int, enabled_key: str, runner) -> None:
        interval_s = max(60, int(interval_minutes) * 60)
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
