"""Weather runtime loops and orchestration."""

from __future__ import annotations

import threading
from datetime import datetime, timezone

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
    ):
        self.config = config
        self.tracker = tracker
        self.strategy_engine = strategy_engine
        self.telegram = telegram
        self.temperature_scanner = temperature_scanner
        self.precipitation_scanner = precipitation_scanner
        self.resolution_fetcher = resolution_fetcher or get_market_resolution
        self._state_lock = threading.RLock()
        self._scan_lock = threading.Lock()
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
                "last_temperature_scan_at": None,
                "last_precipitation_scan_at": None,
                "last_temperature_signal_count": 0,
                "last_precipitation_signal_count": 0,
            },
        )

    def start_background_loops(self) -> None:
        if self._threads:
            return
        self._stop_event.clear()
        loops = [
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
        for thread in self._threads:
            thread.join(timeout=5.0)
        self._threads.clear()

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

    def run_temperature_scan(self, *, send_alerts: bool = True, limit: int | None = None) -> tuple[ScanBatch, list]:
        return self._run_scan(
            scan_type="temperature",
            enabled_key="temperature_enabled",
            scanner=lambda: self.temperature_scanner(limit=limit or self.config.temperature.scan_limit),
            auto_trade=self.config.temperature.auto_paper_trade,
            send_alerts=send_alerts,
        )

    def run_precipitation_scan(self, *, send_alerts: bool = True) -> tuple[ScanBatch, list]:
        return self._run_scan(
            scan_type="precipitation",
            enabled_key="precipitation_enabled",
            scanner=self.precipitation_scanner,
            auto_trade=self.config.precipitation.auto_paper_trade,
            send_alerts=send_alerts,
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

    def _run_scan(self, *, scan_type: str, enabled_key: str, scanner, auto_trade: bool, send_alerts: bool) -> tuple[ScanBatch, list]:
        with self._scan_lock:
            if self.get_status_snapshot().get("state") == "paused":
                empty = ScanBatch(scan_type=scan_type, signals=[], total_events=0, processed_events=0, flagged_events=0, skipped_events=0, started_at=datetime.now(timezone.utc).isoformat(), finished_at=datetime.now(timezone.utc).isoformat())
                return empty, []
            if not self.get_status_snapshot().get(enabled_key, True):
                empty = ScanBatch(scan_type=scan_type, signals=[], total_events=0, processed_events=0, flagged_events=0, skipped_events=0, started_at=datetime.now(timezone.utc).isoformat(), finished_at=datetime.now(timezone.utc).isoformat())
                return empty, []
            self._update_state(scan_in_progress=True)
            settled = self.settle_due_positions(send_alerts=send_alerts)
            batch = scanner()
            results = self.strategy_engine.process_signals(
                batch.signals,
                auto_trade_enabled=bool(self.get_status_snapshot().get("paper_auto_trade", True) and auto_trade),
            )
            if send_alerts and self.config.alerts.telegram_enabled and self.config.alerts.send_scan_summary:
                accepted = sum(1 for item in results if item.decision.accepted)
                opened = sum(1 for item in results if item.position is not None)
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
            state_key = "last_temperature_scan_at" if scan_type == "temperature" else "last_precipitation_scan_at"
            count_key = "last_temperature_signal_count" if scan_type == "temperature" else "last_precipitation_signal_count"
            self._update_state(scan_in_progress=False, **{state_key: datetime.now(timezone.utc).isoformat(), count_key: len(batch.signals)})
            return batch, results

    def _temperature_loop(self) -> None:
        self._loop_runner(
            interval_minutes=int(self.config.app.auto_temperature_scan_minutes),
            enabled_key="temperature_enabled",
            runner=lambda: self.run_temperature_scan(send_alerts=True),
        )

    def _precipitation_loop(self) -> None:
        self._loop_runner(
            interval_minutes=int(self.config.app.auto_precipitation_scan_minutes),
            enabled_key="precipitation_enabled",
            runner=lambda: self.run_precipitation_scan(send_alerts=True),
        )

    def _resolution_loop(self) -> None:
        interval_s = max(60, int(self.config.app.resolution_check_minutes) * 60)
        while not self._stop_event.wait(interval_s):
            try:
                self.settle_due_positions(send_alerts=True)
            except Exception:
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

    def _update_state(self, **changes) -> None:
        with self._state_lock:
            self._state.update(changes)
            self.tracker.set_runtime_state("runtime_status", dict(self._state))


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
