from __future__ import annotations

import json
from pathlib import Path

import yaml

from weather_bot.config import load_config
from weather_bot.control_plane import ControlPlane, ControlRequest
from weather_bot.dashboard_state import DashboardStateService
from weather_bot.models import ForecastSnapshot, ScanBatch, WeatherSignal
from weather_bot.paths import DEFAULT_CONFIG_TEMPLATE_PATH
from weather_bot.runtime import WeatherRuntime, _scheduled_interval_seconds
from weather_bot.strategy import WeatherStrategyEngine
from weather_bot.telegram_client import TelegramClient
from weather_bot.tracker import WeatherTracker


def _write_config(tmp_path: Path) -> Path:
    payload = yaml.safe_load(DEFAULT_CONFIG_TEMPLATE_PATH.read_text(encoding="utf-8"))
    config_path = tmp_path / "active_config.yaml"
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return config_path


def _signal(key: str = "rt-1") -> WeatherSignal:
    return WeatherSignal(
        signal_key=key,
        market_type="temperature",
        event_title="Highest temperature in NYC on April 25",
        market_slug=f"market-{key}",
        event_slug=f"event-{key}",
        city_slug="nyc",
        event_date="2026-04-25",
        label="70-71F",
        direction="YES",
        market_prob=0.25,
        forecast_prob=0.80,
        edge=0.55,
        edge_abs=0.55,
        edge_size="large",
        confidence="confirmed",
        source_count=3,
        liquidity=600.0,
        time_to_resolution_s=4 * 3600.0,
        source_dispersion_pct=0.02,
        score=0.85,
        forecast_snapshot=ForecastSnapshot(
            market_type="temperature",
            city_slug="nyc",
            event_date="2026-04-25",
            unit="F",
            om_temp=72.0,
            vc_temp=73.0,
            source_probabilities={"openmeteo": 0.8, "visual_crossing": 0.79},
        ),
        raw_payload={"event_title": "Highest temperature in NYC on April 25", "label": "70-71F", "direction": "YES"},
    )


def _batch(signal: WeatherSignal | None = None, *, scan_type: str = "temperature", error_count: int = 0) -> ScanBatch:
    timestamp = signal.created_at if signal is not None else "2026-04-24T12:00:00+00:00"
    signals = [signal] if signal is not None else []
    return ScanBatch(
        scan_type=scan_type,
        signals=signals,
        total_events=1 if signal is not None else 0,
        processed_events=1 if signal is not None else 0,
        flagged_events=1 if signal is not None else 0,
        skipped_events=0,
        started_at=timestamp,
        finished_at=timestamp,
        error_count=error_count,
        error_samples=["provider timeout"] if error_count else [],
    )


def test_runtime_settles_market_and_updates_pnl(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    strategy.process_signals([_signal()], auto_trade_enabled=True)
    runtime = WeatherRuntime(
        config=config,
        tracker=tracker,
        strategy_engine=strategy,
        telegram=TelegramClient(),
        resolution_fetcher=lambda slug: "YES",
    )

    outcomes = runtime.settle_due_positions(send_alerts=False)

    assert len(outcomes) == 1
    assert outcomes[0].resolved_positions == 1
    assert tracker.get_paper_stats()["total_pnl"] > 0
    assert len(tracker.get_recent_resolutions()) == 1


def test_dashboard_control_updates_state(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)

    response = dashboard.apply_control_threadsafe({"action": "stop"})

    assert response["ok"] is True
    assert response["state"]["controls"]["state"] == "paused"
    assert response["state"]["controls"]["last_action"] == "stop"
    assert response["state"]["recent_operator_actions"][0]["action"] == "stop"


def test_dashboard_exposes_recent_resolutions(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    strategy.process_signals([_signal("resolved-1")], auto_trade_enabled=True)
    runtime = WeatherRuntime(
        config=config,
        tracker=tracker,
        strategy_engine=strategy,
        telegram=TelegramClient(),
        resolution_fetcher=lambda slug: "YES",
    )
    runtime.settle_due_positions(send_alerts=False)
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)

    dashboard.refresh_once()
    state = dashboard.get_state_threadsafe()

    assert len(state["recent_resolutions"]) == 1
    assert state["recent_resolutions"][0]["resolution"] == "YES"


def test_dashboard_exposes_enriched_open_trade_cards(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    strategy.process_signals([_signal("open-card-1")], auto_trade_enabled=True)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)

    dashboard.refresh_once()
    state = dashboard.get_state_threadsafe()

    assert len(state["open_positions"]) == 1
    trade = state["open_positions"][0]
    assert trade["event_title"] == "Highest temperature in NYC on April 25"
    assert trade["target_label"] == "70-71F"
    assert trade["outcome_probability"] == 0.8
    assert trade["expected_value_pnl"] > 0
    assert trade["holding_seconds"] is not None


def test_control_payload_exposes_paper_metrics(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(750.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)

    payload = control_plane.build_controls_payload()

    assert payload["paper_balance"] == 750.0
    assert payload["paper_initial_capital"] == 750.0
    assert payload["paper_open_positions"] == 0


def test_load_config_reads_second_level_scan_overrides(tmp_path: Path):
    config_path = _write_config(tmp_path)
    options_path = tmp_path / "options.json"
    options_path.write_text(
        json.dumps(
            {
                "temperature_scan_seconds": 5,
                "precipitation_scan_seconds": 12,
                "resolution_check_minutes": 5,
            }
        ),
        encoding="utf-8",
    )

    config = load_config(config_path, ha_options_path=options_path)

    assert config.app.auto_temperature_scan_seconds == 5
    assert config.app.auto_precipitation_scan_seconds == 12
    assert config.app.resolution_check_minutes == 5


def test_scheduled_interval_seconds_prefers_fast_second_overrides():
    assert _scheduled_interval_seconds(5, 120, minimum_seconds=5) == 5
    assert _scheduled_interval_seconds(0, 15, minimum_seconds=5) == 900
    assert _scheduled_interval_seconds(1, 15, minimum_seconds=5) == 5


def test_runtime_processes_queued_scan_and_exports_results(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    signal = _signal("queued-1")
    export_root = tmp_path / "exports"
    calls: list[int] = []

    def temperature_scanner(*, limit: int = 300) -> ScanBatch:
        calls.append(limit)
        return _batch(signal, error_count=1)

    runtime = WeatherRuntime(
        config=config,
        tracker=tracker,
        strategy_engine=strategy,
        telegram=TelegramClient(),
        temperature_scanner=temperature_scanner,
        scan_export_root=export_root,
    )

    try:
        first = runtime.request_scan("temperature", send_alerts=False, reason="operator")
        second = runtime.request_scan("temperature", send_alerts=False, reason="operator")
        assert first["queued"] is True
        assert second["queued"] is False

        runtime.start_background_loops()
        assert runtime.wait_for_idle(timeout=5.0) is True
    finally:
        runtime.stop_background_loops()

    assert calls == [config.temperature.scan_limit]
    state = runtime.get_status_snapshot()
    assert state["last_temperature_scan_status"] == "completed"
    assert state["last_temperature_scan_reason"] == "operator"
    assert state["last_temperature_error_count"] == 1
    assert state["scan_queue_depth"] == 0
    assert tracker.get_paper_stats()["open_positions"] == 1

    export_files = list(export_root.glob("*.json"))
    assert len(export_files) == 1
    payload = json.loads(export_files[0].read_text(encoding="utf-8"))
    assert payload["status"] == "completed"
    assert payload["reason"] == "operator"
    assert payload["batch"]["error_count"] == 1
    assert payload["opened_count"] == 1


def test_dashboard_exports_snapshot_and_control_queue_state(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    export_path = tmp_path / "dashboard_state.json"
    dashboard = DashboardStateService(
        tracker=tracker,
        runtime=runtime,
        control_plane=control_plane,
        state_export_path=export_path,
    )

    result = control_plane.apply_sync(ControlRequest(action="scan_temperature"))
    dashboard.refresh_once()
    state = dashboard.get_state_threadsafe()
    payload = json.loads(export_path.read_text(encoding="utf-8"))

    assert result.ok is True
    assert result.status == 202
    assert state["controls"]["scan_queue_depth"] == 1
    assert state["controls"]["pending_scan_types"] == ["temperature"]
    assert payload["exports"]["dashboard_state_error"] is None
    assert payload["controls"]["scan_queue_depth"] == 1


def test_dashboard_export_failure_is_non_fatal(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(
        tracker=tracker,
        runtime=runtime,
        control_plane=control_plane,
        state_export_path=tmp_path,
    )

    dashboard.refresh_once()
    state = dashboard.get_state_threadsafe()

    assert state["controls"]["state"] == "running"
    assert state["exports"]["dashboard_state_error"] is not None


def test_runtime_scan_export_failure_is_non_fatal(tmp_path: Path, monkeypatch):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    export_root = tmp_path / "scan_exports"
    runtime = WeatherRuntime(
        config=config,
        tracker=tracker,
        strategy_engine=strategy,
        telegram=TelegramClient(),
        temperature_scanner=lambda *, limit=300: _batch(_signal("scan-export-failure")),
        scan_export_root=export_root,
    )
    original_write_text = Path.write_text

    def flaky_write_text(path: Path, *args, **kwargs):
        if path.parent == export_root:
            raise OSError("disk full")
        return original_write_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", flaky_write_text)

    batch, results = runtime.run_temperature_scan(send_alerts=False)
    state = runtime.get_status_snapshot()

    assert len(batch.signals) == 1
    assert len(results) == 1
    assert state["last_temperature_scan_status"] == "completed"
    assert state["last_scan_export_error"] == "disk full"
