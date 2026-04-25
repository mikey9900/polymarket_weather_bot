from __future__ import annotations

from pathlib import Path

import yaml

from weather_bot.config import load_config
from weather_bot.control_plane import ControlPlane
from weather_bot.dashboard_state import DashboardStateService
from weather_bot.models import ForecastSnapshot, WeatherSignal
from weather_bot.paths import DEFAULT_CONFIG_TEMPLATE_PATH
from weather_bot.runtime import WeatherRuntime
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
