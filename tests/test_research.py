from __future__ import annotations

from pathlib import Path

import yaml

from weather_bot.config import load_config
from weather_bot.models import ForecastSnapshot, WeatherSignal
from weather_bot.paths import DEFAULT_CONFIG_TEMPLATE_PATH
from weather_bot.research.artifacts import build_artifacts
from weather_bot.research.runtime import ResearchSnapshotProvider
from weather_bot.research.tuner import propose_tuning
from weather_bot.strategy import WeatherStrategyEngine
from weather_bot.tracker import WeatherTracker


def _write_config(tmp_path: Path) -> Path:
    payload = yaml.safe_load(DEFAULT_CONFIG_TEMPLATE_PATH.read_text(encoding="utf-8"))
    config_path = tmp_path / "active_config.yaml"
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return config_path


def _signal(key: str, *, score: float, liquidity: float) -> WeatherSignal:
    return WeatherSignal(
        signal_key=key,
        market_type="temperature",
        event_title=f"Highest temperature in NYC on April {key[-1]}",
        market_slug=f"market-{key}",
        event_slug=f"event-{key}",
        city_slug="nyc",
        event_date="2026-04-25",
        label="70-71F",
        direction="YES",
        market_prob=0.25,
        forecast_prob=0.75,
        edge=0.50,
        edge_abs=0.50,
        edge_size="large",
        confidence="confirmed",
        source_count=3,
        liquidity=liquidity,
        time_to_resolution_s=4 * 3600.0,
        source_dispersion_pct=0.02,
        score=score,
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


def test_research_artifacts_and_tuning(tmp_path: Path):
    config_path = _write_config(tmp_path)
    config = load_config(config_path)
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(2000.0)
    strategy = WeatherStrategyEngine(config, tracker)

    for index in range(8):
        signal = _signal(f"sig-{index}", score=0.65 + index * 0.01, liquidity=100 + index * 10)
        result = strategy.process_signals([signal], auto_trade_enabled=True)[0]
        tracker.settle_market(signal.market_slug, "YES" if index < 5 else "NO")
        assert result.position is not None

    artifact_result = build_artifacts(
        tracker_db=tmp_path / "weatherbot.db",
        policy_path=tmp_path / "runtime_policy.json",
        report_json_path=tmp_path / "research_report.json",
        report_md_path=tmp_path / "research_report.md",
        warehouse_path=tmp_path / "warehouse.duckdb",
        lookback_days=365,
    )
    tuning_result = propose_tuning(
        config_path=config_path,
        tracker_db=tmp_path / "weatherbot.db",
        tuner_state_path=tmp_path / "tuner_state.json",
        report_json_path=tmp_path / "tuner_report.json",
        report_md_path=tmp_path / "tuner_report.md",
        patch_path=tmp_path / "tuner_patch.diff",
    )
    runtime_provider = ResearchSnapshotProvider(tmp_path / "runtime_policy.json")
    adjustment = runtime_provider.adjust_signal(_signal("probe", score=0.8, liquidity=150.0))

    assert Path(artifact_result["policy_path"]).exists()
    assert Path(tuning_result["artifact_result"]["report_json_path"]).exists()
    assert adjustment["cluster_id"]


def test_research_warehouse_optional_dependency(tmp_path: Path):
    from weather_bot.research.warehouse import ResearchWarehouse

    try:
        warehouse = ResearchWarehouse(tmp_path / "warehouse.duckdb")
    except RuntimeError as exc:
        assert "duckdb" in str(exc)
    else:
        warehouse.close()
