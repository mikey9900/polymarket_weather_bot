from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

from weather_bot.config import load_config
from weather_bot.models import ForecastSnapshot, WeatherSignal
from weather_bot.paths import DEFAULT_CONFIG_TEMPLATE_PATH
from weather_bot.research.artifacts import build_artifacts
from weather_bot.research.runtime import ResearchSnapshotProvider
from weather_bot.research.tuner import promote_candidate, propose_tuning
from weather_bot.strategy import WeatherStrategyEngine
from weather_bot.tracker import WeatherTracker


def _write_config(tmp_path: Path) -> Path:
    payload = yaml.safe_load(DEFAULT_CONFIG_TEMPLATE_PATH.read_text(encoding="utf-8"))
    payload["strategy"]["temperature"]["max_source_age_hours"] = 12.0
    config_path = tmp_path / "active_config.yaml"
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return config_path


def _signal(
    key: str,
    *,
    score: float,
    liquidity: float,
    edge_abs: float,
    source_count: int,
    source_dispersion_pct: float,
    source_age_hours: float,
) -> WeatherSignal:
    created_at = (datetime.now(timezone.utc) - timedelta(hours=source_age_hours)).isoformat()
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
        edge=edge_abs,
        edge_abs=edge_abs,
        edge_size="large" if edge_abs >= 0.2 else "small",
        confidence="confirmed" if source_count >= 2 else "om_only",
        source_count=source_count,
        liquidity=liquidity,
        time_to_resolution_s=12 * 3600.0,
        source_dispersion_pct=source_dispersion_pct,
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
        created_at=created_at,
    )


def test_research_artifacts_tuning_and_promotion(tmp_path: Path):
    config_path = _write_config(tmp_path)
    config = load_config(config_path)
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(4000.0)
    strategy = WeatherStrategyEngine(config, tracker)

    winners = [
        _signal(f"win-{index}", score=0.72 + index * 0.01, liquidity=220 + index * 20, edge_abs=0.55, source_count=3, source_dispersion_pct=0.02, source_age_hours=0.5)
        for index in range(5)
    ]
    losers = [
        _signal(f"loss-{index}", score=0.80 + index * 0.01, liquidity=60 + index * 5, edge_abs=0.48, source_count=2, source_dispersion_pct=0.14, source_age_hours=8.0)
        for index in range(5)
    ]

    for signal in winners + losers:
        result = strategy.process_signals([signal], auto_trade_enabled=True)[0]
        assert result.position is not None
        tracker.settle_market(signal.market_slug, "YES" if signal.signal_key.startswith("win-") else "NO")

    artifact_result = build_artifacts(
        tracker_db=tmp_path / "weatherbot.db",
        policy_path=tmp_path / "runtime_policy.json",
        report_json_path=tmp_path / "research_report.json",
        report_md_path=tmp_path / "research_report.md",
        bundle_path=tmp_path / "latest_bundle.json",
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
        artifact_overrides={
            "policy_path": tmp_path / "runtime_policy.json",
            "report_json_path": tmp_path / "research_report.json",
            "report_md_path": tmp_path / "research_report.md",
            "bundle_path": tmp_path / "latest_bundle.json",
            "warehouse_path": tmp_path / "warehouse.duckdb",
            "lookback_days": 365,
        },
    )
    runtime_provider = ResearchSnapshotProvider(tmp_path / "runtime_policy.json")
    adjustment = runtime_provider.adjust_signal(
        _signal(
            "probe",
            score=0.84,
            liquidity=70.0,
            edge_abs=0.46,
            source_count=2,
            source_dispersion_pct=0.13,
            source_age_hours=10.0,
        )
    )

    policy_payload = json.loads((tmp_path / "runtime_policy.json").read_text(encoding="utf-8"))
    bundle_payload = json.loads((tmp_path / "latest_bundle.json").read_text(encoding="utf-8"))

    assert Path(artifact_result["policy_path"]).exists()
    assert Path(artifact_result["bundle_path"]).exists()
    assert artifact_result["warehouse"]["ok"] is True
    assert artifact_result["warehouse"]["resolved_outcomes"] == 10
    assert policy_payload["dispersion_features"]
    assert policy_payload["staleness_features"]
    assert bundle_payload["summary"]["outcome_count"] == 10
    assert adjustment["cluster_id"]
    assert adjustment["score_adjustment"] < 0
    assert adjustment["feature_keys"]["staleness"] == "aging"
    assert tuning_result["candidate_status"] == "ready"
    assert any(change["path"] == "strategy.temperature.max_source_dispersion_pct" for change in tuning_result["changes"])
    assert any(change["path"] == "strategy.temperature.max_source_age_hours" for change in tuning_result["changes"])

    promote_result = promote_candidate(
        config_path=config_path,
        tuner_state_path=tmp_path / "tuner_state.json",
        receipt_path=tmp_path / "last_apply_receipt.json",
    )
    active_config = yaml.safe_load(config_path.read_text(encoding="utf-8"))

    assert promote_result["ok"] is True
    assert promote_result["receipt"]["changed_paths"]
    assert float(active_config["strategy"]["temperature"]["max_source_dispersion_pct"]) < 0.18


def test_research_warehouse_sync_handles_empty_tracker(tmp_path: Path):
    pytest.importorskip("duckdb")

    from weather_bot.research.warehouse import ResearchWarehouse

    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.close()

    warehouse = ResearchWarehouse(tmp_path / "warehouse.duckdb")
    try:
        result = warehouse.sync_from_tracker(tmp_path / "weatherbot.db")
    finally:
        warehouse.close()

    assert result["ok"] is True
    assert result["signals"] == 0
    assert result["decisions"] == 0
    assert result["paper_positions"] == 0
    assert result["resolved_outcomes"] == 0


def test_research_warehouse_optional_dependency(tmp_path: Path):
    from weather_bot.research.warehouse import ResearchWarehouse

    try:
        warehouse = ResearchWarehouse(tmp_path / "warehouse.duckdb")
    except RuntimeError as exc:
        assert "duckdb" in str(exc)
    else:
        warehouse.close()
