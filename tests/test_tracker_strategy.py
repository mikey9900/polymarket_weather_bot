from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

from weather_bot.config import load_config
from weather_bot.models import ForecastSnapshot, WeatherDecision, WeatherSignal
from weather_bot.paths import DEFAULT_CONFIG_TEMPLATE_PATH
from weather_bot.strategy import WeatherStrategyEngine
from weather_bot.tracker import WeatherTracker


def _write_config(tmp_path: Path) -> Path:
    payload = yaml.safe_load(DEFAULT_CONFIG_TEMPLATE_PATH.read_text(encoding="utf-8"))
    config_path = tmp_path / "active_config.yaml"
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return config_path


def _make_signal(
    key: str = "sig-1",
    *,
    direction: str = "YES",
    score: float = 0.8,
    market_slug: str | None = None,
    created_at: str | None = None,
    source_dispersion_pct: float = 0.02,
    market_prob: float = 0.3,
    forecast_prob: float = 0.75,
    edge: float = 0.45,
    edge_abs: float = 0.45,
) -> WeatherSignal:
    snapshot = ForecastSnapshot(
        market_type="temperature",
        city_slug="nyc",
        event_date="2026-04-25",
        unit="F",
        om_temp=71.0,
        vc_temp=72.0,
        source_probabilities={"openmeteo": 0.74, "visual_crossing": 0.76},
    )
    return WeatherSignal(
        signal_key=key,
        market_type="temperature",
        event_title="Highest temperature in NYC on April 25",
        market_slug=market_slug or f"market-{key}",
        event_slug=f"event-{key}",
        city_slug="nyc",
        event_date="2026-04-25",
        label="70-71F",
        direction=direction,
        market_prob=market_prob,
        forecast_prob=forecast_prob,
        edge=edge,
        edge_abs=edge_abs,
        edge_size="large",
        confidence="confirmed",
        source_count=3,
        liquidity=500.0,
        time_to_resolution_s=8 * 3600.0,
        source_dispersion_pct=source_dispersion_pct,
        score=score,
        forecast_snapshot=snapshot,
        raw_payload={"event_title": "Highest temperature in NYC on April 25", "label": "70-71F", "direction": direction},
        created_at=created_at or datetime.now(timezone.utc).isoformat(),
    )


def test_tracker_migrates_legacy_edges(tmp_path: Path):
    legacy_file = tmp_path / "legacy_edges.json"
    legacy_file.write_text(
        """
        [
          {
            "id": "legacy-1",
            "scan_time": "2026-04-20T12:00:00+00:00",
            "event_title": "Highest temperature in NYC on April 20",
            "city_slug": "nyc",
            "event_date": "2026-04-20",
            "label": "70-71F",
            "direction": "YES",
            "confidence": "confirmed",
            "edge_size": "large",
            "market_price": 0.40,
            "edge": 0.20,
            "liquidity": 200,
            "event_slug": "legacy-event",
            "market_slug": "legacy-market",
            "bought": true,
            "buy_price": 0.40,
            "resolved": true,
            "resolution": "YES"
          }
        ]
        """,
        encoding="utf-8",
    )
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    imported = tracker.migrate_legacy_edges(paths=[legacy_file])

    assert imported == 1
    assert len(tracker.get_recent_signals()) == 1
    positions = tracker.get_recent_paper_positions()
    assert len(positions) == 1
    assert positions[0]["status"] == "resolved"


def test_strategy_opens_paper_position(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)

    result = strategy.process_signals([_make_signal()], auto_trade_enabled=True)[0]

    assert result.decision.accepted is True
    assert result.position is not None
    initial, available = tracker.get_paper_capital()
    assert initial == 1000.0
    assert available < initial


def test_strategy_default_paper_mode_does_not_record_shadow_entries(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)

    result = strategy.process_signals([_make_signal(key="plain-paper")], auto_trade_enabled=True)[0]

    assert result.position is not None
    assert tracker.get_shadow_order_summary()["total_count"] == 0
    assert tracker.get_recent_shadow_order_intents(limit=5) == []


def test_strategy_paper_shadow_mode_records_shadow_entry_and_keeps_paper_flow(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    object.__setattr__(config.paper, "execution_mode", "paper_shadow")
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)

    result = strategy.process_signals([_make_signal(key="paper-shadow-entry")], auto_trade_enabled=True)[0]

    assert result.position is not None
    intents = tracker.get_recent_shadow_order_intents(limit=5)
    assert len(intents) == 1
    assert intents[0]["intent_kind"] == "entry"
    assert intents[0]["execution_mode"] == "paper_shadow"
    assert intents[0]["position_id"] == int(result.position.id)
    assert intents[0]["order_action"] == "BUY"
    assert intents[0]["status"] == "mirrored"
    summary = tracker.get_shadow_order_summary()
    assert summary["total_count"] == 1
    assert summary["entry_count"] == 1
    assert summary["exit_count"] == 0


def test_strategy_uses_final_score_after_research_adjustment(tmp_path: Path):
    class ResearchProvider:
        @staticmethod
        def adjust_signal(_signal: WeatherSignal) -> dict:
            return {"score_adjustment": -0.35}

    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker, research_provider=ResearchProvider())

    result = strategy.process_signals([_make_signal(score=0.76)], auto_trade_enabled=True)[0]

    assert result.decision.accepted is False
    assert "Final score" in result.decision.reason


def test_strategy_enforces_market_position_cap(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)

    first = strategy.process_signals([_make_signal(key="cap-1", market_slug="shared-market", direction="YES")], auto_trade_enabled=True)[0]
    second = strategy.process_signals([_make_signal(key="cap-2", market_slug="shared-market", direction="NO")], auto_trade_enabled=True)[0]

    assert first.position is not None
    assert second.position is None
    assert second.decision.accepted is False
    assert "Maximum paper positions reached for this market." in second.decision.reason


def test_strategy_rejects_stale_signal(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    stale_created_at = (datetime.now(timezone.utc) - timedelta(hours=12)).isoformat()

    result = strategy.process_signals([_make_signal(key="stale", created_at=stale_created_at)], auto_trade_enabled=True)[0]

    assert result.decision.accepted is False
    assert "Signal is stale" in result.decision.reason


def test_strategy_prices_no_contract_using_complement_price(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)

    result = strategy.process_signals(
        [
            _make_signal(
                key="no-side",
                direction="NO",
                market_prob=0.8,
                forecast_prob=0.2,
                edge=-0.49,
                edge_abs=0.49,
            )
        ],
        auto_trade_enabled=True,
    )[0]

    assert result.position is not None
    expected_entry_price = round(0.2 * (1.0 + config.paper.entry_slippage_bps / 10000.0), 6)
    assert result.position.entry_price == expected_entry_price


def test_strategy_edge_floor_override_only_affects_future_entries(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)

    first = strategy.process_signals([_make_signal(key="edge-live-1", edge=0.15, edge_abs=0.15)], auto_trade_enabled=True)[0]
    strategy.set_paper_entry_min_edge_abs(0.20)
    second = strategy.process_signals([_make_signal(key="edge-live-2", edge=0.15, edge_abs=0.15)], auto_trade_enabled=True)[0]

    assert first.position is not None
    assert tracker.get_paper_stats()["open_positions"] == 1
    assert second.position is None
    assert second.decision.accepted is False
    assert "Edge 15.00% below minimum 20.00%." in second.decision.reason


def test_strategy_lowered_open_cap_does_not_close_existing_positions(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)

    first = strategy.process_signals([_make_signal(key="open-cap-live-1")], auto_trade_enabled=True)[0]
    second = strategy.process_signals([_make_signal(key="open-cap-live-2")], auto_trade_enabled=True)[0]
    strategy.set_paper_max_open_positions(1)
    third = strategy.process_signals([_make_signal(key="open-cap-live-3")], auto_trade_enabled=True)[0]

    assert first.position is not None
    assert second.position is not None
    assert tracker.get_paper_stats()["open_positions"] == 2
    assert third.position is None
    assert third.decision.accepted is False
    assert "Maximum open paper positions reached." in third.decision.reason


def test_tracker_dashboard_positions_preserve_zero_probabilities_and_invalid_decision_metadata(tmp_path: Path):
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    signal = _make_signal()
    signal_id = tracker.log_signal(signal)
    decision_id = tracker.log_decision(
        signal_id,
        WeatherDecision(
            signal_key=signal.signal_key,
            accepted=True,
            reason="paper entry",
            final_score=signal.score,
            policy_action="paper_trade",
            metadata={"note": "valid before mutation"},
        ),
    )
    position = tracker.create_paper_position(
        signal_id=signal_id,
        decision_id=decision_id,
        signal=signal,
        stake_usd=10.0,
    )

    assert position is not None

    tracker.conn.execute(
        """
        UPDATE paper_positions
        SET entry_reference_price = NULL,
            mark_price = 0.0,
            mark_probability = 0.0,
            mark_updated_at = ?
        WHERE id = ?
        """,
        (datetime.now(timezone.utc).isoformat(), int(position.id)),
    )
    tracker.conn.execute("UPDATE decisions SET metadata_json = ? WHERE id = ?", ("{", decision_id))
    tracker.conn.commit()

    dashboard_position = tracker.get_dashboard_paper_positions(limit=1)[0]

    assert dashboard_position["entry_reference_price"] == signal.market_prob
    assert dashboard_position["market_probability"] == 0.0
    assert dashboard_position["outcome_probability"] == 0.0
    assert dashboard_position["expected_value_mode"] == "reviewed_model_prob"
    assert dashboard_position["decision_metadata"] == {}


def test_tracker_create_paper_position_rejects_fee_dominated_notional(tmp_path: Path):
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    signal = _make_signal()
    signal_id = tracker.log_signal(signal)
    decision_id = tracker.log_decision(
        signal_id,
        WeatherDecision(
            signal_key=signal.signal_key,
            accepted=True,
            reason="paper entry",
            final_score=signal.score,
            policy_action="paper_trade",
        ),
    )

    position = tracker.create_paper_position(
        signal_id=signal_id,
        decision_id=decision_id,
        signal=signal,
        stake_usd=10.0,
        fee_bps=10000.0,
    )
    initial, available = tracker.get_paper_capital()

    assert position is None
    assert initial == 1000.0
    assert available == 1000.0


def test_tracker_close_paper_position_preserves_explicit_zero_exit_price(tmp_path: Path):
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    signal = _make_signal()
    signal_id = tracker.log_signal(signal)
    decision_id = tracker.log_decision(
        signal_id,
        WeatherDecision(
            signal_key=signal.signal_key,
            accepted=True,
            reason="paper entry",
            final_score=signal.score,
            policy_action="paper_trade",
        ),
    )
    position = tracker.create_paper_position(
        signal_id=signal_id,
        decision_id=decision_id,
        signal=signal,
        stake_usd=10.0,
    )

    assert position is not None

    result = tracker.close_paper_position(int(position.id), exit_price=0.0, reason="manual_zero_exit")
    latest_trade = tracker.get_dashboard_paper_positions(limit=1)[0]

    assert result is not None
    assert result["exit_reference_price"] == 0.0
    assert result["exit_price"] == 0.0
    assert result["net_exit_payout"] == 0.0
    assert result["realized_pnl"] == -position.cost
    assert latest_trade["status"] == "closed"
    assert latest_trade["exit_reference_price"] == 0.0


def test_no_stop_loss_holds_temperature_no_when_model_still_supports_position(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)

    opened = strategy.process_signals(
        [
            _make_signal(
                key="no-stop-loss-1",
                direction="NO",
                market_prob=0.30,
                forecast_prob=0.10,
                edge=-0.20,
                edge_abs=0.20,
            )
        ],
        auto_trade_enabled=True,
    )[0]

    assert opened.position is not None
    position = tracker.get_dashboard_paper_positions(limit=1, status="open")[0]
    review_signal = _make_signal(
        key="no-stop-loss-1-review",
        direction="NO",
        market_slug=position["market_slug"],
        market_prob=0.80,
        forecast_prob=0.10,
        edge=-0.70,
        edge_abs=0.70,
    )

    decision = strategy.evaluate_position_exit(position, signal=review_signal)

    assert decision.should_close is False
    assert decision.reason_code == "hold"


def test_no_stop_loss_closes_temperature_no_when_price_bad_and_model_deteriorates(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)

    opened = strategy.process_signals(
        [
            _make_signal(
                key="no-stop-loss-deteriorated",
                direction="NO",
                market_prob=0.30,
                forecast_prob=0.10,
                edge=-0.20,
                edge_abs=0.20,
            )
        ],
        auto_trade_enabled=True,
    )[0]

    assert opened.position is not None
    position = tracker.get_dashboard_paper_positions(limit=1, status="open")[0]
    review_signal = _make_signal(
        key="no-stop-loss-deteriorated-review",
        direction="NO",
        market_slug=position["market_slug"],
        market_prob=0.80,
        forecast_prob=0.35,
        edge=-0.45,
        edge_abs=0.45,
    )

    decision = strategy.evaluate_position_exit(position, signal=review_signal)

    assert decision.should_close is True
    assert decision.reason_code == "no_stop_loss"
    assert "model probability fell" in decision.reason


def test_no_stop_loss_ignores_no_entries_below_price_floor(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)

    opened = strategy.process_signals(
        [
            _make_signal(
                key="no-stop-loss-floor",
                direction="NO",
                market_prob=0.70,
                forecast_prob=0.10,
                edge=-0.60,
                edge_abs=0.60,
            )
        ],
        auto_trade_enabled=True,
    )[0]

    assert opened.position is not None
    position = tracker.get_dashboard_paper_positions(limit=1, status="open")[0]
    review_signal = _make_signal(
        key="no-stop-loss-floor-review",
        direction="NO",
        market_slug=position["market_slug"],
        market_prob=0.90,
        forecast_prob=0.10,
        edge=-0.80,
        edge_abs=0.80,
    )

    decision = strategy.evaluate_position_exit(position, signal=review_signal)

    assert decision.should_close is False
    assert decision.reason_code == "hold"


def test_no_stop_loss_does_not_apply_to_yes_positions(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)

    opened = strategy.process_signals(
        [
            _make_signal(
                key="yes-stop-loss-ignore",
                direction="YES",
                market_prob=0.70,
                forecast_prob=0.90,
                edge=0.20,
                edge_abs=0.20,
            )
        ],
        auto_trade_enabled=True,
    )[0]

    assert opened.position is not None
    position = tracker.get_dashboard_paper_positions(limit=1, status="open")[0]
    review_signal = _make_signal(
        key="yes-stop-loss-ignore-review",
        direction="YES",
        market_slug=position["market_slug"],
        market_prob=0.20,
        forecast_prob=0.90,
        edge=0.70,
        edge_abs=0.70,
    )

    decision = strategy.evaluate_position_exit(position, signal=review_signal)

    assert decision.should_close is False
    assert decision.reason_code == "hold"


def test_missing_signal_holds_until_forecast_aware_review(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)

    opened = strategy.process_signals([_make_signal(key="missing-review-hold")], auto_trade_enabled=True)[0]

    assert opened.position is not None
    position = tracker.get_dashboard_paper_positions(limit=1, status="open")[0]

    decision = strategy.evaluate_position_exit(position, signal=None, allow_close_on_missing_signal=True)

    assert decision.should_close is False
    assert decision.reason_code == "missing_signal"
    assert "forecast-aware review" in decision.reason


def test_forecast_aware_exit_requires_two_consecutive_bad_reviews(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)

    opened = strategy.process_signals([_make_signal(key="two-bad-review")], auto_trade_enabled=True)[0]

    assert opened.position is not None
    position = tracker.get_dashboard_paper_positions(limit=1, status="open")[0]
    review_signal = _make_signal(
        key="two-bad-review-review",
        market_slug=position["market_slug"],
        market_prob=0.50,
        forecast_prob=0.52,
        edge=0.02,
        edge_abs=0.02,
    )

    first = strategy.evaluate_position_exit(position, signal=review_signal)
    assert first.should_close is False
    assert first.reason_code == "edge_near_fair"
    assert "First bad review" in first.reason

    tracker.update_paper_position_review(
        int(position["id"]),
        mark_price=first.mark_price,
        mark_probability=first.mark_probability,
        edge_abs=first.edge_abs,
        final_score=first.final_score,
        reason=first.reason,
        reason_code=first.reason_code,
    )
    reviewed_position = tracker.get_dashboard_paper_positions(limit=1, status="open")[0]

    second = strategy.evaluate_position_exit(reviewed_position, signal=review_signal)

    assert second.should_close is True
    assert second.reason_code == "edge_near_fair"
    assert "Remaining model edge" in second.reason


def test_tracker_records_position_review_history_for_review_and_close(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)

    opened = strategy.process_signals([_make_signal(key="review-history-1")], auto_trade_enabled=True)[0]
    assert opened.position is not None
    position_id = int(opened.position.id)

    updated = tracker.update_paper_position_review(
        position_id,
        mark_price=0.72,
        mark_probability=0.78,
        edge_abs=0.18,
        final_score=0.80,
        reviewed_at="2026-05-02T10:30:00+00:00",
        reason="scheduled_open_position_review: Holding test position.",
        reason_code="hold",
    )
    assert updated is True

    closed = tracker.close_paper_position(
        position_id,
        exit_price=0.68,
        reason="NO stop loss hit: mark-to-market P/L -$4.25 is at or below -$4.00.",
        closed_at="2026-05-02T11:00:00+00:00",
        mark_probability=0.78,
        edge_abs=0.18,
        final_score=0.80,
        mark_reason="scheduled_open_position_review: NO stop loss hit.",
        reason_code="no_stop_loss",
    )

    assert closed is not None
    history = tracker.get_position_review_history(limit=None, position_id=position_id)

    assert len(history) == 2
    assert {item["event_kind"] for item in history} == {"review", "close"}
    assert {item["review_reason_code"] for item in history} == {"hold", "no_stop_loss"}
    assert all(item["mark_to_market_pnl"] is not None for item in history)
